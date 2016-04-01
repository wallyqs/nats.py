# Copyright 2015-2016 Apcera Inc. All rights reserved.

import asyncio
import json
import time
from random import shuffle
from datetime import datetime
from urllib.parse import urlparse

from nats.aio.errors import *
from nats.aio.utils  import new_inbox
from nats.protocol.parser import *

__version__ = '0.2.0'
__lang__    = 'python3'

INFO_OP     = b'INFO'
CONNECT_OP  = b'CONNECT'
PING_OP     = b'PING'
PONG_OP     = b'PONG'
PUB_OP      = b'PUB'
SUB_OP      = b'SUB'
UNSUB_OP    = b'UNSUB'
OK_OP       = b'+OK'
ERR_OP      = b'-ERR'
_CRLF_      = b'\r\n'
_SPC_       = b' '
_EMPTY_     = b''
EMPTY       = ""

PING_PROTO = PING_OP + _CRLF_
PONG_PROTO = PONG_OP + _CRLF_

MAX_CONTROL_LINE_SIZE  = 1024

DEFAULT_PENDING_SIZE           = 1024 * 1024
DEFAULT_BUFFER_SIZE            = 32768
DEFAULT_RECONNECT_TIME_WAIT    = 2   # in seconds
DEFAULT_MAX_RECONNECT_ATTEMPTS = 10
DEFAULT_PING_INTERVAL          = 120 # in seconds
DEFAULT_MAX_OUTSTANDING_PINGS  = 2
DEFAULT_MAX_PAYLOAD_SIZE       = 1048576

# Buffering Queues for processing commands.
DEFAULT_FLUSHER_QUEUE_SIZE = 1024
DEFAULT_BUFFER_QUEUE_SIZE  = 320
DEFAULT_BUFFER_QUEUE_PENDING_LIMIT = 32

# Pending Limits for Susbcriptions.
DEFAULT_SUB_PENDING_MSGS_LIMIT  = 65536
DEFAULT_SUB_PENDING_BYTES_LIMIT = 65536 * 1024

class Client():
    """
    Asyncio based client for NATS.
    """

    DISCONNECTED = 0
    CONNECTED    = 1
    CLOSED       = 2
    RECONNECTING = 3
    CONNECTING   = 4

    def __repr__(self):
        return "<nats client v{}>".format(__version__)

    def __init__(self):
        self._loop = None
        self._current_server = None
        self._server_info = {}
        self._server_pool = []
        self._reading_task = None
        self._parsing_task = None
        self._msg_task = None
        self._ping_interval_task = None
        self._pings_outstanding = 0
        self._pongs_received = 0
        self._pongs = []
        self._io_reader = None
        self._io_writer = None
        self._err = None
        self._error_cb = None
        self._disconnected_cb = None
        self._closed_cb = None
        self._reconnected_cb = None
        self._max_payload = DEFAULT_MAX_PAYLOAD_SIZE
        self._ssid = 0
        self._subs = {}
        self._status = Client.DISCONNECTED
        self._ps = Parser(self)
        self._pending = []
        self._pending_data_size = 0
        self._flush_queue = None
        self._flusher_task = None
        self._buffer_queue = None
        self._buffer_queue_size = 0
        self.options = {}
        self.stats = {
            'in_msgs':    0,
            'out_msgs':   0,
            'in_bytes':   0,
            'out_bytes':  0,
            'reconnects': 0,
            'errors_received': 0
            }

        # DEBUG
        # self._monitor_task = None

    @asyncio.coroutine
    def connect(self,
                servers=["nats://127.0.0.1:4222"],
                io_loop=asyncio.get_event_loop(),
                error_cb=None,
                disconnected_cb=None,
                closed_cb=None,
                reconnected_cb=None,
                name=None,
                pedantic=False,
                verbose=False,
                allow_reconnect=True,
                reconnect_time_wait=DEFAULT_RECONNECT_TIME_WAIT,
                max_reconnect_attempts=DEFAULT_MAX_RECONNECT_ATTEMPTS,
                ping_interval=DEFAULT_PING_INTERVAL,
                max_outstanding_pings=DEFAULT_MAX_OUTSTANDING_PINGS,
                dont_randomize=False,
                ):
        self._setup_server_pool(servers)
        self._loop = io_loop
        self._error_cb        = error_cb
        self._closed_cb       = closed_cb
        self._reconnected_cb  = reconnected_cb
        self._disconnected_cb = disconnected_cb

        # Customizable options
        self.options["verbose"] = verbose
        self.options["pedantic"] = pedantic
        self.options["name"] = name
        self.options["allow_reconnect"] = allow_reconnect
        self.options["dont_randomize"] = dont_randomize
        self.options["reconnect_time_wait"] = reconnect_time_wait
        self.options["max_reconnect_attempts"] = max_reconnect_attempts
        self.options["ping_interval"] = ping_interval
        self.options["max_outstanding_pings"] = max_outstanding_pings

        if self.options["dont_randomize"] is False:
            shuffle(self._server_pool)

        while True:
            try:
                yield from self._select_next_server()
                yield from self._process_connect_init()
                self._current_server.reconnects = 0
                break
            except ErrNoServers as e:
                self._err = e
                raise e
            except NatsError as e:
                yield from self._close(Client.DISCONNECTED, False)
                self._err = e
                self._current_server.last_attempt = time.monotonic()
                self._current_server.reconnects += 1

    @asyncio.coroutine
    def close(self):
        """
        Closes the socket to which we are connected and
        sets the client to be in the CLOSED state.
        """
        yield from self._close(Client.CLOSED)

    @asyncio.coroutine
    def _close(self, status, do_cbs=True):
        if self.is_closed:
            self._status = status
            return
        self._status = Client.CLOSED

        # Kick the flusher once again so it breaks
        # and avoid pending futures.
        yield from self._flush_pending()

        if self._reading_task is not None and not self._reading_task.cancelled():
            self._reading_task.cancel()

        if self._ping_interval_task is not None and not self._ping_interval_task.cancelled():
            self._ping_interval_task.cancel()

        if self._flusher_task is not None and not self._flusher_task.cancelled():
            try:
                if not self._flush_queue.empty():
                    self._flush_queue.task_done()
            except:
                pass
            finally:
                self._flusher_task.cancel()

        if self._parsing_task is not None and not self._parsing_task.cancelled():
            try:
                if not self._buffer_queue.empty():
                    self._buffer_queue.task_done()
            except:
                pass
            finally:
                self._parsing_task.cancel()

        # In case there are subscriptions using a task, notify that it is done.
        for sid, sub in self._subs.items():
            try:
                if sub.buffer_queue is not None:
                    sub.buffer_queue.task_done()
            except:
                continue

        # Cleanup subscriptions
        self._subs.clear()

        if self._io_writer is not None:
            self._io_writer.close()

        if do_cbs:
            if self._disconnected_cb is not None:
                yield from self._disconnected_cb()
            if self._closed_cb is not None:
                yield from self._closed_cb()

    @asyncio.coroutine
    def publish(self, subject, payload):
        """
        Sends a PUB command to the server on the specified subject.

          ->> PUB hello 5
          ->> MSG_PAYLOAD: world
          <<- MSG hello 2 5

        """
        if self.is_closed:
            raise ErrConnectionClosed
        payload_size = len(payload)
        if payload_size > self._max_payload:
            raise ErrMaxPayload
        yield from self._publish(subject, _EMPTY_, payload, payload_size)

    @asyncio.coroutine
    def publish_request(self, subject, reply, payload):
        """
        Publishes a message tagging it with a reply subscription
        which can be used by those receiving the message to respond.

           ->> PUB hello   _INBOX.2007314fe0fcb2cdc2a2914c1 5
           ->> MSG_PAYLOAD: world
           <<- MSG hello 2 _INBOX.2007314fe0fcb2cdc2a2914c1 5

        """
        if self.is_closed:
            raise ErrConnectionClosed
        payload_size = len(payload)
        if payload_size > self._max_payload:
            raise ErrMaxPayload
        yield from self._publish(subject, reply.encode(), payload, payload_size)

    @asyncio.coroutine
    def _publish(self, subject, reply, payload, payload_size):
        """
        Sends PUB command to the NATS server.
        """
        if subject == "":
            # Avoid sending messages with empty replies.
            raise ErrBadSubject

        payload_size_bytes = ("%d" % payload_size).encode()
        pub_cmd = b''.join([PUB_OP, _SPC_, subject.encode(), _SPC_, reply, _SPC_, payload_size_bytes, _CRLF_, payload, _CRLF_])
        self.stats['out_msgs']  += 1
        self.stats['out_bytes'] += payload_size
        yield from self._send_command(pub_cmd)
        if self._flush_queue.empty():
            yield from self._flush_pending()

    @asyncio.coroutine
    def subscribe(self, subject, queue="", cb=None, future=None, max_msgs=0, async=True, buffer_queue=None):
        """
        Takes a subject string and optional queue string to send a SUB cmd,
        and a callback which to which messages (Msg) will be dispatched.
        """
        if subject == "":
            raise ErrBadSubject

        if self.is_closed:
            raise ErrConnectionClosed

        self._ssid += 1
        ssid = self._ssid
        sub = Subscription(subject=subject,
                           queue=queue,
                           cb=cb,
                           future=future,
                           max_msgs=max_msgs,
                           async=async,
                           buffer_queue=buffer_queue)
        self._subs[ssid] = sub
        yield from self._subscribe(sub, ssid)
        return ssid

    @asyncio.coroutine
    def subscribe_sync(self, subject, **kwargs):
        """
        Sets the subcription to await for the callback processing
        the message once at a time.
        """
        kwargs["async"] = False
        sid = yield from self.subscribe(subject, **kwargs)
        return sid

    @asyncio.coroutine
    def subscribe_task(self, subject, maxsize=8192, **kwargs):
        """
        Creates task with buffered queue to which the incoming messages
        will be put and then consumed by the callback.
        """
        buffer_queue = asyncio.Queue(maxsize=maxsize, loop=self._loop)
        kwargs["buffer_queue"] = buffer_queue
        ssid = yield from self.subscribe(subject, **kwargs)
        sub = self._subs[ssid]

        @asyncio.coroutine
        def consumer(sub):
            while True:
                try:
                    msg = yield from sub.buffer_queue.get()
                    if sub.async:
                        self._loop.create_task(sub.cb(msg))
                    else:
                        yield from sub.cb(msg)
                except asyncio.CancelledError:
                    break

        task = self._loop.create_task(consumer(sub))
        return task

    @asyncio.coroutine
    def unsubscribe(self, ssid, max_msgs=0):
        """
        Takes a subscription sequence id and removes the subscription
        from the client, optionally after receiving more than max_msgs.
        """
        if self.is_closed:
            raise ErrConnectionClosed

        sub = None
        try:
            sub = self._subs[ssid]
        except KeyError:
            # Already unsubscribed.
            return

        # In case subscription has already received enough messages
        # then announce to the server that we are unsubscribing and
        # remove the callback locally too.
        if max_msgs == 0 or sub.received >= max_msgs:
            self._subs.pop(ssid, None)

        # We will send these for all subs when we reconnect anyway,
        # so that we can suppress here.
        if not self.is_reconnecting:
            yield from self.auto_unsubscribe(ssid, max_msgs)

    @asyncio.coroutine
    def _subscribe(self, sub, ssid):
        sub_cmd = b''.join([SUB_OP, _SPC_, sub.subject.encode(), _SPC_, sub.queue.encode(), _SPC_, ("%d" % ssid).encode(), _CRLF_])
        yield from self._send_command(sub_cmd)
        yield from self._flush_pending()

    @asyncio.coroutine
    def request(self, subject, payload, expected=1, cb=None):
        """
        Implements the request/response pattern via pub/sub
        using an ephemeral subscription which will be published
        with customizable limited interest.

          ->> SUB _INBOX.2007314fe0fcb2cdc2a2914c1 90
          ->> UNSUB 90 1
          ->> PUB hello _INBOX.2007314fe0fcb2cdc2a2914c1 5
          ->> MSG_PAYLOAD: world
          <<- MSG hello 2 _INBOX.2007314fe0fcb2cdc2a2914c1 5

        """
        inbox = new_inbox()
        sid = yield from self.subscribe(inbox, cb=cb)
        yield from self.auto_unsubscribe(sid, expected)
        yield from self.publish_request(subject, inbox, payload)
        return sid

    @asyncio.coroutine
    def timed_request(self, subject, payload, timeout=0.5):
        """
        Implements the request/response pattern via pub/sub
        using an ephemeral subscription which will be published
        with a limited interest of 1 reply returning the response
        or raising a Timeout error.

          ->> SUB _INBOX.2007314fe0fcb2cdc2a2914c1 90
          ->> UNSUB 90 1
          ->> PUB hello _INBOX.2007314fe0fcb2cdc2a2914c1 5
          ->> MSG_PAYLOAD: world
          <<- MSG hello 2 _INBOX.2007314fe0fcb2cdc2a2914c1 5

        """
        inbox = new_inbox()
        future = asyncio.Future(loop=self._loop)
        sid = yield from self.subscribe(inbox, future=future, max_msgs=1)
        yield from self.auto_unsubscribe(sid, 1)
        yield from self.publish_request(subject, inbox, payload)

        try:
            msg = yield from asyncio.wait_for(future, timeout, loop=self._loop)
            return msg
        except asyncio.TimeoutError:
            future.cancel()
            raise ErrTimeout

    @asyncio.coroutine
    def auto_unsubscribe(self, sid, limit=1):
        """
        Sends an UNSUB command to the server.  Unsubscribe is one of the basic building
        blocks in order to be able to define request/response semantics via pub/sub
        by announcing the server limited interest a priori.
        """
        b_limit = b''
        if limit > 0:
            b_limit = ("%d" % limit).encode()
        b_sid = ("%d" % sid).encode()
        unsub_cmd = b''.join([UNSUB_OP, _SPC_, b_sid, _SPC_, b_limit, _CRLF_])
        yield from self._send_command(unsub_cmd)
        yield from self._flush_pending()

    @asyncio.coroutine
    def flush(self, timeout=60):
        """
        Sends a pong to the server expecting a pong back ensuring
        what we have written so far has made it to the server and
        also enabling measuring of roundtrip time.
        In case a pong is not returned within the allowed timeout,
        then it will raise ErrTimeout.
        """
        if timeout <= 0:
            raise ErrBadTimeout

        if self.is_closed:
            raise ErrConnectionClosed

        future = asyncio.Future(loop=self._loop)
        try:
            yield from self._send_ping(future)
            yield from asyncio.wait_for(future, timeout, loop=self._loop)
        except asyncio.TimeoutError:
            future.cancel()
            raise ErrTimeout

    @property
    def connected_url(self):
        if self.is_connected:
            return self._current_server.uri
        else:
            return None

    @property
    def last_error(self):
        """
        Returns the last error which may have occured.
        """
        return self._err

    @property
    def pending_data_size(self):
        return self._pending_data_size

    @property
    def is_closed(self):
        return self._status == Client.CLOSED

    @property
    def is_reconnecting(self):
        return self._status == Client.RECONNECTING

    @property
    def is_connected(self):
        return self._status == Client.CONNECTED

    @property
    def is_connecting(self):
        return self._status == Client.CONNECTING

    @asyncio.coroutine
    def _send_command(self, cmd, priority=False):
        if priority:
            self._pending.insert(0, cmd)
        else:
            self._pending.append(cmd)

        self._pending_data_size += len(self._pending)
        if self._pending_data_size > DEFAULT_PENDING_SIZE:
            yield from self._flush_pending()

    @asyncio.coroutine
    def _flush_pending(self):
        if not self.is_connected:
            return

        try:
            # kick the flusher!
            yield from self._flush_queue.put(None)
        except asyncio.CancelledError:
            pass
        except:
            self._process_op_err(
                NatsError("nats: error kicking the flusher"))

    def _setup_server_pool(self, servers):
        for server in servers:
            uri = urlparse(server)
            self._server_pool.append(Srv(uri))

    @asyncio.coroutine
    def _select_next_server(self):
        """
        Looks up in the server pool for an available server
        and attempts to connect.
        """
        srv = None
        now = time.monotonic()
        for s in self._server_pool:
            if s.reconnects > self.options["max_reconnect_attempts"]:
                continue
            if s.did_connect and now > s.last_attempt + self.options["reconnect_time_wait"]:
                yield from asyncio.sleep(self.options["reconnect_time_wait"], loop=self._loop)
            try:
                s.last_attempt = time.monotonic()
                r, w = yield from asyncio.open_connection(
                    s.uri.hostname,
                    s.uri.port,
                    loop=self._loop,
                    limit=DEFAULT_BUFFER_SIZE)
                srv = s
                self._io_reader = r
                self._io_writer = w
                s.did_connect = True
                break
            except Exception as e:
                self._err = e

        if srv is None:
            raise ErrNoServers
        self._current_server = srv

    @asyncio.coroutine
    def _process_err(self, err_msg):
        """
        Processes the raw error message sent by the server
        and close connection with current server.
        """
        if STALE_CONNECTION in err_msg:
            self._process_op_err(ErrStaleConnection)
            return

        if AUTHORIZATION_VIOLATION in err_msg:
            self._err = ErrAuthorization
        else:
            m = b"nats: "+err_msg
            self._err = NatsError(m.decode())

        do_cbs = False
        if not self.is_connecting:
            do_cbs = True

        yield from self._close(Client.CLOSED, do_cbs)

    def _process_op_err(self, e):
        """
        Process errors which occured while reading or parsing
        the protocol. If allow_reconnect is enabled it will
        try to switch the server to which it is currently connected
        otherwise it will disconnect.
        """
        if self.is_connecting or self.is_closed or self.is_reconnecting:
            return

        if self.options["allow_reconnect"] and self.is_connected:
            self._status = Client.RECONNECTING

            if self._reading_task is not None:
                self._reading_task.cancel()

            if self._ping_interval_task is not None:
                self._ping_interval_task.cancel()

            if self._io_writer is not None:
                self._io_writer.close()

            if self._flush_queue is not None:
                try:
                    if not self._flush_queue.empty():
                        self._flush_queue.task_done()
                except:
                    pass
                finally:
                    self._flusher_task.cancel()

            if self._parsing_task is not None and not self._parsing_task.cancelled():
                try:
                    if not self._buffer_queue.empty():
                        self._buffer_queue.task_done()
                except:
                    pass
                finally:
                    self._parsing_task.cancel()

            self._loop.create_task(self._attempt_reconnect())
        else:
            self._process_disconnect()
            self._err = e
            self._loop.create_task(self._close(Client.CLOSED, True))

    @asyncio.coroutine
    def _attempt_reconnect(self):
        self._err = None
        if self._disconnected_cb is not None:
            yield from self._disconnected_cb()

        if self.is_closed:
            return

        if self.options["dont_randomize"]:
            server = self._server_pool.pop(0)
            self._server_pool.append(server)
        else:
            shuffle(self._server_pool)

        while True:
            try:
                yield from self._select_next_server()
                yield from self._process_connect_init()
                self.stats["reconnects"] += 1
                self._current_server.reconnects = 0

                # Replay all the subscriptions in case there were some.
                for ssid, sub in self._subs.items():
                    sub_cmd = b''.join([SUB_OP, _SPC_, sub.subject.encode(), _SPC_, sub.queue.encode(), _SPC_, ("%d" % ssid).encode(), _CRLF_])
                    self._io_writer.write(sub_cmd)
                yield from self._io_writer.drain()

                # Flush pending data before continuing in connected status.
                # FIXME: Could use future here and wait for an error result
                # to bail earlier in case there are errors in the connection.
                yield from self._flush_pending()

                self._status = Client.CONNECTED

                yield from self.flush()
                if self._reconnected_cb is not None:
                    yield from self._reconnected_cb()
                break
            except ErrNoServers as e:
                self._err = e
                yield from self.close()
                break
            except NatsError as e:
                self._err = e
                self._status = Client.RECONNECTING
                self._current_server.last_attempt = time.monotonic()
                self._current_server.reconnects += 1

    def _connect_command(self):
        '''
        Generates a JSON string with the params to be used
        when sending CONNECT to the server.

          ->> CONNECT {"lang": "python3"}

        '''
        options = {
            "verbose":  self.options["verbose"],
            "pedantic": self.options["pedantic"],
            "lang":     __lang__,
            "version":  __version__
        }
        if "auth_required" in self._server_info:
            if self._server_info["auth_required"] == True:
                options["user"] = self._current_server.uri.username
                options["pass"] = self._current_server.uri.password
        if self.options["name"] is not None:
            options["name"] = self.options["name"]

        connect_opts = json.dumps(options, sort_keys=True)
        return b''.join([CONNECT_OP + _SPC_ + connect_opts.encode() + _CRLF_])

    @asyncio.coroutine
    def _process_ping(self):
        """
        Process PING sent by server.
        """
        yield from self._send_command(PONG)
        yield from self._flush_pending()

    @asyncio.coroutine
    def _process_pong(self):
        """
        Process PONG sent by server.
        """
        if len(self._pongs) > 0:
            future = self._pongs.pop(0)
            future.set_result(True)
            self._pongs_received += 1
            self._pings_outstanding -= 1

    @asyncio.coroutine
    def _process_msg(self, msg):
        """
        Process MSG sent by server.
        """
        self.stats['in_msgs']  += 1
        self.stats['in_bytes'] += len(msg.data)

        sub = None
        try:
            sub = self._subs[msg.sid]
        except KeyError:
            # Skip in case no subscription present.
            return

        sub.received += 1
        if sub.max_msgs > 0 and sub.received >= sub.max_msgs:
            # Enough messages so can throwaway subscription now.
            self._subs.pop(msg.sid, None)
            return

        # If the subscription is buffered then put the message
        # in its queue and wait for the task to process it.
        if sub.buffer_queue is not None:
            yield from sub.buffer_queue.put(msg)

        elif sub.cb is not None:
            if asyncio.iscoroutinefunction(sub.cb):
                if sub.async:
                    # Dispatch each one of the callbacks using a task
                    # to run them asynchronously.
                    self._loop.create_task(sub.cb(msg))
                else:
                    # Await for the result each callback at a time
                    # and process sequentially.
                    yield from sub.cb(msg)
            else:
                # Schedule regular callbacks to be processed sequentially.
                self._loop.call_soon(sub.cb, msg)

        elif sub.future is not None and not sub.future.cancelled():
            # Used for timed requests which expect a single response.
            sub.future.set_result(msg)

    def _process_disconnect(self):
        """
        Process disconnection from the server and set client status
        to DISCONNECTED.
        """
        self._status = Client.DISCONNECTED

    @asyncio.coroutine
    def _process_connect_init(self):
        """
        Process INFO received from the server and CONNECT to the server
        with authentication.  It is also responsible of setting up the
        reading and ping interval tasks from the client.
        """
        self._status = Client.CONNECTING

        # FIXME: Add readline timeout
        info_line = yield from self._io_reader.readline()
        _, info = info_line.split(INFO_OP+_SPC_, 1)
        self._server_info = json.loads(info.decode())
        self._max_payload = self._server_info["max_payload"]

        # Refresh state of parser upon reconnect.
        if self.is_reconnecting:
            self._ps.reset()

        connect_cmd = self._connect_command()
        self._io_writer.write(connect_cmd)
        self._io_writer.write(PING_PROTO)
        yield from self._io_writer.drain()

        # FIXME: Add readline timeout
        next_op = yield from self._io_reader.readline()
        if self.options["verbose"] and OK_OP in next_op:
            next_op = yield from self._io_reader.readline()

        if ERR_OP in next_op:
            err_line = next_op.decode()
            _, err_msg = err_line.split(" ", 1)
            # FIXME: Maybe handling could be more special here,
            # checking for ErrAuthorization for example.
            # yield from self._process_err(err_msg)
            raise NatsError("nats: "+err_msg.rstrip('\r\n'))

        if PONG_PROTO in next_op:
            self._status = Client.CONNECTED

        self._reading_task = self._loop.create_task(self._read_loop())
        self._pongs = []
        self._pings_outstanding = 0
        self._ping_interval_task = self._loop.create_task(self._ping_interval())

        # Queue for kicking the flusher
        self._flush_queue = asyncio.Queue(maxsize=DEFAULT_FLUSHER_QUEUE_SIZE, loop=self._loop)
        self._flusher_task = self._loop.create_task(self._flusher())

        # Have 320 slots of DEFAULT_BUFFER_SIZE totalling in maximum 10MB usage.
        self._buffer_queue = asyncio.Queue(maxsize=DEFAULT_BUFFER_QUEUE_SIZE, loop=self._loop)

        # Parsing and Messages tasks cooperate to handle the commands
        # sent by NATS server.
        self._parsing_task = self._loop.create_task(self._parse_loop())

        # DEBUG
        # self._monitor_task = self._loop.create_task(self._monitor_loop())

    @asyncio.coroutine
    def _send_ping(self, future=None):
        if future is None:
            future = asyncio.Future(loop=self._loop)
        self._pongs.append(future)
        self._io_writer.write(PING_PROTO)
        yield from self._flush_pending()

    @asyncio.coroutine
    def _flusher(self):
        """
        Coroutine which continuously tries to consume pending commands
        and then flushes them to the socket.
        """
        while True:
            if self.is_closed:
                break

            try:
                yield from self._flush_queue.get()
                self._io_writer.writelines(self._pending[:])
                self._pending = []
                self._pending_data_size = 0
                yield from self._io_writer.drain()
            except OSError as e:
                self._process_op_err(e)
            except asyncio.CancelledError:
                break
            except:
                self._process_op_err(
                    NatsError("nats: error during flush"))

    @asyncio.coroutine
    def _ping_interval(self):
        while True:
            yield from asyncio.sleep(self.options["ping_interval"],
                                     loop=self._loop)
            if not self.is_connected:
                continue
            try:
                self._pings_outstanding += 1
                if self._pings_outstanding > self.options["max_outstanding_pings"]:
                    self._process_op_err(ErrStaleConnection)
                    return
                yield from self._send_ping()
            except asyncio.CancelledError:
                break
            # except asyncio.InvalidStateError:
            #     pass

    @asyncio.coroutine
    def _read_loop(self):
        """
        Coroutine which gathers bytes sent by the server
        and feeds them to the protocol parser.
        In case of error while reading, it will stop running
        and its task has to be rescheduled.
        """
        while True:
            try:
                should_bail = self.is_closed or self.is_reconnecting
                if should_bail or self._io_reader is None:
                    break
                if self.is_connected and self._io_reader.at_eof():
                    self._process_op_err(ErrStaleConnection)
                    break

                # Read and add chunk to buffer queue
                b = yield from self._io_reader.read(DEFAULT_BUFFER_SIZE)
                yield from self._buffer_queue.put(b)
                self._buffer_queue_size += len(b)
            except ErrProtocol:
                self._process_op_err(ErrProtocol)
                break
            except OSError as e:
                self._process_op_err(e)
                break
            except asyncio.CancelledError:
                break
            # except asyncio.InvalidStateError:
            #     pass

    @asyncio.coroutine
    def _parse_loop(self):
        while True:
            try:
                # Start parsing as soon as we have something pending
                # in the buffer queue.
                # FIXME: Handle 'None' 
                buf = yield from self._buffer_queue.get()
                if buf is None:
                    # FIXME: Process this as an error?
                    break

                self._buffer_queue_size -= len(buf)
                yield from self._ps.parse(buf)
                yield from asyncio.sleep(0, loop=self._loop)

                # Try to parse more in the same pass in case we have
                # many pending chunks to process.
                if self._buffer_queue.qsize() >= DEFAULT_BUFFER_QUEUE_PENDING_LIMIT:
                    for i in range(0, DEFAULT_BUFFER_QUEUE_PENDING_LIMIT):
                        chunk = yield from self._buffer_queue.get()
                        self._buffer_queue_size -= len(chunk)
                        yield from self._ps.parse(chunk)
                        yield from asyncio.sleep(0, loop=self._loop)

            except asyncio.CancelledError:
                break

    def __enter__(self):
        """For when NATS client is used in a context manager"""

        return self

    def __exit__(self, *exc_info):
        """Close connection to NATS when used in a context manager"""

        self._loop.create_task(self._close(Client.CLOSED, True))

    @asyncio.coroutine
    def _monitor_loop(self):
        """
        For debugging....
        """
        while True:
            try:
                start_out_messages = self.stats["out_msgs"]
                start_in_messages = self.stats["in_msgs"]
                yield from asyncio.sleep(1, loop=self._loop)
                end_out_messages = self.stats["out_msgs"]
                end_in_messages = self.stats["in_msgs"]
                print("{} -- delta out: {} -- delta in: {} -- queue size: {} -- queue bytes: {} -- Parser State -- {}".format(
                    time.time(),
                    end_out_messages - start_out_messages,
                    end_in_messages - start_in_messages,
                    self._buffer_queue.qsize(),
                    self._buffer_queue_size,
                    self._ps.state,
                    ))
                print(self.stats)
            except Exception as e:
                print(e)
                continue

class Subscription():

    def __init__(self,
                 subject='',
                 queue='',
                 cb=None,
                 future=None,
                 max_msgs=0,
                 async=True,
                 buffer_queue=None,
                 ):
        self.subject   = subject
        self.queue     = queue
        self.cb        = cb
        self.future    = future
        self.max_msgs  = max_msgs
        self.received  = 0
        self.async     = async
        self.buffer_queue = buffer_queue

class Srv():
    """
    Srv is a helper data structure to hold state of a server.
    """
    def __init__(self, uri):
        self.uri = uri
        self.reconnects = 0
        self.last_attempt = None
        self.did_connect = False
