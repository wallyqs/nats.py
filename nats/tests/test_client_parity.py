"""
Tests for the nats.go parity features of the legacy asyncio client.
"""

import asyncio
import json
import os
import shutil
import ssl
import tempfile
import unittest
from unittest import mock

import nats
import nats.aio.subscription
import nats.errors
from nats.aio.client import Client as NATS

from nats.aio.msg import Msg

from tests.utils import (
    NATSD,
    ClusteringTestCase,
    NkeysServerTestCase,
    SingleJetStreamServerTestCase,
    SingleServerTestCase,
    SingleWebSocketServerTestCase,
    SingleWebSocketTLSServerTestCase,
    TLSServerTestCase,
    TrustedServerTestCase,
    async_test,
    get_config_file,
    start_natsd,
)


class FakeServer:
    """A minimal NATS server that sends the given INFO and answers PING."""

    def __init__(self, info, after_info=b""):
        self.info = info
        self.after_info = after_info
        self.lines = []

    async def __aenter__(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        self.url = f"nats://127.0.0.1:{self.port}"
        return self

    async def __aexit__(self, *exc):
        self.server.close()

    async def _handle(self, reader, writer):
        info = dict({"server_id": "FAKE", "version": "2.10.0", "max_payload": 1048576}, **self.info)
        writer.write(b"INFO " + json.dumps(info).encode() + b"\r\n" + self.after_info)
        await writer.drain()
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                self.lines.append(line)
                if line.startswith(b"PING"):
                    writer.write(b"PONG\r\n")
                    await writer.drain()
        except ConnectionError:
            pass
        writer.close()


class ConfiguredServerTestCase(unittest.TestCase):
    """Runs one nats-server on port 4222 with the class's ``config`` text."""

    config = ""

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.tmpdir = tempfile.mkdtemp()
        self.server_pool = []
        self.server_pool.append(self.start_server(self.config))

    def start_server(self, config, port=4222, http_port=8222, name="server.conf"):
        path = os.path.join(self.tmpdir, name)
        with open(path, "w") as f:
            f.write(config)
        server = NATSD(port=port, http_port=http_port, config_file=path)
        start_natsd(server)
        return server

    def tearDown(self):
        for natsd in self.server_pool:
            natsd.stop()
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        self.loop.close()


PERMISSIONS_CONF = """
authorization {
  users = [
    { user: "limited", password: "pass",
      permissions: { subscribe: { allow: ["allowed", "_INBOX.>"] },
                     publish: { allow: ["allowed", "_INBOX.>"] } } }
  ]
}
"""


class ServerErrorsTest(ConfiguredServerTestCase):
    config = PERMISSIONS_CONF

    @async_test
    async def test_permission_violation_error(self):
        errs = asyncio.Queue()

        async def error_cb(e):
            await errs.put(e)

        nc = await nats.connect("nats://limited:pass@127.0.0.1:4222", error_cb=error_cb)
        await nc.subscribe("denied")
        err = await asyncio.wait_for(errs.get(), 2)
        self.assertIsInstance(err, nats.errors.PermissionViolationError)
        self.assertEqual(str(err), 'nats: permissions violation for subscription to "denied"')
        # The server keeps the connection open after a permissions violation.
        self.assertTrue(nc.is_connected)
        await nc.publish("denied", b"")
        err = await asyncio.wait_for(errs.get(), 2)
        self.assertIsInstance(err, nats.errors.PermissionViolationError)
        self.assertIn("publish", str(err))
        self.assertIs(nc.last_error, err)
        await nc.close()


class PermissionErrOnSubscribeTest(ConfiguredServerTestCase):
    config = PERMISSIONS_CONF

    @async_test
    async def test_next_msg_raises_permission_error(self):
        nc = await nats.connect(
            "nats://limited:pass@127.0.0.1:4222", permission_err_on_subscribe=True, error_cb=self.ignore
        )
        allowed = await nc.subscribe("allowed")
        sub = await nc.subscribe("denied")
        queue_sub = await nc.subscribe("denied", queue="workers")
        # A waiting next_msg wakes up with the error.
        with self.assertRaises(nats.errors.PermissionViolationError) as raised:
            await sub.next_msg(timeout=2)
        self.assertEqual(str(raised.exception), 'nats: permissions violation for subscription to "denied"')
        # And later calls keep failing with it.
        with self.assertRaises(nats.errors.PermissionViolationError):
            await sub.next_msg(timeout=2)
        with self.assertRaises(nats.errors.PermissionViolationError) as raised:
            async for msg in queue_sub.messages:
                pass
        self.assertIn('using queue "workers"', str(raised.exception))

        # Other subscriptions are not affected.
        await nc.publish("allowed", b"ok")
        msg = await allowed.next_msg()
        self.assertEqual(msg.data, b"ok")
        await nc.close()

    @async_test
    async def test_disabled_by_default(self):
        nc = await nats.connect("nats://limited:pass@127.0.0.1:4222", error_cb=self.ignore)
        sub = await nc.subscribe("denied")
        with self.assertRaises(nats.errors.TimeoutError):
            await sub.next_msg(timeout=0.5)
        await nc.close()

    async def ignore(self, e):
        pass


class MaxSubscriptionsTest(ConfiguredServerTestCase):
    config = "max_subscriptions: 1\n"

    @async_test
    async def test_max_subscriptions_exceeded_keeps_connection(self):
        errs = asyncio.Queue()

        async def error_cb(e):
            await errs.put(e)

        nc = await nats.connect("nats://127.0.0.1:4222", error_cb=error_cb)
        await nc.subscribe("one")
        await nc.subscribe("two")
        err = await asyncio.wait_for(errs.get(), 2)
        self.assertIsInstance(err, nats.errors.MaxSubscriptionsExceededError)
        await nc.flush()
        # The server does not close the connection for this error.
        self.assertTrue(nc.is_connected)
        await nc.close()


class AuthErrorsTest(ConfiguredServerTestCase):
    config = 'authorization { token: "secret" }\n'

    @async_test
    async def test_authorization_error_on_connect(self):
        nc = NATS()
        with self.assertRaises(nats.errors.AuthorizationError) as raised:
            await nc.connect("nats://wrong@127.0.0.1:4222", allow_reconnect=False)
        # The server's text is kept.
        self.assertEqual(str(raised.exception), "nats: 'Authorization Violation'")


class ServerErrorMappingTest(unittest.IsolatedAsyncioTestCase):
    async def test_no_info_received(self):
        async def handle(reader, writer):
            writer.write(b"HELLO\r\n")
            await writer.drain()
            await asyncio.sleep(0.5)
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        nc = NATS()
        with self.assertRaises(nats.errors.NoInfoReceivedError):
            await nc.connect(f"nats://127.0.0.1:{port}", allow_reconnect=False)
        server.close()
        await server.wait_closed()

    async def _auth_error_twice(self, err_msg, **options):
        nc = NATS()
        nc.options.update(options)
        nc._current_server = nats.aio.client.Srv(nc._parse_server_uri("nats://127.0.0.1:4222"))
        closed, processed, reported = [], [], []

        async def close(status, do_cbs=True):
            closed.append(status)

        async def process_op_err(e):
            processed.append(e)

        async def error_cb(e):
            reported.append(e)

        nc._close = close
        nc._process_op_err = process_op_err
        nc._error_cb = error_cb
        await nc._process_err(err_msg)
        await asyncio.sleep(0)
        # As nats.go's processAuthError: reported, then a reconnect.
        self.assertEqual(closed, [])
        self.assertEqual(len(processed), 1)
        self.assertEqual(reported, processed)
        await nc._process_err(err_msg)
        await asyncio.sleep(0)
        self.assertEqual(len(reported), 2)
        return nc, closed, processed

    async def test_auth_revoked_reconnects_then_closes(self):
        nc, closed, processed = await self._auth_error_twice("'user authentication revoked'")
        self.assertIsInstance(nc.last_error, nats.errors.AuthRevokedError)
        self.assertEqual(str(nc.last_error), "nats: user authentication revoked")
        # The same error again from the same server aborts.
        self.assertEqual(closed, [NATS.CLOSED])
        self.assertEqual(len(processed), 1)
        self.assertIs(nc._close_err, nc.last_error)

    async def test_authorization_violation_reconnects_then_closes(self):
        nc, closed, processed = await self._auth_error_twice("'authorization violation'")
        self.assertIsInstance(nc.last_error, nats.errors.AuthorizationError)
        self.assertEqual(closed, [NATS.CLOSED])
        self.assertEqual(len(processed), 1)

    async def test_auth_error_ignore_abort_keeps_reconnecting(self):
        nc, closed, processed = await self._auth_error_twice(
            "'user authentication revoked'", ignore_auth_error_abort=True
        )
        self.assertEqual(closed, [])
        self.assertEqual(len(processed), 2)

    async def test_account_auth_expired_reconnects(self):
        nc = NATS()
        processed = []

        async def process_op_err(e):
            processed.append(e)

        nc._process_op_err = process_op_err
        await nc._process_err("'account authentication expired'")
        self.assertEqual(len(processed), 1)
        self.assertIsInstance(processed[0], nats.errors.AccountAuthExpiredError)
        # Code catching the older, broader class still sees it.
        self.assertIsInstance(processed[0], nats.errors.AuthenticationExpiredError)
        self.assertEqual(str(processed[0]), "nats: account authentication expired")

    async def test_max_account_connections_error(self):
        nc = NATS()

        async def close(status, do_cbs=True):
            pass

        nc._close = close
        await nc._process_err("'maximum account active connections exceeded'")
        await asyncio.sleep(0)
        self.assertIsInstance(nc.last_error, nats.errors.MaxAccountConnectionsExceededError)


class ClientErrorsTest(SingleServerTestCase):
    @async_test
    async def test_bad_queue_name(self):
        nc = await nats.connect()
        with self.assertRaises(nats.errors.BadQueueNameError) as raised:
            await nc.subscribe("foo", queue="bad queue")
        self.assertIsInstance(raised.exception, nats.errors.BadSubjectError)
        await nc.close()

    @async_test
    async def test_sync_sub_required(self):
        nc = await nats.connect()

        async def cb(msg):
            pass

        sub = await nc.subscribe("foo", cb=cb)
        with self.assertRaises(nats.errors.SyncSubRequiredError):
            await sub.next_msg()
        await nc.close()

    @async_test
    async def test_max_messages(self):
        nc = await nats.connect()
        sub = await nc.subscribe("foo")
        await sub.unsubscribe(limit=1)
        await nc.publish("foo", b"1")
        await nc.publish("foo", b"2")
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"1")
        with self.assertRaises(nats.errors.MaxMessagesError):
            await sub.next_msg()
        await nc.close()

    @async_test
    async def test_msg_respond_errors(self):
        nc = await nats.connect()
        with self.assertRaises(nats.errors.MsgNoReplyError):
            await Msg(_client=nc, subject="foo").respond(b"")
        with self.assertRaises(nats.errors.MsgNotBoundError):
            await Msg(_client=None, subject="foo", reply="bar").respond(b"")
        await nc.close()

    @async_test
    async def test_mixing_websocket_schemes(self):
        nc = NATS()
        with self.assertRaises(nats.errors.MixingWebsocketSchemesError):
            await nc.connect(["nats://127.0.0.1:4222", "ws://127.0.0.1:8080"])

    @async_test
    async def test_rtt_while_reconnecting(self):
        reconnecting = asyncio.Event()

        async def disconnected_cb():
            reconnecting.set()

        nc = await nats.connect(disconnected_cb=disconnected_cb, reconnect_time_wait=0.2)
        self.server_pool[0].stop()
        await asyncio.wait_for(reconnecting.wait(), 2)
        self.assertTrue(nc.is_reconnecting)
        with self.assertRaises(nats.errors.DisconnectedError):
            await nc.rtt()
        await nc.close()

    @async_test
    async def test_bad_header_msg(self):
        errs = []

        async def error_cb(e):
            errs.append(e)

        nc = await nats.connect(error_cb=error_cb)

        def broken(raw):
            raise ValueError("broken")

        nc._parse_header_lines = broken
        await nc._process_headers(b"NATS/1.0\r\nfoo: bar\r\n\r\n")
        self.assertEqual(len(errs), 1)
        self.assertIsInstance(errs[0], nats.errors.BadHeaderMsgError)
        self.assertIsInstance(errs[0].__cause__, ValueError)
        await nc.close()


class FakeServerErrorsTest(unittest.IsolatedAsyncioTestCase):
    async def test_max_connections_exceeded_on_connect(self):
        # A real server closes the connection after this -ERR, and the reset
        # can make the client miss it, so the fake one keeps it open.
        async with FakeServer({}, after_info=b"-ERR 'maximum connections exceeded'\r\n") as server:
            nc = NATS()
            with self.assertRaises(nats.errors.MaxConnectionsExceededError) as raised:
                await nc.connect(server.url, allow_reconnect=False)
            self.assertEqual(str(raised.exception), "nats: 'maximum connections exceeded'")
            await nc.close()

    async def test_headers_not_supported(self):
        async with FakeServer({"headers": False, "proto": 1}) as server:
            nc = await nats.connect(server.url, allow_reconnect=False)
            with self.assertRaises(nats.errors.HeadersNotSupportedError):
                await nc.publish("foo", b"", headers={"a": "b"})
            await nc.publish("foo", b"")
            await nc.close()

    async def test_no_echo_not_supported(self):
        async with FakeServer({"headers": True, "proto": 0}) as server:
            nc = NATS()
            with self.assertRaises(nats.errors.NoEchoNotSupportedError):
                await nc.connect(server.url, no_echo=True, allow_reconnect=False)
            await nc.close()
            nc = await nats.connect(server.url, allow_reconnect=False)
            await nc.close()


class FakeServerInfoTest(unittest.IsolatedAsyncioTestCase):
    async def test_client_id_and_ip_not_supported(self):
        # A server whose INFO has neither client_id nor client_ip.
        async with FakeServer({}) as server:
            nc = await nats.connect(server.url, allow_reconnect=False)
            with self.assertRaises(nats.errors.ClientIDNotSupportedError):
                nc.get_client_id()
            with self.assertRaises(nats.errors.ClientIPNotSupportedError):
                nc.get_client_ip()
            await nc.close()

        async with FakeServer({"client_id": 7, "client_ip": "10.0.0.1"}) as server:
            nc = await nats.connect(server.url, allow_reconnect=False)
            self.assertEqual(nc.get_client_id(), 7)
            self.assertEqual(nc.get_client_ip(), "10.0.0.1")
            await nc.close()
            with self.assertRaises(nats.errors.ConnectionClosedError):
                nc.get_client_id()

    async def test_cluster_domain_and_system_account(self):
        info = {"cluster": "C1", "domain": "hub", "acc_is_sys": True}
        async with FakeServer(info) as server:
            nc = await nats.connect(server.url, allow_reconnect=False)
            self.assertEqual(nc.connected_cluster_name, "C1")
            self.assertEqual(nc.connected_domain, "hub")
            self.assertTrue(nc.is_system_account)
            await nc.close()


class TLSErrorTest(TLSServerTestCase):
    @async_test
    async def test_tls_error(self):
        nc = NATS()
        with self.assertRaises(nats.errors.TLSError) as raised:
            await nc.connect("tls://127.0.0.1:4224", allow_reconnect=False)
        err = raised.exception
        # Still the ssl errors raised before.
        self.assertIsInstance(err, ssl.SSLCertVerificationError)
        self.assertIsInstance(err, nats.errors.TLSCertVerificationError)
        self.assertIsInstance(err.__cause__, ssl.SSLError)
        self.assertTrue(str(err).startswith("nats: tls error:"))


class CallbacksTest(SingleServerTestCase):
    @async_test
    async def test_connected_cb_on_first_connect_only(self):
        connected = []
        reconnected = asyncio.Event()

        async def connected_cb():
            connected.append(True)

        async def reconnected_cb():
            reconnected.set()

        nc = await nats.connect(connected_cb=connected_cb, reconnected_cb=reconnected_cb, reconnect_time_wait=0.1)
        self.assertEqual(connected, [True])
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 2)
        self.assertEqual(connected, [True])
        await nc.close()

    @async_test
    async def test_disconnected_err_cb(self):
        errs = asyncio.Queue()
        plain = []

        async def disconnected_err_cb(e):
            await errs.put(e)

        async def disconnected_cb():
            plain.append(True)

        nc = await nats.connect(
            disconnected_err_cb=disconnected_err_cb,
            disconnected_cb=disconnected_cb,
            reconnect_time_wait=0.1,
            max_reconnect_attempts=-1,
        )
        self.server_pool[0].stop()
        err = await asyncio.wait_for(errs.get(), 2)
        self.assertIsInstance(err, Exception)
        await nc.close()
        # A close by the user reports no error.
        err = await asyncio.wait_for(errs.get(), 2)
        self.assertIsNone(err)
        # disconnected_err_cb replaces disconnected_cb, as in nats.go.
        self.assertEqual(plain, [])

    @async_test
    async def test_reconnect_error_cb(self):
        errs = asyncio.Queue()

        async def reconnect_error_cb(e):
            await errs.put(e)

        nc = await nats.connect(reconnect_error_cb=reconnect_error_cb, reconnect_time_wait=0.1)
        self.server_pool[0].stop()
        err = await asyncio.wait_for(errs.get(), 2)
        self.assertIsInstance(err, OSError)
        await nc.close()

    @async_test
    async def test_set_callbacks_after_connect(self):
        events = []

        async def disconnected_cb():
            events.append("disconnected")

        async def closed_cb():
            events.append("closed")

        async def reconnected_cb():
            events.append("reconnected")

        async def error_cb(e):
            events.append(e)

        async def discovered_server_cb():
            pass

        async def disconnected_err_cb(e):
            pass

        nc = await nats.connect(reconnect_time_wait=0.1)
        self.assertIsNone(nc.error_cb)
        self.assertIsNone(nc.closed_cb)

        async def cb(msg):
            raise ValueError("handler failed")

        await nc.subscribe("foo", cb=cb)

        nc.set_disconnected_cb(disconnected_cb)
        nc.set_closed_cb(closed_cb)
        nc.set_reconnected_cb(reconnected_cb)
        nc.set_error_cb(error_cb)
        nc.set_discovered_server_cb(discovered_server_cb)
        self.assertIs(nc.disconnected_cb, disconnected_cb)
        self.assertIs(nc.closed_cb, closed_cb)
        self.assertIs(nc.reconnected_cb, reconnected_cb)
        self.assertIs(nc.error_cb, error_cb)
        self.assertIs(nc.discovered_server_cb, discovered_server_cb)
        nc.set_disconnected_err_cb(disconnected_err_cb)
        self.assertIs(nc.disconnected_err_cb, disconnected_err_cb)
        nc.set_disconnected_err_cb(None)

        with self.assertRaises(nats.errors.InvalidCallbackTypeError):
            nc.set_closed_cb(lambda: None)

        # The new error callback also covers subscriptions made before.
        await nc.publish("foo", b"")
        await nc.flush()
        await asyncio.sleep(0.1)
        self.assertIsInstance(events[0], ValueError)

        await nc.force_reconnect()
        for _ in range(20):
            if "reconnected" in events:
                break
            await asyncio.sleep(0.1)
        await nc.close()
        self.assertEqual(events[1:], ["disconnected", "reconnected", "disconnected", "closed"])

        nc.set_error_cb(None)
        self.assertIsNone(nc.error_cb)

    @async_test
    async def test_no_callbacks_after_client_close(self):
        events = []

        async def disconnected_cb():
            events.append("disconnected")

        async def closed_cb():
            events.append("closed")

        nc = await nats.connect(
            disconnected_cb=disconnected_cb, closed_cb=closed_cb, no_callbacks_after_client_close=True
        )
        await nc.close()
        nc = await nats.connect(
            disconnected_cb=disconnected_cb, closed_cb=closed_cb, no_callbacks_after_client_close=True
        )
        await nc.drain()
        self.assertEqual(events, [])

        nc = await nats.connect(disconnected_cb=disconnected_cb, closed_cb=closed_cb)
        await nc.close()
        self.assertEqual(events, ["disconnected", "closed"])


class AuthErrorAbortTest(ConfiguredServerTestCase):
    config = 'authorization { token: "secret" }\n'

    async def _connect_then_change_token(self, **options):
        closed = asyncio.Event()
        errs = []

        async def closed_cb():
            closed.set()

        async def reconnect_error_cb(e):
            errs.append(e)

        nc = await nats.connect(
            "nats://secret@127.0.0.1:4222",
            closed_cb=closed_cb,
            reconnect_error_cb=reconnect_error_cb,
            reconnect_time_wait=0.1,
            max_reconnect_attempts=-1,
            **options,
        )
        self.server_pool[0].stop()
        self.server_pool.append(self.start_server('authorization { token: "other" }\n', name="other.conf"))
        return nc, closed, errs

    @async_test
    async def test_abort_after_repeated_auth_error(self):
        nc, closed, errs = await self._connect_then_change_token()
        await asyncio.wait_for(closed.wait(), 4)
        self.assertTrue(nc.is_closed)
        auth_errs = [e for e in errs if isinstance(e, nats.errors.AuthorizationError)]
        self.assertEqual(len(auth_errs), 2)
        self.assertIsInstance(nc.last_error, nats.errors.AuthorizationError)

    @async_test
    async def test_ignore_auth_error_abort(self):
        reconnected = asyncio.Event()

        async def reconnected_cb():
            reconnected.set()

        nc, closed, errs = await self._connect_then_change_token(
            ignore_auth_error_abort=True, reconnected_cb=reconnected_cb
        )
        while len([e for e in errs if isinstance(e, nats.errors.AuthorizationError)]) < 3:
            await asyncio.sleep(0.05)
        self.assertFalse(nc.is_closed)
        # Once the server accepts the credentials again the client reconnects.
        self.server_pool[1].stop()
        self.server_pool.append(self.start_server(self.config, name="again.conf"))
        await asyncio.wait_for(reconnected.wait(), 4)
        self.assertTrue(nc.is_connected)
        await nc.close()


class LiveAuthErrorServer:
    """
    A NATS server that accepts the first connection and later sends it an
    authentication -ERR, then rejects every later CONNECT with ``reject``.
    """

    def __init__(self, live_err, reject=b"-ERR 'Authorization Violation'\r\n"):
        self.live_err = live_err
        self.reject = reject
        self.connections = 0
        self.first = asyncio.get_running_loop().create_future()

    async def __aenter__(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.url = f"nats://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        return self

    async def __aexit__(self, *exc):
        self.server.close()

    def send_live_err(self):
        writer = self.first.result()
        writer.write(self.live_err)
        writer.close()

    async def _handle(self, reader, writer):
        self.connections += 1
        accept = self.connections == 1
        info = {"server_id": "FAKE", "version": "2.10.0", "max_payload": 1048576}
        writer.write(b"INFO " + json.dumps(info).encode() + b"\r\n")
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                if line.startswith(b"CONNECT") and not accept:
                    writer.write(self.reject)
                    await writer.drain()
                    break
                if line.startswith(b"PING"):
                    writer.write(b"PONG\r\n")
                    await writer.drain()
                    if accept and not self.first.done():
                        self.first.set_result(writer)
        except ConnectionError:
            pass
        writer.close()


class LiveAuthErrorTest(unittest.IsolatedAsyncioTestCase):
    async def run_live_auth_error(self, live_err, **options):
        events = []
        closed = asyncio.Event()

        async def error_cb(e):
            events.append(("error", type(e)))

        async def disconnected_cb():
            events.append("disconnected")

        async def closed_cb():
            closed.set()

        async with LiveAuthErrorServer(live_err) as server:
            nc = await nats.connect(
                server.url,
                error_cb=error_cb,
                disconnected_cb=disconnected_cb,
                closed_cb=closed_cb,
                reconnect_time_wait=0.05,
                max_reconnect_attempts=-1,
                **options,
            )
            server.send_live_err()
            if options.get("ignore_auth_error_abort"):

                async def reconnecting():
                    while server.connections < 4 and not nc.is_closed:
                        await asyncio.sleep(0.02)

                await asyncio.wait_for(reconnecting(), 2)
                self.assertFalse(nc.is_closed)
                await nc.close()
            else:
                await asyncio.wait_for(closed.wait(), 2)
            return nc, events, server

    async def test_authorization_violation_reconnects_then_aborts(self):
        nc, events, server = await self.run_live_auth_error(b"-ERR 'Authorization Violation'\r\n")
        # As nats.go: reported to the error callback, then a reconnect, and
        # the same error again from the same server closes the connection.
        self.assertEqual(
            events[:4],
            [
                ("error", nats.errors.AuthorizationError),
                "disconnected",
                ("error", nats.errors.AuthorizationError),
                "disconnected",
            ],
        )
        self.assertEqual(server.connections, 2)
        self.assertIsInstance(nc.last_error, nats.errors.AuthorizationError)

    async def test_auth_revoked_reconnects(self):
        nc, events, server = await self.run_live_auth_error(b"-ERR 'User Authentication Revoked'\r\n")
        self.assertEqual(events[:2], [("error", nats.errors.AuthRevokedError), "disconnected"])
        # A different auth error on reconnect does not abort at once.
        self.assertEqual(server.connections, 3)
        self.assertIsInstance(nc.last_error, nats.errors.AuthorizationError)

    async def test_ignore_auth_error_abort_keeps_reconnecting(self):
        nc, events, server = await self.run_live_auth_error(
            b"-ERR 'Authorization Violation'\r\n", ignore_auth_error_abort=True
        )
        self.assertEqual(events[0], ("error", nats.errors.AuthorizationError))
        self.assertGreaterEqual(server.connections, 4)


class RetryOnFailedConnectTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.server_pool = []

    def tearDown(self):
        for natsd in self.server_pool:
            natsd.stop()
        self.loop.close()

    @async_test
    async def test_retry_on_failed_connect(self):
        connected = asyncio.Event()
        reconnected = []

        async def connected_cb():
            connected.set()

        async def reconnected_cb():
            reconnected.append(True)

        async def error_cb(e):
            pass

        nc = await nats.connect(
            "nats://127.0.0.1:4222",
            retry_on_failed_connect=True,
            connected_cb=connected_cb,
            reconnected_cb=reconnected_cb,
            error_cb=error_cb,
            reconnect_time_wait=0.1,
            max_reconnect_attempts=-1,
        )
        # connect returned without a server; the client keeps trying.
        self.assertTrue(nc.is_reconnecting)
        self.assertFalse(connected.is_set())
        sub = await nc.subscribe("foo")
        await nc.publish("foo", b"buffered")

        server = NATSD(port=4222)
        self.server_pool.append(server)
        start_natsd(server)
        await asyncio.wait_for(connected.wait(), 3)
        self.assertTrue(nc.is_connected)
        self.assertEqual(reconnected, [])
        self.assertEqual(nc.stats["reconnects"], 0)
        msg = await sub.next_msg(timeout=2)
        self.assertEqual(msg.data, b"buffered")
        await nc.close()

    @async_test
    async def test_retry_on_failed_connect_connects_right_away(self):
        server = NATSD(port=4222)
        self.server_pool.append(server)
        start_natsd(server)
        connected = []

        async def connected_cb():
            connected.append(True)

        nc = await nats.connect("nats://127.0.0.1:4222", retry_on_failed_connect=True, connected_cb=connected_cb)
        self.assertTrue(nc.is_connected)
        self.assertEqual(connected, [True])
        await nc.close()

    @async_test
    async def test_without_retry_connect_fails(self):
        nc = NATS()
        with self.assertRaises(nats.errors.NoServersError):
            await nc.connect("nats://127.0.0.1:4222", max_reconnect_attempts=1, reconnect_time_wait=0.1)


class ReconnectDelayTest(SingleServerTestCase):
    @async_test
    async def test_custom_reconnect_delay_cb(self):
        backoffs = []
        reconnected = asyncio.Event()

        def delay(attempts):
            backoffs.append(attempts)
            return 0.05

        async def reconnected_cb():
            reconnected.set()

        async def error_cb(e):
            pass

        nc = await nats.connect(
            custom_reconnect_delay_cb=delay,
            reconnected_cb=reconnected_cb,
            error_cb=error_cb,
            max_reconnect_attempts=-1,
        )
        self.server_pool[0].stop()
        # With the default reconnect_time_wait of 2s this would take far longer.
        while len(backoffs) < 3:
            await asyncio.sleep(0.05)
        self.assertEqual(backoffs[:3], [1, 2, 3])
        start_natsd(self.server_pool[0])
        await asyncio.wait_for(reconnected.wait(), 3)
        await nc.close()

    @async_test
    async def test_custom_reconnect_delay_cb_without_reconnect_time_wait(self):
        backoffs = []
        reconnected = asyncio.Event()

        def delay(attempts):
            backoffs.append(attempts)
            return 0.05

        async def reconnected_cb():
            reconnected.set()

        async def error_cb(e):
            pass

        nc = await nats.connect(
            custom_reconnect_delay_cb=delay,
            reconnect_time_wait=0,
            reconnected_cb=reconnected_cb,
            error_cb=error_cb,
            max_reconnect_attempts=-1,
        )
        self.server_pool[0].stop()
        # The callback replaces reconnect_time_wait, even when it is zero.
        while len(backoffs) < 3:
            await asyncio.sleep(0.05)
        self.assertEqual(backoffs[:3], [1, 2, 3])
        start_natsd(self.server_pool[0])
        await asyncio.wait_for(reconnected.wait(), 3)
        await nc.close()

    @async_test
    async def test_custom_reconnect_delay_cb_once_per_pass(self):
        events = []

        def delay(attempts):
            events.append(("delay", attempts))
            return 0.01

        async def reconnect_error_cb(e):
            events.append("attempt")

        async def error_cb(e):
            pass

        nc = await nats.connect(
            servers=["nats://127.0.0.1:4222", "nats://127.0.0.1:4991", "nats://127.0.0.1:4992"],
            dont_randomize=True,
            custom_reconnect_delay_cb=delay,
            reconnect_error_cb=reconnect_error_cb,
            error_cb=error_cb,
            max_reconnect_attempts=-1,
        )
        self.server_pool[0].stop()
        while len([e for e in events if e != "attempt"]) < 3:
            await asyncio.sleep(0.01)
        await nc.close()
        # As nats.go: the callback is called with the pass count, once per
        # pass through the three servers, before the pass's last server.
        expected = ["attempt", "attempt", ("delay", 1)]
        expected += ["attempt"] * 3 + [("delay", 2)] + ["attempt"] * 3 + [("delay", 3)]
        self.assertEqual(events[: len(expected)], expected)

    @async_test
    async def test_custom_reconnect_delay_cb_not_used_by_force_reconnect(self):
        backoffs = []
        reconnected = asyncio.Event()

        async def reconnected_cb():
            reconnected.set()

        nc = await nats.connect(
            custom_reconnect_delay_cb=lambda attempts: backoffs.append(attempts) or 2,
            reconnected_cb=reconnected_cb,
        )
        # As nats.go's ForceReconnect, the first server is tried at once.
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 1)
        self.assertEqual(backoffs, [])
        await nc.close()

    def test_reconnect_jitter(self):
        nc = NATS()
        nc.options.update(
            reconnect_time_wait=1,
            reconnect_jitter=0.2,
            reconnect_jitter_tls=2,
        )
        with mock.patch("random.random", return_value=0.5):
            self.assertAlmostEqual(nc._reconnect_delay(), 1.1)
            nc.options["tls"] = object()
            self.assertAlmostEqual(nc._reconnect_delay(), 2.0)
        # No jitter by default.
        nc = NATS()
        nc.options.update(reconnect_time_wait=1)
        self.assertEqual(nc._reconnect_delay(), 1)


class IgnoreDiscoveredServersTest(ClusteringTestCase):
    @async_test
    async def test_ignore_discovered_servers(self):
        discovered = []

        async def discovered_server_cb():
            discovered.append(True)

        nc = await nats.connect(
            "nats://127.0.0.1:4223", ignore_discovered_servers=True, discovered_server_cb=discovered_server_cb
        )
        control = await nats.connect("nats://127.0.0.1:4223")
        await asyncio.get_running_loop().run_in_executor(None, start_natsd, self.server_pool[1])
        while len(control.discovered_servers) == 0:
            await asyncio.sleep(0.05)
        await nc.flush()
        self.assertEqual(len(nc.servers), 1)
        self.assertEqual(nc.discovered_servers, [])
        self.assertEqual(discovered, [])
        await nc.close()
        await control.close()


class FlushOrderTest(SingleServerTestCase):
    @async_test
    async def test_flush_follows_earlier_publishes(self):
        nc = await nats.connect()
        sub = await nc.subscribe("foo")
        await nc.flush()
        for i in range(100):
            await nc.publish("foo", b"x")
        await nc.flush()
        # The PING of flush() was sent after the messages, so the server
        # delivered them all before its PONG.
        self.assertEqual(sub.pending_msgs, 100)
        await nc.close()


class IntrospectionTest(SingleServerTestCase):
    @async_test
    async def test_server_info_accessors(self):
        nc = NATS()
        self.assertIsNone(nc.connected_server_id)
        await nc.connect("nats://127.0.0.1:4222")
        info = nc._server_info
        self.assertEqual(nc.connected_server_id, info["server_id"])
        self.assertEqual(nc.connected_server_name, info["server_name"])
        self.assertIsNone(nc.connected_cluster_name)
        self.assertIsNone(nc.connected_domain)
        self.assertFalse(nc.connected_server_jetstream)
        self.assertFalse(nc.is_system_account)
        self.assertFalse(nc.auth_required)
        self.assertFalse(nc.tls_required)
        self.assertTrue(nc.headers_supported)
        self.assertEqual(nc.connected_addr, "127.0.0.1:4222")
        self.assertTrue(nc.local_addr.startswith("127.0.0.1:"))
        self.assertNotEqual(nc.local_addr, nc.connected_addr)
        self.assertEqual(nc.connected_url_redacted, "nats://127.0.0.1:4222")
        self.assertIsInstance(nc.get_client_id(), int)
        self.assertEqual(nc.get_client_ip(), "127.0.0.1")

        self.assertEqual(nc.num_subscriptions, 0)
        sub = await nc.subscribe("foo")
        await nc.subscribe("bar")
        self.assertEqual(nc.num_subscriptions, 2)
        await sub.unsubscribe()
        self.assertEqual(nc.num_subscriptions, 1)

        with self.assertRaises(nats.errors.ConnectionNotTLSError):
            nc.tls_connection_state()
        await nc.flush()
        self.assertEqual(nc.buffered(), 0)

        await nc.close()
        self.assertIsNone(nc.connected_server_id)
        self.assertIsNone(nc.connected_addr)
        self.assertIsNone(nc.connected_url_redacted)
        with self.assertRaises(nats.errors.DisconnectedError):
            nc.tls_connection_state()
        with self.assertRaises(nats.errors.ConnectionClosedError):
            nc.buffered()
        with self.assertRaises(nats.errors.ConnectionClosedError):
            nc.get_client_id()

    @async_test
    async def test_new_resp_inbox(self):
        nc = await nats.connect()
        inbox = nc.new_resp_inbox()
        prefix = inbox.rsplit(".", 1)[0]
        self.assertTrue(inbox.startswith("_INBOX."))
        self.assertEqual(len(inbox.split(".")), 3)
        self.assertEqual(nc.new_resp_inbox().rsplit(".", 1)[0], prefix)

        # Requests use the same response subscription.
        async def responder(msg):
            await msg.respond(msg.reply.encode())

        await nc.subscribe("service", cb=responder)
        resp = await nc.request("service", b"", timeout=1)
        self.assertEqual(resp.data.decode().rsplit(".", 1)[0], prefix)
        await nc.close()

    def test_new_inbox(self):
        inbox = nats.new_inbox()
        self.assertTrue(inbox.startswith("_INBOX."))
        self.assertNotEqual(inbox, nats.new_inbox())
        self.assertTrue(nats.new_inbox("_MY").startswith("_MY."))

    @async_test
    async def test_barrier(self):
        nc = await nats.connect()
        events = []

        async def slow(msg):
            await asyncio.sleep(0.01)
            events.append(msg.data)

        async def fast(msg):
            events.append(msg.data)

        await nc.subscribe("slow", cb=slow)
        await nc.subscribe("fast", cb=fast)
        for i in range(5):
            await nc.publish("slow", b"slow%d" % i)
        await nc.publish("fast", b"fast")
        await nc.flush()
        done = asyncio.Event()

        async def barrier_fn():
            events.append(b"barrier")
            done.set()

        await nc.barrier(barrier_fn)
        await asyncio.wait_for(done.wait(), 2)
        self.assertEqual(events[-1], b"barrier")
        self.assertEqual(len(events), 7)
        await nc.close()

        with self.assertRaises(nats.errors.ConnectionClosedError):
            await nc.barrier(barrier_fn)

    @async_test
    async def test_barrier_without_subscriptions(self):
        nc = await nats.connect()
        called = []
        await nc.barrier(lambda: called.append(True))
        self.assertEqual(called, [True])
        await nc.close()


class RedactedUrlTest(ConfiguredServerTestCase):
    config = 'authorization { user: "foo", password: "secret" }\n'

    @async_test
    async def test_connected_url_redacted(self):
        nc = await nats.connect("nats://foo:secret@127.0.0.1:4222")
        self.assertEqual(nc.connected_url_redacted, "nats://foo:xxxxx@127.0.0.1:4222")
        self.assertTrue(nc.auth_required)
        await nc.close()


class WebSocketIntrospectionTest(SingleWebSocketServerTestCase):
    @async_test
    async def test_websocket_addresses(self):
        nc = await nats.connect("ws://127.0.0.1:8080")
        self.assertEqual(nc.connected_addr, "127.0.0.1:8080")
        self.assertTrue(nc.local_addr.startswith("127.0.0.1:"))
        with self.assertRaises(nats.errors.ConnectionNotTLSError):
            nc.tls_connection_state()
        await nc.close()


class TLSCallbacksTest(TLSServerTestCase):
    @async_test
    async def test_tls_callbacks_run_on_every_handshake(self):
        calls = []

        def roots(ctx):
            calls.append("roots")
            ctx.load_verify_locations(get_config_file("certs/ca.pem"))

        def cert(ctx):
            calls.append("cert")
            ctx.load_cert_chain(
                certfile=get_config_file("certs/client-cert.pem"),
                keyfile=get_config_file("certs/client-key.pem"),
            )

        reconnected = asyncio.Event()

        async def reconnected_cb():
            reconnected.set()

        nc = await nats.connect(
            "nats://127.0.0.1:4224",
            tls_cert_cb=cert,
            tls_roots_cb=roots,
            reconnected_cb=reconnected_cb,
            reconnect_time_wait=0.1,
        )
        self.assertEqual(calls, ["roots", "cert"])
        self.assertIsInstance(nc.tls_connection_state(), ssl.SSLObject)
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 2)
        self.assertEqual(calls, ["roots", "cert", "roots", "cert"])
        await nc.close()

    @async_test
    async def test_tls_roots_cb_replaces_system_roots(self):
        nc = NATS()
        # Without the CA the server certificate cannot be verified.
        with self.assertRaises(nats.errors.TLSError):
            await nc.connect("tls://127.0.0.1:4224", tls_roots_cb=lambda ctx: None, allow_reconnect=False)

    @async_test
    async def test_client_tls_config(self):
        from nats.aio.client import ClientTLSConfig

        calls = []

        def roots(ctx):
            calls.append("roots")
            ctx.load_verify_locations(get_config_file("certs/ca.pem"))

        def cert(ctx):
            calls.append("cert")
            ctx.load_cert_chain(
                certfile=get_config_file("certs/client-cert.pem"),
                keyfile=get_config_file("certs/client-key.pem"),
            )

        nc = await nats.connect(
            "nats://127.0.0.1:4224", client_tls_config=ClientTLSConfig(cert_cb=cert, roots_cb=roots)
        )
        self.assertEqual(calls, ["roots", "cert"])
        self.assertIsInstance(nc.tls_connection_state(), ssl.SSLObject)
        await nc.close()

        # As nats.go's ErrClientCertOrRootCAsRequired, one callback is needed.
        nc = NATS()
        with self.assertRaises(nats.errors.ClientCertOrRootCAsRequiredError) as err:
            await nc.connect("tls://127.0.0.1:4224", client_tls_config=ClientTLSConfig(), allow_reconnect=False)
        self.assertEqual(str(err.exception), "nats: at least one of certCB or rootCAsCB must be set")
        self.assertFalse(nc.is_connected)

        nc = NATS()
        with self.assertRaises(nats.errors.Error):
            await nc.connect(
                "tls://127.0.0.1:4224",
                client_tls_config=ClientTLSConfig(roots_cb=roots),
                tls_roots_cb=roots,
                allow_reconnect=False,
            )

    @async_test
    async def test_tls_with_callbacks_rejected(self):
        nc = NATS()
        with self.assertRaises(nats.errors.Error):
            await nc.connect("tls://127.0.0.1:4224", tls=self.ssl_ctx, tls_roots_cb=lambda ctx: None)


class WebSocketOptionsTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()

    async def handshake_lines(self, url_path="", **options):
        from tests.test_custom_headers_websocket import start_header_catcher

        addr, got, close_ln = start_header_catcher()
        try:
            with self.assertRaises(Exception):
                await asyncio.wait_for(
                    nats.connect(f"ws://{addr}{url_path}", allow_reconnect=False, **options), timeout=1.0
                )
        finally:
            lines = got.get(timeout=2.0)
            close_ln()
        return lines

    @async_test
    async def test_ws_connection_headers_cb(self):
        from tests.test_custom_headers_websocket import has_header_value

        calls = []

        def headers():
            calls.append(True)
            return {"Authorization": ["Bearer dynamic-%d" % len(calls)]}

        lines = await self.handshake_lines(ws_connection_headers_cb=headers)
        self.assertEqual(len(calls), 1)
        self.assertTrue(has_header_value(lines, "Authorization", "Bearer dynamic-1"))

        with self.assertRaises(nats.errors.WebSocketHeadersAlreadySetError):
            await nats.connect(
                "ws://127.0.0.1:8080", ws_connection_headers={"A": ["b"]}, ws_connection_headers_cb=headers
            )

    @async_test
    async def test_ws_compression(self):
        lines = await self.handshake_lines(ws_compression=True)
        extensions = [line for line in lines if line.lower().startswith("sec-websocket-extensions:")]
        self.assertEqual(len(extensions), 1)
        self.assertIn("permessage-deflate", extensions[0])
        lines = await self.handshake_lines()
        self.assertFalse([line for line in lines if line.lower().startswith("sec-websocket-extensions:")])

    @async_test
    async def test_ws_proxy_path(self):
        lines = await self.handshake_lines(url_path="/ignored", ws_proxy_path="my/proxy")
        self.assertTrue(lines[0].startswith("GET /my/proxy "), lines[0])
        lines = await self.handshake_lines(url_path="/kept")
        self.assertTrue(lines[0].startswith("GET /kept "), lines[0])

    def test_discovered_websocket_servers_keep_scheme(self):
        async def run():
            nc = NATS()
            nc._setup_server_pool("ws://127.0.0.1:8080")
            nc._current_server = nc._server_pool[0]
            nc.options["dont_randomize"] = True
            await nc._process_info({"connect_urls": ["127.0.0.1:8081"]}, initial_connection=True)
            self.assertEqual([s.uri.geturl() for s in nc._server_pool], ["ws://127.0.0.1:8080", "ws://127.0.0.1:8081"])

        self.loop.run_until_complete(run())


WS_COMPRESSION_CONF = """
websocket {
    port: 8080
    no_tls: true
    compression: true
}
"""


class WebSocketCompressionTest(ConfiguredServerTestCase):
    config = WS_COMPRESSION_CONF

    @async_test
    async def test_pub_sub_with_compression(self):
        nc = await nats.connect("ws://127.0.0.1:8080", ws_compression=True)
        sub = await nc.subscribe("foo")
        payload = b"a" * 10000
        await nc.publish("foo", payload)
        msg = await sub.next_msg()
        self.assertEqual(msg.data, payload)
        await nc.close()


class TLSIntrospectionTest(TLSServerTestCase):
    @async_test
    async def test_tls_connection_state(self):
        nc = await nats.connect("nats://127.0.0.1:4224", tls=self.ssl_ctx)
        state = nc.tls_connection_state()
        self.assertIsInstance(state, ssl.SSLObject)
        self.assertIsNotNone(state.version())
        self.assertIsNotNone(state.getpeercert())
        self.assertTrue(nc.tls_required)
        await nc.close()


FOO_USER_SEED = "SUAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU"
FOO_USER_NKEY = "UCK5N7N66OBOINFXAYC2ACJQYFSOD4VYNU6APEJTAVFZB2SVHLKGEW7L"


def foo_user_signature(nonce):
    import base64

    import nkeys

    kp = nkeys.from_seed(bytearray(FOO_USER_SEED.encode()))
    return base64.b64encode(kp.sign(nonce.encode()))


class NkeyAuthTest(NkeysServerTestCase):
    @async_test
    async def test_nkey_with_signature_cb(self):
        nc = await nats.connect(nkey=FOO_USER_NKEY, signature_cb=foo_user_signature, allow_reconnect=False)

        async def help_handler(msg):
            await msg.respond(b"OK!")

        await nc.subscribe("help", cb=help_handler)
        msg = await nc.request("help", b"", timeout=1)
        self.assertEqual(msg.data, b"OK!")
        await nc.close()


class NkeysNotSupportedTest(SingleServerTestCase):
    @async_test
    async def test_nkeys_not_supported(self):
        nc = NATS()
        with self.assertRaises(nats.errors.NkeysNotSupportedError):
            await nc.connect(nkey=FOO_USER_NKEY, signature_cb=foo_user_signature, allow_reconnect=False)


class UserJWTAndSeedTest(TrustedServerTestCase):
    @async_test
    async def test_user_jwt_and_seed(self):
        with open(get_config_file("nkeys/foo-user.creds")) as f:
            lines = f.read().splitlines()
        user_jwt = lines[lines.index("-----BEGIN NATS USER JWT-----") + 1]
        nc = await nats.connect(user_jwt_and_seed=(user_jwt, FOO_USER_SEED), allow_reconnect=False)
        self.assertTrue(nc.is_connected)
        await nc.close()


class UserInfoTest(ConfiguredServerTestCase):
    config = 'authorization { user: "foo", password: "secret" }\n'

    @async_test
    async def test_user_info_cb(self):
        calls = []

        def user_info():
            calls.append(True)
            return "foo", "secret"

        nc = await nats.connect("nats://127.0.0.1:4222", user_info_cb=user_info, allow_reconnect=False)
        self.assertTrue(nc.is_connected)
        self.assertEqual(len(calls), 1)
        await nc.close()

        nc = NATS()
        with self.assertRaises(nats.errors.AuthorizationError):
            await nc.connect("nats://127.0.0.1:4222", user_info_cb=lambda: ("foo", "wrong"), allow_reconnect=False)

    @async_test
    async def test_user_info_cb_with_url_credentials(self):
        calls = []

        def user_info():
            calls.append(True)
            return "foo", "secret"

        # As nats.go's ErrUserInfoAlreadySet: the URL's user:password conflicts.
        nc = NATS()
        with self.assertRaises(nats.errors.UserInfoAlreadySetError):
            await nc.connect("nats://foo:secret@127.0.0.1:4222", user_info_cb=user_info, allow_reconnect=False)
        self.assertEqual(calls, [])
        self.assertFalse(nc.is_connected)


class AuthOptionErrorsTest(unittest.IsolatedAsyncioTestCase):
    async def check(self, error, servers="nats://127.0.0.1:4999", **options):
        nc = NATS()
        with self.assertRaises(error):
            await nc.connect(servers, allow_reconnect=False, **options)

    async def test_conflicting_auth_options(self):
        def jwt():
            return b"jwt"

        await self.check(nats.errors.NkeyButNoSigCBError, nkey=FOO_USER_NKEY)
        await self.check(
            nats.errors.NkeyAndUserError, nkey=FOO_USER_NKEY, signature_cb=foo_user_signature, user_jwt_cb=jwt
        )
        await self.check(nats.errors.UserButNoSigCBError, user_jwt_cb=jwt)
        await self.check(nats.errors.NoUserCBError, signature_cb=foo_user_signature)
        await self.check(nats.errors.UserInfoAlreadySetError, user="foo", user_info_cb=lambda: ("a", "b"))
        await self.check(
            nats.errors.UserInfoAlreadySetError,
            servers=["nats://127.0.0.1:4998", "nats://foo:bar@127.0.0.1:4999"],
            user_info_cb=lambda: ("a", "b"),
        )
        await self.check(nats.errors.TokenAlreadySetError, servers="nats://token@127.0.0.1:4999", token=lambda: "other")


class CustomDialerTest(SingleServerTestCase):
    @async_test
    async def test_custom_dialer(self):
        dialed = []

        async def dialer(host, port):
            dialed.append((host, port))
            # E.g. a tunnel: the client asks for an unreachable address.
            return await asyncio.open_connection("127.0.0.1", 4222)

        reconnected = asyncio.Event()

        async def reconnected_cb():
            reconnected.set()

        nc = await nats.connect(
            "nats://nats.invalid:4999", custom_dialer=dialer, reconnected_cb=reconnected_cb, reconnect_time_wait=0.1
        )
        self.assertEqual(dialed, [("nats.invalid", 4999)])
        sub = await nc.subscribe("foo")
        await nc.publish("foo", b"via dialer")
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"via dialer")
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 2)
        self.assertEqual(len(dialed), 2)
        await nc.close()


class WebSocketCustomDialerTest(SingleWebSocketServerTestCase):
    @async_test
    async def test_custom_dialer_websocket(self):
        dialed = []

        async def dialer(host, port):
            dialed.append((host, port))
            return await asyncio.open_connection("127.0.0.1", 8080)

        reconnected = asyncio.Event()

        async def reconnected_cb():
            reconnected.set()

        # As nats.go, the dialer opens the WebSocket's connection too.
        nc = await nats.connect(
            "ws://nats.invalid:4999", custom_dialer=dialer, reconnected_cb=reconnected_cb, reconnect_time_wait=0.1
        )
        self.assertEqual(dialed, [("nats.invalid", 4999)])
        sub = await nc.subscribe("foo")
        await nc.publish("foo", b"ws via dialer")
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"ws via dialer")
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 2)
        self.assertEqual(len(dialed), 2)
        await nc.publish("foo", b"again")
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"again")
        await nc.close()


class WebSocketTLSCustomDialerTest(SingleWebSocketTLSServerTestCase):
    @async_test
    async def test_custom_dialer_secure_websocket(self):
        dialed = []

        async def dialer(host, port):
            dialed.append((host, port))
            return await asyncio.open_connection("127.0.0.1", 8081)

        # TLS runs over the dialed connection, verifying the URL's host name.
        nc = await nats.connect("wss://localhost:4999", custom_dialer=dialer, tls=self.ssl_ctx)
        self.assertEqual(dialed, [("localhost", 4999)])
        sub = await nc.subscribe("foo")
        await nc.publish("foo", b"wss via dialer")
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"wss via dialer")
        await nc.close()


class StuckTransport:
    """A transport whose writes never complete."""

    def writelines(self, payload):
        pass

    async def drain(self):
        await asyncio.sleep(60)


class FlusherTest(unittest.IsolatedAsyncioTestCase):
    async def run_flusher(self, **options):
        nc = NATS()
        errs = []

        async def error_cb(e):
            errs.append(e)

        op_errs = []

        async def process_op_err(e):
            op_errs.append(e)

        nc._error_cb = error_cb
        nc._process_op_err = process_op_err
        nc.options.update(options)
        nc._transport = StuckTransport()
        nc._status = NATS.CONNECTED
        nc._flush_queue = asyncio.Queue()
        nc._pending = [b"PUB foo 0\r\n\r\n"]
        nc._pending_data_size = len(nc._pending[0])
        flusher = asyncio.create_task(nc._flusher())
        future = asyncio.get_running_loop().create_future()
        await nc._flush_queue.put(future)
        await asyncio.wait_for(future, 2)
        return nc, flusher, errs, op_errs

    async def test_flusher_timeout_reconnects(self):
        nc, flusher, errs, op_errs = await self.run_flusher(flusher_timeout=0.05)
        await asyncio.wait_for(flusher, 1)
        self.assertEqual(len(errs), 1)
        self.assertIsInstance(errs[0], nats.errors.FlushTimeoutError)
        self.assertEqual(op_errs, errs)

    async def test_flusher_error_without_reconnect(self):
        nc, flusher, errs, op_errs = await self.run_flusher(flusher_timeout=0.05, reconnect_on_flusher_error=False)
        self.assertEqual(len(errs), 1)
        self.assertIsInstance(errs[0], nats.errors.FlushTimeoutError)
        self.assertIs(nc.last_error, errs[0])
        self.assertEqual(op_errs, [])
        # The flusher keeps running.
        self.assertFalse(flusher.done())
        flusher.cancel()


class MsgTest(SingleServerTestCase):
    @async_test
    async def test_multi_value_headers_and_size(self):
        nc = await nats.connect()
        sub = await nc.subscribe("foo")
        await nc.publish("foo", b"data", reply="bar", headers={"X": ["a", "b"], "Y": "c"})
        msg = await sub.next_msg()
        # The dict keeps one value per header, as before.
        self.assertEqual(msg.headers, {"X": "b", "Y": "c"})
        self.assertEqual(msg.header_values("X"), ["a", "b"])
        self.assertEqual(msg.header_values("Y"), ["c"])
        self.assertEqual(msg.header_values("Z"), [])
        raw = b"NATS/1.0\r\nX: a\r\nX: b\r\nY: c\r\n\r\n"
        self.assertEqual(msg.size, len("foo") + len("bar") + len(raw) + len(b"data"))

        await nc.publish("foo", b"plain")
        msg = await sub.next_msg()
        self.assertEqual(msg.size, len("foo") + len(b"plain"))
        self.assertEqual(msg.header_values("X"), [])
        await nc.close()

    @async_test
    async def test_add_header(self):
        nc = await nats.connect()
        sub = await nc.subscribe("foo")
        msg = Msg(_client=nc, subject="foo", data=b"x")
        msg.add_header("X", "a")
        msg.add_header("X", "b")
        msg.add_header("Y", "c")
        self.assertEqual(msg.header_values("X"), ["a", "b"])
        self.assertEqual(msg.size, len("foo") + len(b"NATS/1.0\r\nX: a\r\nX: b\r\nY: c\r\n\r\n") + 1)
        await nc.publish(msg.subject, msg.data, headers=msg.headers)
        received = await sub.next_msg()
        self.assertEqual(received.header_values("X"), ["a", "b"])
        await nc.close()

    @async_test
    async def test_respond_msg(self):
        nc = await nats.connect()

        async def handler(msg):
            await msg.respond_msg(Msg(_client=nc, data=b"response", headers={"H": "1"}))

        await nc.subscribe("service", cb=handler)
        resp = await nc.request("service", b"", timeout=1)
        self.assertEqual(resp.data, b"response")
        self.assertEqual(resp.headers, {"H": "1"})

        with self.assertRaises(nats.errors.InvalidMsgError):
            await Msg(_client=nc, subject="foo", reply="bar").respond_msg(None)
        with self.assertRaises(nats.errors.MsgNoReplyError):
            await Msg(_client=nc, subject="foo").respond_msg(Msg(_client=nc))
        await nc.close()


class SubscriptionTest(SingleServerTestCase):
    @async_test
    async def test_pending_limits_and_dropped(self):
        errs = []

        async def error_cb(e):
            errs.append(e)

        nc = await nats.connect(error_cb=error_cb)
        sub = await nc.subscribe("foo", pending_msgs_limit=5)
        self.assertEqual(sub.pending_limits, (5, nats.aio.subscription.DEFAULT_SUB_PENDING_BYTES_LIMIT))
        for i in range(8):
            await nc.publish("foo", b"x")
        await nc.flush()
        self.assertEqual(sub.dropped, 3)
        self.assertEqual(sub.max_pending, (5, 5))
        self.assertEqual(len([e for e in errs if isinstance(e, nats.errors.SlowConsumerError)]), 3)

        for i in range(5):
            await sub.next_msg()
        self.assertEqual(sub.max_pending, (5, 5))
        sub.clear_max_pending()
        self.assertEqual(sub.max_pending, (0, 0))

        # Raising the limit lets more messages queue up; negative is unlimited.
        sub.set_pending_limits(-1, -1)
        self.assertEqual(sub.pending_limits, (-1, -1))
        for i in range(8):
            await nc.publish("foo", b"x")
        await nc.flush()
        self.assertEqual(sub.pending_msgs, 8)
        self.assertEqual(sub.dropped, 3)

        with self.assertRaises(nats.errors.InvalidArgError):
            sub.set_pending_limits(0, 100)
        await sub.unsubscribe()
        with self.assertRaises(nats.errors.BadSubscriptionError):
            sub.set_pending_limits(10, 100)
        await nc.close()

    @async_test
    async def test_connection_default_pending_limits(self):
        nc = await nats.connect(sub_pending_msgs_limit=10, sub_pending_bytes_limit=1024)
        sub = await nc.subscribe("foo")
        self.assertEqual(sub.pending_limits, (10, 1024))
        sub = await nc.subscribe("foo", pending_msgs_limit=20)
        self.assertEqual(sub.pending_limits, (20, 1024))
        await nc.close()

    @async_test
    async def test_is_valid_and_closed_cb(self):
        nc = await nats.connect()
        closed = []

        async def async_closed(subject):
            closed.append(subject)

        sub = await nc.subscribe("unsub")
        sub.set_closed_cb(closed.append)
        self.assertTrue(sub.is_valid)
        await sub.unsubscribe()
        self.assertFalse(sub.is_valid)
        self.assertEqual(closed, ["unsub"])

        sub = await nc.subscribe("auto")
        sub.set_closed_cb(async_closed)
        await sub.unsubscribe(limit=1)
        await nc.publish("auto", b"")
        await nc.flush()
        await asyncio.sleep(0)
        self.assertFalse(sub.is_valid)
        self.assertEqual(closed, ["unsub", "auto"])

        sub = await nc.subscribe("conn")
        sub.set_closed_cb(closed.append)
        await nc.close()
        self.assertFalse(sub.is_valid)
        self.assertEqual(closed, ["unsub", "auto", "conn"])

    @async_test
    async def test_is_draining(self):
        nc = await nats.connect()
        release = asyncio.Event()
        closed = []

        async def cb(msg):
            await release.wait()

        sub = await nc.subscribe("foo", cb=cb)
        sub.set_closed_cb(closed.append)
        await nc.publish("foo", b"")
        await nc.flush()
        self.assertFalse(sub.is_draining)
        drain = asyncio.create_task(sub.drain())
        await asyncio.sleep(0.1)
        self.assertTrue(sub.is_draining)
        release.set()
        await asyncio.wait_for(drain, 2)
        self.assertFalse(sub.is_draining)
        self.assertFalse(sub.is_valid)
        self.assertEqual(closed, ["foo"])
        await nc.close()


class JetStreamSubscriptionTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_push_subscription_state(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="S", subjects=["s"])
        await js.publish("s", b"1")
        closed = []
        sub = await js.subscribe("s")
        sub.set_closed_cb(closed.append)
        msg = await sub.next_msg()
        self.assertEqual(msg.data, b"1")
        self.assertTrue(sub.is_valid)
        self.assertEqual(sub.dropped, 0)
        self.assertEqual(sub.max_pending[0], 1)
        self.assertFalse(sub.is_draining)
        await sub.unsubscribe()
        self.assertFalse(sub.is_valid)
        self.assertEqual(closed, [sub.subject])
        await nc.close()


if __name__ == "__main__":
    import sys

    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
