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
import nats.errors
from nats.aio.client import Client as NATS

from nats.aio.msg import Msg

from tests.utils import (
    NATSD,
    ClusteringTestCase,
    SingleServerTestCase,
    SingleWebSocketServerTestCase,
    TLSServerTestCase,
    async_test,
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

    async def test_auth_revoked_closes(self):
        nc = NATS()
        closed = []

        async def close(status, do_cbs=True):
            closed.append(status)

        nc._close = close
        await nc._process_err("'user authentication revoked'")
        await asyncio.sleep(0)
        self.assertIsInstance(nc.last_error, nats.errors.AuthRevokedError)
        self.assertEqual(str(nc.last_error), "nats: user authentication revoked")
        self.assertEqual(closed, [NATS.CLOSED])

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


if __name__ == "__main__":
    import sys

    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
