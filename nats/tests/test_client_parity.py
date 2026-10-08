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

import nats
import nats.errors
from nats.aio.client import Client as NATS

from nats.aio.msg import Msg

from tests.utils import (
    NATSD,
    SingleServerTestCase,
    TLSServerTestCase,
    async_test,
    start_natsd,
)


class FakeServer:
    """A minimal NATS server that sends the given INFO and answers PING."""

    def __init__(self, info):
        self.info = info
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
        writer.write(b"INFO " + json.dumps(info).encode() + b"\r\n")
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


class MaxConnectionsTest(ConfiguredServerTestCase):
    config = "max_connections: 1\n"

    @async_test
    async def test_max_connections_exceeded_on_connect(self):
        nc = await nats.connect("nats://127.0.0.1:4222")
        nc2 = NATS()
        with self.assertRaises(nats.errors.MaxConnectionsExceededError) as raised:
            await nc2.connect("nats://127.0.0.1:4222", allow_reconnect=False)
        self.assertIsInstance(raised.exception, nats.errors.Error)
        self.assertIn("maximum connections exceeded", str(raised.exception))
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


if __name__ == "__main__":
    import sys

    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
