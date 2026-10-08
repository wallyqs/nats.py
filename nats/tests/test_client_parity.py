"""
Tests for the nats.go parity features of the legacy asyncio client.
"""

import asyncio
import os
import shutil
import tempfile
import unittest

import nats
import nats.errors
from nats.aio.client import Client as NATS

from tests.utils import (
    NATSD,
    SingleServerTestCase,
    async_test,
    start_natsd,
)


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


if __name__ == "__main__":
    import sys

    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
