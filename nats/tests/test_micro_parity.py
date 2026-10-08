import asyncio

import nats
import nats.errors
import nats.micro
from nats.micro import add_service
from nats.micro.errors import (
    ArgRequiredError,
    ConfigValidationError,
    MicroError,
    NATSError,
    RespondError,
    ServiceNameRequiredError,
    VerbNotSupportedError,
)
from nats.micro.request import Request
from nats.micro.service import (
    EndpointConfig,
    ServiceConfig,
    ServiceVerb,
    control_subject,
)

from tests.utils import SingleServerTestCase, async_test


async def noop_handler(request: Request) -> None:
    pass


class MicroErrorsTest(SingleServerTestCase):
    def test_config_validation_errors(self):
        cases = [
            lambda: ServiceConfig(name="", version="0.1.0"),
            lambda: ServiceConfig(name="bad name", version="0.1.0"),
            lambda: ServiceConfig(name="svc", version="abc"),
            lambda: ServiceConfig(name="svc", version="0.1.0", queue_group="a b"),
            lambda: EndpointConfig(name="", handler=noop_handler),
            lambda: EndpointConfig(name="a.b", handler=noop_handler),
            lambda: EndpointConfig(name="e", subject="a >.b", handler=noop_handler),
            lambda: EndpointConfig(name="e", queue_group="a b", handler=noop_handler),
        ]
        for i, case in enumerate(cases):
            with self.subTest(case=i):
                with self.assertRaises(ConfigValidationError) as ctx:
                    case()
                # Still a ValueError, as raised by earlier releases.
                self.assertIsInstance(ctx.exception, ValueError)
                self.assertIsInstance(ctx.exception, MicroError)
                self.assertIsInstance(ctx.exception, nats.errors.Error)

    def test_control_subject_errors(self):
        with self.assertRaises(ServiceNameRequiredError) as ctx:
            control_subject(ServiceVerb.PING, name="", id="123")
        self.assertIsInstance(ctx.exception, ValueError)
        self.assertEqual(str(ctx.exception), "service name is required to generate ID control subject")

        with self.assertRaises(VerbNotSupportedError) as ctx:
            control_subject("FOO", name="svc")
        self.assertIsInstance(ctx.exception, ValueError)
        self.assertEqual(str(ctx.exception), 'unsupported verb: "FOO"')

        # Verbs may be given by their name too.
        self.assertEqual(control_subject("STATS", name="svc", id="1"), "$SRV.STATS.svc.1")
        self.assertEqual(nats.micro.control_subject(ServiceVerb.INFO), "$SRV.INFO")

    def test_nats_error(self):
        cause = nats.errors.SlowConsumerError(subject="foo", reply="", sid=1, sub=None)
        err = NATSError("foo.bar", "slow consumer", cause)
        self.assertEqual(err.subject, "foo.bar")
        self.assertEqual(err.description, "slow consumer")
        self.assertEqual(str(err), '"foo.bar": slow consumer')
        self.assertIs(err.unwrap(), cause)
        self.assertIs(err.__cause__, cause)
        self.assertEqual(err, NATSError("foo.bar", "slow consumer"))
        self.assertNotEqual(err, NATSError("foo.bar", "other"))
        self.assertNotEqual(err, NATSError("foo", "slow consumer"))
        self.assertIsInstance(err, MicroError)

    @async_test
    async def test_request_errors(self):
        errors = []

        async def handler(request: Request):
            for code, description in (("", "desc"), ("400", "")):
                try:
                    await request.respond_error(code, description)
                except ArgRequiredError as e:
                    errors.append(e)
            await request.respond(b"ok")

        nc = await nats.connect()
        svc = await add_service(nc, name="svc", version="0.1.0")
        await svc.add_endpoint(name="e", subject="svc.e", handler=handler)

        resp = await nc.request("svc.e", b"", timeout=1)
        self.assertEqual(resp.data, b"ok")
        self.assertEqual(
            [str(e) for e in errors],
            ["argument required: error code", "argument required: description"],
        )
        self.assertTrue(all(isinstance(e, ValueError) for e in errors))

        # A request without a reply subject cannot be responded to.
        failures = []
        done = asyncio.Event()

        async def no_reply_handler(request: Request):
            try:
                await request.respond(b"ok")
            except RespondError as e:
                failures.append(e)
            done.set()

        await svc.add_endpoint(name="noreply", subject="svc.noreply", handler=no_reply_handler)
        await nc.publish("svc.noreply", b"")
        await asyncio.wait_for(done.wait(), 1)
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0], ValueError)
        self.assertTrue(str(failures[0]).startswith("NATS error when sending response"))

        await svc.stop()
        await nc.close()
