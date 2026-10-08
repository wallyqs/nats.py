import asyncio
import json

import nats
import nats.errors
import nats.micro
from nats.micro import add_service
from nats.micro.errors import (
    ArgRequiredError,
    ConfigValidationError,
    MarshalResponseError,
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


class MicroRequestTest(SingleServerTestCase):
    @async_test
    async def test_respond_json_and_reply(self):
        replies = []
        failures = []

        async def handler(request: Request):
            replies.append(request.reply)
            if request.data == b"bad":
                try:
                    await request.respond_json(object())
                except MarshalResponseError as e:
                    failures.append(e)
                await request.respond(b"fallback")
                return
            await request.respond_json({"a": [1, 2], "b": "c"}, headers={"X-Key": "v"})

        nc = await nats.connect()
        svc = await add_service(nc, name="svc", version="0.1.0")
        await svc.add_endpoint(name="e", subject="svc.e", handler=handler)

        resp = await nc.request("svc.e", b"", timeout=1)
        self.assertEqual(resp.data, b'{"a":[1,2],"b":"c"}')
        self.assertEqual(json.loads(resp.data), {"a": [1, 2], "b": "c"})
        self.assertEqual(resp.headers, {"X-Key": "v"})

        resp = await nc.request("svc.e", b"bad", timeout=1)
        self.assertEqual(resp.data, b"fallback")
        self.assertEqual(len(failures), 1)
        self.assertEqual(str(failures[0]), "marshaling response")
        self.assertIsInstance(failures[0].__cause__, TypeError)

        # The reply accessor is the request's reply subject.
        inbox = nc.new_inbox()
        sub = await nc.subscribe(inbox)
        await nc.publish("svc.e", b"", reply=inbox)
        msg = await sub.next_msg(timeout=1)
        self.assertEqual(json.loads(msg.data), {"a": [1, 2], "b": "c"})
        self.assertEqual(replies[-1], inbox)
        self.assertTrue(all(r.startswith("_INBOX.") for r in replies))

        await svc.stop()
        await nc.close()

    @async_test
    async def test_respond_error_headers_override(self):
        async def handler(request: Request):
            await request.respond_error(
                "400",
                "bad request",
                b"details",
                headers={"Nats-Service-Error-Code": "401", "X-Key": "v"},
            )

        nc = await nats.connect()
        svc = await add_service(nc, name="svc", version="0.1.0")
        await svc.add_endpoint(name="e", subject="svc.e", handler=handler)

        resp = await nc.request("svc.e", b"", timeout=1)
        self.assertEqual(resp.data, b"details")
        # User headers are applied after the error headers, as in nats.go.
        self.assertEqual(resp.headers["Nats-Service-Error-Code"], "401")
        self.assertEqual(resp.headers["Nats-Service-Error"], "bad request")
        self.assertEqual(resp.headers["X-Key"], "v")
        stats = svc.stats()
        self.assertEqual(stats.endpoints[0].num_errors, 1)
        self.assertEqual(stats.endpoints[0].last_error, "400:bad request")

        await svc.stop()
        await nc.close()
