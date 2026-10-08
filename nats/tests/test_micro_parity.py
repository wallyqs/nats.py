import asyncio
import dataclasses
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
    Endpoint,
    EndpointConfig,
    EndpointStats,
    ServiceConfig,
    ServiceStats,
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


async def count_responses(nc, subject, wait=0.3):
    inbox = nc.new_inbox()
    sub = await nc.subscribe(inbox)
    await nc.publish(subject, b"", reply=inbox)
    await nc.flush()
    await asyncio.sleep(wait)
    count = sub.pending_msgs
    await sub.unsubscribe()
    return count


class MicroQueueGroupTest(SingleServerTestCase):
    @async_test
    async def test_queue_group_disabled(self):
        async def handler(request: Request):
            await request.respond(b"ok")

        nc = await nats.connect()
        services = []
        for _ in range(2):
            svc = await add_service(nc, name="svc", version="0.1.0", queue_group_disabled=True)
            # Inherits the service's disabled queue group.
            await svc.add_endpoint(name="plain", handler=handler)
            # Its own queue group wins over the service's disabled flag.
            await svc.add_endpoint(name="queued", queue_group="custom", handler=handler)

            group = svc.add_group(name="g", queue_group="gq")
            # Disabled on the endpoint only.
            await group.add_endpoint(name="noqueue", queue_group_disabled=True, handler=handler)
            await group.add_endpoint(name="grouped", handler=handler)

            disabled_group = svc.add_group(name="d")
            await disabled_group.add_endpoint(name="inherited", handler=handler)
            nested = disabled_group.add_group(name="n", queue_group="nq")
            await nested.add_endpoint(name="nested", handler=handler)
            services.append(svc)

        info = {e.subject: e.queue_group for e in services[0].info().endpoints}
        self.assertEqual(
            info,
            {
                "plain": "",
                "queued": "custom",
                "g.noqueue": "",
                "g.grouped": "gq",
                "d.inherited": "",
                "d.n.nested": "nq",
            },
        )
        stats = {e.subject: e.queue_group for e in services[0].stats().endpoints}
        self.assertEqual(stats, info)

        expected = {
            "plain": 2,
            "queued": 1,
            "g.noqueue": 2,
            "g.grouped": 1,
            "d.inherited": 2,
            "d.n.nested": 1,
        }
        for subject, count in expected.items():
            with self.subTest(subject=subject):
                self.assertEqual(await count_responses(nc, subject), count)

        for svc in services:
            await svc.stop()
        await nc.close()

    @async_test
    async def test_queue_group_default(self):
        async def handler(request: Request):
            await request.respond(b"ok")

        nc = await nats.connect()
        services = []
        for _ in range(2):
            svc = await add_service(nc, name="svc", version="0.1.0")
            await svc.add_endpoint(EndpointConfig(name="e", handler=handler, queue_group_disabled=True))
            group = svc.add_group(name="g", queue_group_disabled=True)
            await group.add_endpoint(name="e", handler=handler)
            await group.add_endpoint(name="q", queue_group="q2", handler=handler)
            await svc.add_endpoint(name="default", handler=handler)
            services.append(svc)

        self.assertEqual(await count_responses(nc, "e"), 2)
        self.assertEqual(await count_responses(nc, "g.e"), 2)
        self.assertEqual(await count_responses(nc, "g.q"), 1)
        self.assertEqual(await count_responses(nc, "default"), 1)
        info = {e.subject: e.queue_group for e in services[1].info().endpoints}
        self.assertEqual(info, {"e": "", "g.e": "", "g.q": "q2", "default": "q"})

        for svc in services:
            await svc.stop()
        await nc.close()


class MicroEndpointOptionsTest(SingleServerTestCase):
    def test_pending_limits_validation(self):
        with self.assertRaises(ConfigValidationError) as ctx:
            EndpointConfig(name="e", handler=noop_handler, pending_msgs_limit=0, pending_bytes_limit=0)
        self.assertEqual(str(ctx.exception), "at least one pending limit must be non-zero")
        with self.assertRaises(ConfigValidationError):
            EndpointConfig(name="e", handler=noop_handler, pending_msgs_limit=0, pending_bytes_limit=10)
        EndpointConfig(name="e", handler=noop_handler, pending_msgs_limit=-1, pending_bytes_limit=10)
        EndpointConfig(name="e", handler=noop_handler, pending_msgs_limit=5)

    @async_test
    async def test_pending_limits(self):
        slow = []

        async def error_cb(e):
            if isinstance(e, nats.errors.SlowConsumerError):
                slow.append(e)

        release = asyncio.Event()

        async def handler(request: Request):
            await release.wait()
            await request.respond(b"ok")

        nc = await nats.connect(error_cb=error_cb)
        svc = await add_service(nc, name="svc", version="0.1.0")
        await svc.add_endpoint(name="limited", handler=handler, pending_msgs_limit=1, pending_bytes_limit=-1)
        await svc.add_endpoint(name="unlimited", handler=handler, pending_msgs_limit=-1, pending_bytes_limit=-1)

        subs = {e._subject: e._subscription for e in svc._endpoints}
        self.assertEqual(subs["limited"]._pending_msgs_limit, 1)
        self.assertEqual(subs["limited"]._pending_bytes_limit, 0)
        self.assertEqual(subs["unlimited"]._pending_msgs_limit, 0)

        for _ in range(5):
            await nc.publish("limited", b"x")
            await nc.publish("unlimited", b"x")
        await nc.flush()
        await asyncio.sleep(0.2)
        self.assertTrue(slow)
        self.assertTrue(all(e.subject == "limited" for e in slow))

        release.set()
        await svc.stop()
        await nc.close()

    @async_test
    async def test_metadata_key(self):
        config = EndpointConfig(name="e", handler=noop_handler, metadata={"a": "1"})
        updated = config.with_metadata_key("b", "2").with_metadata_key("a", "3")
        self.assertEqual(config.metadata, {"a": "1"})
        self.assertEqual(updated.metadata, {"a": "3", "b": "2"})
        self.assertEqual(
            EndpointConfig(name="e", handler=noop_handler).with_metadata_key("k", "v").metadata,
            {"k": "v"},
        )

        nc = await nats.connect()
        svc = await add_service(nc, name="svc", version="0.1.0")
        await svc.add_endpoint(updated)
        info = await nc.request(control_subject(ServiceVerb.INFO, "svc"), b"", timeout=1)
        self.assertEqual(json.loads(info.data)["endpoints"][0]["metadata"], {"a": "3", "b": "2"})
        await svc.stop()
        await nc.close()


class MicroEndpointTest(SingleServerTestCase):
    @async_test
    async def test_endpoint_accessors(self):
        nc = await nats.connect()
        svc = await add_service(nc, name="svc", version="0.1.0", queue_group="sq")

        endpoint = await svc.add_endpoint(name="e", handler=noop_handler, metadata={"k": "v"})
        self.assertIsInstance(endpoint, Endpoint)
        self.assertEqual(endpoint.name, "e")
        self.assertEqual(endpoint.subject, "e")
        self.assertEqual(endpoint.queue_group, "sq")
        self.assertFalse(endpoint.queue_group_disabled)
        self.assertEqual(endpoint.metadata, {"k": "v"})
        self.assertIs(endpoint.handler, noop_handler)
        self.assertIsInstance(endpoint.config, EndpointConfig)
        self.assertEqual(endpoint.config.name, "e")
        self.assertEqual(endpoint.config.subject, "e")
        self.assertEqual(endpoint.config.queue_group, "sq")
        self.assertEqual(endpoint.config.metadata, {"k": "v"})

        group = svc.add_group(name="g", queue_group_disabled=True)
        grouped = await group.add_endpoint(name="ge", subject="sub", handler=noop_handler)
        self.assertEqual(grouped.name, "ge")
        self.assertEqual(grouped.subject, "g.sub")
        self.assertEqual(grouped.config.subject, "g.sub")
        self.assertEqual(grouped.queue_group, "")
        self.assertTrue(grouped.queue_group_disabled)
        self.assertTrue(grouped.config.queue_group_disabled)

        await svc.stop()
        await nc.close()

    @async_test
    async def test_stats_handler_receives_endpoint(self):
        seen = []

        def stats_handler(stats: EndpointStats):
            # The current EndpointStats argument keeps working ...
            self.assertIsInstance(stats, EndpointStats)
            # ... and also exposes the endpoint, as nats.go passes *Endpoint.
            seen.append(stats.endpoint)
            return {"endpoint": stats.endpoint.name, "requests": stats.num_requests, **stats.endpoint.config.metadata}

        nc = await nats.connect()
        svc = await add_service(nc, name="svc", version="0.1.0", stats_handler=stats_handler)
        first = await svc.add_endpoint(name="first", handler=noop_handler, metadata={"m": "1"})
        second = await svc.add_group(name="g").add_endpoint(name="second", handler=noop_handler, metadata={"m": "2"})

        resp = await nc.request(control_subject(ServiceVerb.STATS, "svc"), b"", timeout=1)
        stats = ServiceStats.from_dict(json.loads(resp.data))
        self.assertEqual(
            [e.data for e in stats.endpoints],
            [
                {"endpoint": "first", "requests": 0, "m": "1"},
                {"endpoint": "second", "requests": 0, "m": "2"},
            ],
        )
        self.assertEqual(seen, [first, second])
        # Decoded stats have no endpoint, and the attribute is not a field.
        self.assertIsNone(stats.endpoints[0].endpoint)
        self.assertNotIn("endpoint", dataclasses.asdict(svc.stats().endpoints[0]))
        self.assertIs(svc.stats().endpoints[1].endpoint, second)

        await svc.stop()
        await nc.close()


class MicroDefaultEndpointTest(SingleServerTestCase):
    @async_test
    async def test_config_endpoint(self):
        async def handler(request: Request):
            await request.respond(b"default:" + request.data)

        nc = await nats.connect()
        svc = await add_service(
            nc,
            name="svc",
            version="0.1.0",
            queue_group="sq",
            endpoint=EndpointConfig(name="default", subject="svc.default", handler=handler, metadata={"k": "v"}),
        )

        resp = await nc.request("svc.default", b"hi", timeout=1)
        self.assertEqual(resp.data, b"default:hi")

        info = svc.info()
        self.assertEqual(len(info.endpoints), 1)
        self.assertEqual(info.endpoints[0].name, "default")
        self.assertEqual(info.endpoints[0].subject, "svc.default")
        # Inherits the service's queue group.
        self.assertEqual(info.endpoints[0].queue_group, "sq")
        self.assertEqual(info.endpoints[0].metadata, {"k": "v"})
        self.assertEqual(svc.stats().endpoints[0].num_requests, 1)

        # More endpoints can still be added.
        await svc.add_endpoint(name="other", handler=handler)
        self.assertEqual([e.name for e in svc.info().endpoints], ["default", "other"])
        await svc.stop()

        # Also when the service is started as a context manager, with the
        # endpoint's own queue group and the subject defaulting to the name.
        config = ServiceConfig(
            name="svc2",
            version="0.1.0",
            queue_group="sq",
            endpoint=EndpointConfig(name="default", queue_group="eq", handler=handler),
        )
        async with nats.micro.Service(nc, config) as svc2:
            resp = await nc.request("default", b"x", timeout=1)
            self.assertEqual(resp.data, b"default:x")
            self.assertEqual(svc2.info().endpoints[0].queue_group, "eq")

        await nc.close()


class MicroLifecycleTest(SingleServerTestCase):
    @async_test
    async def test_done_handler_on_stop(self):
        done = []

        async def async_done(svc):
            done.append(("async", svc.id))

        nc = await nats.connect()
        svc = await add_service(nc, name="svc", version="0.1.0", done_handler=async_done)
        await svc.add_endpoint(name="e", handler=noop_handler)
        await svc.stop()
        await svc.stop()
        self.assertEqual(done, [("async", svc.id)])
        self.assertTrue(svc.stopped.is_set())

        # A plain function works too.
        svc2 = await add_service(nc, name="svc", version="0.1.0", done_handler=lambda s: done.append(("sync", s.id)))
        await svc2.stop()
        self.assertEqual(done[-1], ("sync", svc2.id))
        await nc.close()

    @async_test
    async def test_connection_close_stops_service(self):
        events = []

        async def closed_cb():
            events.append("closed_cb")

        nc = await nats.connect(closed_cb=closed_cb)
        services = []
        for i in range(2):
            svc = await add_service(
                nc,
                name=f"svc{i}",
                version="0.1.0",
                done_handler=lambda s, i=i: events.append(f"done{i}"),
            )
            await svc.add_endpoint(name="e", subject=f"svc{i}.e", handler=noop_handler)
            services.append(svc)

        await nc.close()
        self.assertTrue(all(svc.stopped.is_set() for svc in services))
        self.assertEqual(sorted(events[:2]), ["done0", "done1"])
        # The connection's own closed callback still runs, after the services stop.
        self.assertEqual(events[2:], ["closed_cb"])

    @async_test
    async def test_stop_restores_connection_callbacks(self):
        async def closed_cb():
            pass

        async def error_cb(e):
            pass

        nc = await nats.connect(closed_cb=closed_cb, error_cb=error_cb)
        svc = await add_service(nc, name="svc", version="0.1.0")
        self.assertIsNot(nc._closed_cb, closed_cb)
        self.assertIsNot(nc._error_cb, error_cb)
        await svc.stop()
        self.assertIs(nc._closed_cb, closed_cb)
        self.assertIs(nc._error_cb, error_cb)

        # Stopped out of order, the remaining service still stops on close.
        done = []
        first = await add_service(nc, name="a", version="0.1.0", done_handler=lambda s: done.append("a"))
        second = await add_service(nc, name="b", version="0.1.0", done_handler=lambda s: done.append("b"))
        await first.stop()
        await nc.close()
        self.assertTrue(second.stopped.is_set())
        self.assertEqual(done, ["a", "b"])

    @async_test
    async def test_error_handler_on_slow_consumer(self):
        errors = []
        conn_errors = []
        done = asyncio.Event()

        async def error_cb(e):
            conn_errors.append(e)

        def error_handler(svc, err):
            errors.append(err)

        release = asyncio.Event()

        async def handler(request: Request):
            await release.wait()

        nc = await nats.connect(error_cb=error_cb)
        svc = await add_service(
            nc,
            name="svc",
            version="0.1.0",
            error_handler=error_handler,
            done_handler=lambda s: done.set(),
        )
        endpoint = await svc.add_endpoint(name="e", subject="svc.e", handler=handler, pending_msgs_limit=1)
        await svc.add_endpoint(name="other", subject="svc.other", handler=noop_handler)

        # An error on a subscription that is not the service's is not its business.
        other = await nc.subscribe("not.svc", cb=handler, pending_msgs_limit=1)
        for _ in range(4):
            await nc.publish("not.svc", b"x")
        await nc.flush()
        await asyncio.sleep(0.1)
        self.assertFalse(svc.stopped.is_set())
        self.assertEqual(errors, [])
        self.assertTrue(conn_errors)
        self.assertTrue(all(e.sub is other for e in conn_errors))
        conn_errors.clear()

        for _ in range(4):
            await nc.publish("svc.e", b"x")
        await nc.flush()
        await asyncio.wait_for(done.wait(), 2)

        self.assertTrue(svc.stopped.is_set())
        self.assertTrue(errors)
        self.assertIsInstance(errors[0], NATSError)
        self.assertEqual(errors[0].subject, "svc.e")
        self.assertIsInstance(errors[0].unwrap(), nats.errors.SlowConsumerError)
        self.assertEqual(errors[0].description, str(errors[0].unwrap()))
        # The error is counted on the endpoint, then the connection's own
        # error callback still receives it.
        self.assertGreaterEqual(endpoint._num_errors, 1)
        self.assertEqual(endpoint._last_error, errors[0].description)
        await asyncio.sleep(0.05)
        self.assertTrue(
            any(isinstance(e, nats.errors.SlowConsumerError) and e.sub.subject == "svc.e" for e in conn_errors)
        )

        release.set()
        self.assertFalse(nc.is_closed)
        await nc.close()
