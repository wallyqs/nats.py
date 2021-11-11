import asyncio
import http.client
import json
import ssl
import time
import unittest
import datetime
from unittest import mock
import tempfile
import shutil

import nats
import nats.js.api
from nats.aio.client import Client as NATS
from nats.aio.client import __version__
from nats.errors import *
from nats.aio.errors import *
from nats.js.errors import *
from tests.utils import *


class PublishTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_publish(self):
        nc = NATS()
        await nc.connect()
        js = nc.jetstream()

        with self.assertRaises(NoStreamResponseError):
            await js.publish("foo", b'bar')

        await js.add_stream(name="QUUX", subjects=["quux"])

        ack = await js.publish("quux", b'bar:1', stream="QUUX")
        self.assertEqual(ack.stream, "QUUX")
        self.assertEqual(ack.seq, 1)

        ack = await js.publish("quux", b'bar:2')
        self.assertEqual(ack.stream, "QUUX")
        self.assertEqual(ack.seq, 2)

        with self.assertRaises(BadRequestError) as err:
            await js.publish("quux", b'bar', stream="BAR")
        self.assertEqual(err.exception.err_code, 10060)

        await nc.close()


class PullSubscribeTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_auto_create_consumer(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()
        sinfo = await js.add_stream(name="TEST2", subjects=["a2"])
        sub = await js.pull_subscribe("a2", "auto")
        await js.publish("a2", b'one')

        msgs = await sub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        self.assertEqual(msg.data, b'one')

        # Customize consumer config.
        sub = await js.pull_subscribe(
            "a2", "auto2", config=nats.js.api.ConsumerConfig(max_waiting=10)
        )
        msgs = await sub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        self.assertEqual(msg.data, b'one')

        info = await js.consumer_info("TEST2", "auto2")
        self.assertEqual(info.config.max_waiting, 10)

        await nc.close()
        await asyncio.sleep(1)

    @async_test
    async def test_fetch_one(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        sinfo = await js.add_stream(name="TEST1", subjects=["foo.1", "bar"])

        # cinfo = await js.add_consumer(
        #     "TEST1", durable_name="dur", ack_policy=api.AckPolicy.explicit
        # )

        ack = await js.publish("foo.1", f'Hello from NATS!'.encode())
        self.assertEqual(ack.stream, "TEST1")
        self.assertEqual(ack.seq, 1)

        # Bind to the consumer that is already present.
        sub = await js.pull_subscribe("foo.1", "dur")
        msgs = await sub.fetch(1)
        for msg in msgs:
            await msg.ack()

        msg = msgs[0]
        self.assertEqual(msg.metadata.sequence.stream, 1)
        self.assertEqual(msg.metadata.sequence.consumer, 1)
        self.assertTrue(datetime.datetime.now() > msg.metadata.timestamp)
        self.assertEqual(msg.metadata.num_pending, 0)
        self.assertEqual(msg.metadata.num_delivered, 1)

        with self.assertRaises(asyncio.TimeoutError):
            await sub.fetch(timeout=1)

        for i in range(0, 10):
            await js.publish(
                "foo.1", f"i:{i}".encode(), headers={'hello': 'world'}
            )

        # nak
        msgs = await sub.fetch()
        msg = msgs[0]

        # FIXME: This is failing with a Request Timeout error on CI
        info = await js.consumer_info("TEST1", "dur", timeout=1)
        self.assertEqual(msg.header, {'hello': 'world'})

        await msg.nak()

        info = await js.consumer_info("TEST1", "dur", timeout=1)
        self.assertEqual(info.stream_name, "TEST1")
        self.assertEqual(info.num_ack_pending, 1)
        self.assertEqual(info.num_redelivered, 0)

        # in_progress
        msgs = await sub.fetch()
        for msg in msgs:
            await msg.in_progress()

        # term
        msgs = await sub.fetch()
        for msg in msgs:
            await msg.term()

        info = await js.consumer_info("TEST1", "dur", timeout=1)
        self.assertEqual(info.num_ack_pending, 1)
        self.assertEqual(info.num_redelivered, 1)

        await nc.close()

    @async_test
    async def test_fetch_max_waiting(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(name="TEST3", subjects=["max"])

        sub = await js.pull_subscribe(
            "max", "example", config={'max_waiting': 3}
        )
        results = None
        try:
            results = await asyncio.gather(
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                return_exceptions=True,
            )
        except:
            pass

        err = None
        for e in results:
            if isinstance(e, asyncio.TimeoutError):
                continue
            else:
                self.assertIsInstance(e, APIError)
                err = e
                break

        # Should get at least one Request Timeout error.
        self.assertEqual(e.code, 408)
        info = await js.consumer_info("TEST3", "example")
        self.assertEqual(info.num_waiting, 3)
        await nc.close()


class JSMTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_stream_management(self):
        nc = NATS()
        await nc.connect()
        jsm = nc.jsm()

        acc = await jsm.account_info()
        self.assertIsInstance(acc, nats.js.api.AccountInfo)

        # Create stream
        stream = await jsm.add_stream(
            name="hello", subjects=["hello", "world", "hello.>"]
        )
        self.assertIsInstance(stream, nats.js.api.StreamInfo)
        self.assertIsInstance(stream.config, nats.js.api.StreamConfig)
        self.assertEqual(stream.config.name, "hello")
        self.assertIsInstance(stream.state, nats.js.api.StreamState)

        # Get info
        current = await jsm.stream_info("hello")
        stream.did_create = None
        self.assertEqual(stream, current)

        self.assertIsInstance(current, nats.js.api.StreamInfo)
        self.assertIsInstance(current.config, nats.js.api.StreamConfig)
        self.assertEqual(current.config.name, "hello")
        self.assertIsInstance(current.state, nats.js.api.StreamState)

        # Send messages
        producer = nc.jetstream()
        ack = await producer.publish('world', b'Hello world!')
        self.assertEqual(ack.stream, "hello")
        self.assertEqual(ack.seq, 1)

        current = await jsm.stream_info("hello")
        self.assertEqual(current.state.messages, 1)
        self.assertEqual(current.state.bytes, 47)

        # Delete stream
        is_deleted = await jsm.delete_stream("hello")
        self.assertTrue(is_deleted)

        # Not foundError since there is none
        with self.assertRaises(NotFoundError):
            await jsm.stream_info("hello")

        await nc.close()

    @async_test
    async def test_consumer_management(self):
        nc = NATS()
        await nc.connect()
        jsm = nc.jsm()

        acc = await jsm.account_info()
        self.assertIsInstance(acc, nats.js.api.AccountInfo)

        # Create stream.
        await jsm.add_stream(name="ctests", subjects=["a", "b", "c.>"])

        # Create durable consumer.
        cinfo = await jsm.add_consumer(
            "ctests",
            durable_name="dur",
            ack_policy="explicit",
        )

        # Fail with missing stream.
        with self.assertRaises(NotFoundError) as err:
            await jsm.consumer_info("missing", "c")
        self.assertEqual(err.exception.err_code, 10059)

        # Get consumer, there should be no changes.
        current = await jsm.consumer_info("ctests", "dur")
        self.assertEqual(cinfo, current)

        # Delete consumer.
        ok = await jsm.delete_consumer("ctests", "dur")
        self.assertTrue(ok)

        # Consumer lookup should not be 404 now.
        with self.assertRaises(NotFoundError) as err:
            await jsm.consumer_info("ctests", "dur")
        self.assertEqual(err.exception.err_code, 10014)

        # Create ephemeral consumer.
        cinfo = await jsm.add_consumer(
            "ctests",
            ack_policy="explicit",
            deliver_subject="asdf",
        )
        # Should not be empty.
        self.assertTrue(len(cinfo.name) > 0)
        ok = await jsm.delete_consumer("ctests", cinfo.name)
        self.assertTrue(ok)

        # Ephemeral no longer found after delete.
        with self.assertRaises(NotFoundError):
            await jsm.delete_consumer("ctests", cinfo.name)

        await nc.close()


class QueueSubscribeTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_deliver_group(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.add_stream(name="qsub", subjects=["quux"])

        async def cb1(msg):
            print(msg)
            await msg.ack()

        async def cb2(msg):
            print(msg)
            await msg.ack()

        async def cb3(msg):
            print(msg)
            await msg.ack()

        subs = []

        sub1 = await js.subscribe("quux", "wg", cb=cb1)
        sub2 = await js.subscribe("quux", "wg", cb=cb2)
        sub3 = await js.subscribe("quux", "wg", cb=cb3)

        subs.append(sub1)
        subs.append(sub2)
        subs.append(sub3)
        print(subs)


if __name__ == '__main__':
    import sys
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
