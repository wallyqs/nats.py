import asyncio
import http.client
import json
import ssl
import time
import unittest
from unittest import mock

import nats
from nats.aio.client import Client as NATS
from nats.aio.client import __version__
from nats.aio.errors import ErrConnectionClosed, ErrNoServers, ErrTimeout, \
    ErrBadSubject, ErrConnectionDraining, ErrDrainTimeout, NatsError, ErrInvalidCallbackType
from nats.aio.utils import new_inbox, INBOX_PREFIX
from tests.utils import async_test, SingleServerTestCase, MultiServerAuthTestCase, MultiServerAuthTokenTestCase, \
    TLSServerTestCase, \
    MultiTLSServerAuthTestCase, ClusteringTestCase, ClusteringDiscoveryAuthTestCase

from tests.utils import SingleJetStreamServerTestCase

class ClientJetStreamTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_check_account_info(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()
        info = await js.account_info()
        self.assertTrue(len(info["limits"]) > 0)

        await nc.close()

    @async_test
    async def test_add_stream(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        result = await js.add_stream(name="TEST")
        info = await js.account_info()
        self.assertTrue(info['streams'] > 1)

        await nc.close()

    @async_test
    async def test_add_consumer(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(name="lacroix")

        # type ConsumerConfig struct {
        # 	Durable         string        `json:"durable_name,omitempty"`
        # 	DeliverSubject  string        `json:"deliver_subject,omitempty"`
        # 	DeliverPolicy   DeliverPolicy `json:"deliver_policy"`
        # 	OptStartSeq     uint64        `json:"opt_start_seq,omitempty"`
        # 	OptStartTime    *time.Time    `json:"opt_start_time,omitempty"`
        # 	AckPolicy       AckPolicy     `json:"ack_policy"`
        # 	AckWait         time.Duration `json:"ack_wait,omitempty"`
        # 	MaxDeliver      int           `json:"max_deliver,omitempty"`
        # 	FilterSubject   string        `json:"filter_subject,omitempty"`
        # 	ReplayPolicy    ReplayPolicy  `json:"replay_policy"`
        # 	RateLimit       uint64        `json:"rate_limit_bps,omitempty"` // Bits per sec
        # 	SampleFrequency string        `json:"sample_freq,omitempty"`
        # 	MaxWaiting      int           `json:"max_waiting,omitempty"`
        # 	MaxAckPending   int           `json:"max_ack_pending,omitempty"`
        # 	FlowControl     bool          `json:"flow_control,omitempty"`
        # 	Heartbeat       time.Duration `json:"idle_heartbeat,omitempty"`
        # }
        await js.add_consumer(stream_name="lacroix", config={
            "durable_name": "mochi",
            "ack_policy": "explicit"
            })
        await nc.publish("lacroix", b"pepe")

        subject = "$JS.API.CONSUMER.MSG.NEXT.lacroix.mochi"

        msgs = []
        inbox = nc.new_inbox()
        future = asyncio.Future()

        async def receive(msg):
            if future.done():
                return

            msgs.append(msg)
            future.set_result(True)
        
        sub = await nc.subscribe(inbox, cb=receive)

        # Fetch request
        await nc.publish(subject, b'', reply=inbox)
        await asyncio.wait_for(future, 1)


        self.assertTrue(len(msgs) > 0)

        msg = msgs[0]
        #print(msg.data)

        info = await js.account_info()
        #print(info)

        self.assertTrue(info["consumers"] ==1)
        await nc.close()            

if __name__ == '__main__':
    import sys
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
