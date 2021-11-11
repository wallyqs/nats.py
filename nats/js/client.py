# Copyright 2021 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import asyncio
import json
import time
import nats.errors
import nats.js.errors
from nats.js.manager import JetStreamManager
from nats.js import api
from typing import Any, Dict, List, Optional
from dataclasses import asdict


class JetStream:
    """
    JetStream returns a context that can be used to produce and consume
    messages from NATS JetStream.
    """
    def __init__(
        self,
        conn,
        prefix=api.DefaultPrefix,
        domain=None,
        timeout=5,
    ):
        self._prefix = prefix
        if domain is not None:
            self._prefix = f"$JS.{domain}.API"
        self._nc = conn
        self._timeout = timeout
        self._jsm = JetStreamManager(
            conn, prefix=prefix, domain=domain, timeout=timeout
        )

    async def publish(
        self,
        subject: str,
        payload: bytes = b'',
        timeout: float = None,
        stream: str = None,
        headers: dict = None
    ) -> api.PubAck:
        """
        publish emits a new message to JetStream.
        """
        hdr = headers
        if timeout is None:
            timeout = self._timeout
        if stream is not None:
            if headers is None:
                hdr = {}
            hdr[nats.js.api.ExpectedStreamHdr] = stream

        try:
            msg = await self._nc.request(
                subject, payload, timeout=timeout, headers=hdr
            )
        except nats.errors.NoRespondersError:
            raise nats.js.errors.NoStreamResponseError

        resp = json.loads(msg.data)
        if 'error' in resp:
            raise nats.js.errors.APIError.from_error(resp['error'])

        return api.PubAck.loads(**resp)

    async def subscribe(
        self,
        subject: str,
        queue: Optional[str] = None,
        cb=None,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: api.ConsumerConfig = None,
        ):
        """
        subscribe returns a `Subscription` that is bound to a push based consumer.

        :param subject: Subject from a stream from JetStream.
        :param queue: Deliver group name from a set a of queue subscribers.
        :param durable: Name of the durable consumer to which the the subscription should be bound.
        :param stream: Name of the stream to which the subscription should be bound.
        """
        if stream is None:
            stream = await self._jsm.find_stream_name_by_subject(subject)

        try:
            # TODO: Detect configuration drift with the consumer.
            if queue is not None:
                durable = queue
            cinfo = await self._jsm.consumer_info(stream, durable)

        except nats.js.errors.NotFoundError:
            # If not found then attempt to create a consumer.
            if config is None:
                # Defaults
                config = api.ConsumerConfig(
                    ack_policy=api.AckPolicy.explicit,
                )             
            elif isinstance(config, dict):
                config = api.ConsumerConfig.loads(**config)
            elif not isinstance(config, api.ConsumerConfig):
                raise ValueError("nats: invalid ConsumerConfig")

            if config.durable_name is None:
                config.durable_name = durable
            if config.deliver_group is None:
                config.deliver_group = queue

            deliver = self._nc.new_inbox()
            config.deliver_subject = deliver

            await self._jsm.add_consumer(stream, config=config)

        pass

    async def pull_subscribe(
        self,
        subject: str,
        durable: str,
        stream: str = None,
        config: api.ConsumerConfig = None,
    ):
        """
        pull_subscribe returns a `PullSubscription` that can be delivered messages
        from a JetStream pull based consumer by calling `sub.fetch`.

        In case 'stream' is passed, there will not be a lookup of the stream
        based on the subject.
        """
        if stream is None:
            stream = await self._jsm.find_stream_name_by_subject(subject)

        try:
            # TODO: Detect configuration drift with the consumer.
            await self._jsm.consumer_info(stream, durable)
        except nats.js.errors.NotFoundError:
            # If not found then attempt to create with the defaults.
            if config is None:
                # Defaults
                config = api.ConsumerConfig(
                    ack_policy=api.AckPolicy.explicit,
                )
            elif isinstance(config, dict):
                config = api.ConsumerConfig.loads(**config)
            elif not isinstance(config, api.ConsumerConfig):
                raise ValueError("nats: invalid ConsumerConfig")

            config.durable_name = durable
            await self._jsm.add_consumer(stream, config=config)

        # FIXME: Make this inbox prefix customizable.
        deliver = api.InboxPrefix[:]
        deliver.extend(self._nc._nuid.next())

        consumer = durable
        sub = await self._nc.subscribe(deliver.decode())
        return JetStream.PullSubscription(self, sub, stream, consumer, deliver)

    @classmethod
    def is_status_msg(cls, msg):
        if msg is not None and \
           msg.header is not None and \
           api.StatusHdr in msg.header:
            return True
        else:
            return False

    class PullSubscription:
        """
        PullSubscription is a subscription that can fetch messages.
        """
        def __init__(self, js, sub, stream, consumer, deliver):
            # JS/JSM context
            self._js = js
            self._nc = js._nc

            # NATS Subscription
            self._sub = sub
            self._stream = stream
            self._consumer = consumer
            prefix = self._js._prefix
            self._nms = f'{prefix}.CONSUMER.MSG.NEXT.{stream}.{consumer}'
            self._deliver = deliver.decode()

        async def unsubscribe():
            """
            unsubscribe destroys de inboxes of the pull subscription making it
            unable to continue to receive messages.
            """
            if self._sub is None:
                raise ValueError("nats: invalid subscription")

            await self._sub.unsubscribe()
            self._sub = None

        async def fetch(self, batch: int = 1, timeout: int = 5):
            if self._sub is None:
                raise ValueError("nats: invalid subscription")

            # FIXME: Check connection is not closed, etc...

            if batch < 1:
                raise ValueError("nats: invalid batch size")
            if timeout <= 0:
                raise ValueError("nats: invalid fetch timeout")

            msgs = []
            expires = (timeout * 1_000_000_000) - 100_000
            if batch == 1:
                msg = await self._fetch_one(batch, expires, timeout)
                msgs.append(msg)
            else:
                msgs = await self._fetch_n(batch, expires, timeout)
            return msgs

        async def _fetch_one(self, batch, expires, timeout):
            queue = self._sub._pending_queue

            # Check the next message in case there are any.
            if not queue.empty():
                try:
                    msg = queue.get_nowait()
                    return msg
                except:
                    # Fallthrough to make request in case this fails.
                    pass

            # Make lingering request with expiration.
            next_req = {}
            next_req['batch'] = 1
            next_req['expires'] = expires

            # Make publish request and wait for response.
            await self._nc.publish(
                self._nms,
                json.dumps(next_req).encode(),
                self._deliver,
            )

            # Wait for the response.
            msg = None
            try:
                fut = queue.get()
                msg = await asyncio.wait_for(fut, timeout=timeout)
            except asyncio.TimeoutError:
                raise nats.errors.TimeoutError

            # Should have received at least a message at this point,
            # if that is not the case then error already.
            if JetStream.is_status_msg(msg):
                if api.StatusHdr in msg.headers:
                    raise nats.js.errors.APIError.from_msg(msg)

            return msg

        async def _fetch_n(self, batch, expires, timeout):
            # TODO: Implement fetching more than one.
            raise NotImplementedError

    class _JS():
        def __init__(
            self,
            conn=None,
            prefix=None,
            stream=None,
            consumer=None,
            nms=None,
        ):
            self._prefix = prefix
            self._nc = conn
            self._stream = stream
            self._consumer = consumer
            self._nms = nms


class JetStreamContext(JetStream, JetStreamManager):
    """
    JetStreamContext includes both the JetStream and JetStream Manager.
    """
    def __init__(self, conn, **opts):
        super().__init__(conn, **opts)
