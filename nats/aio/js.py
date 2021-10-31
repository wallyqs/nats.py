# Copyright 2016-2021 The NATS Authors
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
import nats.aio.client
import nats.aio.errors

class JetStream():
    """
    JetStream returns a context that can be used to produce and consume
    messages from NATS JetStream.
    """    

    def __init__(self, conn=None, prefix=_JS.DEFAULT_JS_API_PREFIX):
        self._prefix = prefix
        self._nc = conn
        self._jsm = JetStream._JS(conn, prefix)

    async def pull_subscribe(
        self,
        subject: str,
        durable: str,
        stream: str = None,
    ):
        """
        pull_subscribe returns a Subscription that can be delivered messages
        from a JetStream pull based consumer by calling `sub.fetch`.

        In case 'stream' is passed, there will not be a lookup of the stream
        based on the subject.
        """
        if stream is None:
            stream = await self._jsm._lookup_stream_by_subject(subject)

        # Lookup for the consumer based on the durable name.
        cinfo = await self._jsm._consumer_info(stream, durable)
        found = Falseg
        if 'error' in cinfo and cinfo['error']['code'] == 404:
            # {'code': 404, 'err_code': 10014, 'description': 'consumer not found'}
            found = False
        else:
            found = True

        return sub

    class _Sub():
        def __init__(self, js=None):
            self._js = js
            self._nc = js._nc
            self._rpre = None
            self._psub = None
            self._freqs = None
            self._stream = None
            self._consumer = None

        async def fetch(self, batch: int = 1, timeout: float = 5.0):
            prefix = self._js._prefix
            stream = self._stream
            consumer = self._consumer

            msgs = []

            # Use async handler for single response.
            if batch == 1:
                # Setup inbox to wait for the response.
                inbox = self._rpre[:]
                inbox.extend(b'.')
                inbox.extend(self._nc._nuid.next())
                subject = f"{prefix}.CONSUMER.MSG.NEXT.{stream}.{consumer}"

                # Publish the message and wait for the response.
                msg = None
                future = asyncio.Future()
                await self._freqs.put(future)
                req = json.dumps({"no_wait": True, "batch": 1})
                await self._nc.publish(
                    subject,
                    req.encode(),
                    reply=inbox.decode(),
                )

                try:
                    msg = await asyncio.wait_for(future, timeout)
                except asyncio.TimeoutError:
                    # Cancel the future and try with longer request.
                    future.cancel()

                if msg is not None:
                    if msg.headers is not None and msg.headers[
                            nats.aio.client.STATUS_HDR] == NO_MSGS_STATUS:
                        # Now retry with the old style request and set it
                        # to expire 100ms before the timeout.
                        expires = (timeout * 1_000_000_000) - 10_000_000
                        req = json.dumps({"batch": 1, "expires": int(expires)})
                        msg = await self._nc.request(
                            subject,
                            req.encode(),
                            old_style=True,
                        )
                        _check_js_msg(msg)

                msgs.append(msg)
                return msgs

            return msgs

    class _JS():
        LAST_CONSUMER_SEQ_HDR = "Nats-Last-Consumer"
        LAST_STREAM_SEQ_HDR = "Nats-Last-Stream"
        NO_MSGS_STATUS = "404"
        CTRL_MSG_STATUS = "100"
        DEFAULT_JS_API_PREFIX = "$JS.API"
        INBOX_PREFIX = bytearray(b'_INBOX.')

        # AckPolicy
        AckExplicit = "explicit"
        AckAll = "all"
        AckNone = "none"

        # DeliverPolicy
        DeliverAll = "all"
        DeliverLast = "lastt"
        DeliverNew = "new"
        DeliverByStart = "by_start_sequence"
        DeliverByStartTime = "by_start_time"

        # ReplayPolicy
        ReplayInstantPolicy = "instant"
        ReplayOriginal = "original"

        def __init__(self, conn=None, prefix=DEFAULT_JS_API_PREFIX):
            self._prefix = prefix
            self._nc = conn

        async def _account_info(self):
            msg = await self._nc.request(f"{self._prefix}.INFO")
            account_info = json.loads(msg.data)
            return account_info

        async def _lookup_stream_by_subject(self, subject: str):
            req_sub = f"{self._prefix}.STREAM.NAMES"
            req_data = json.dumps({"subject": subject})
            msg = await self._nc.request(req_sub, req_data.encode())
            info = json.loads(msg.data)
            return info['streams'][0]

        async def _add_stream(self, config={}):
            name = config["name"]
            data = json.dumps(config)
            msg = await self._nc.request(
                f"{self._prefix}.STREAM.CREATE.{name}", data.encode()
            )
            return msg

        async def _consumer_info(self, stream=None, consumer=None):
            msg = None
            msg = await self._nc.request(
                f"{self._prefix}.CONSUMER.INFO.{stream}.{consumer}"
            )
            result = json.loads(msg.data)
            return result

        async def _create_consumer(
            self,
            stream: str = None,
            durable: str = None,
            deliver_subject: str = None,
            deliver_policy: str = None,
            opt_start_seq: int = None,
            opt_start_time: int = None,
            ack_policy: str = None,
            max_deliver: int = None,
            filter_subject: str = None,
            replay_policy: str = None,
            max_ack_pending: int = None,
            ack_wait: int = None,
            num_waiting: int = None
        ):
            config = {
                "durable_name": durable,
                "deliver_subject": deliver_subject,
                "deliver_policy": deliver_policy,
                "opt_start_seq": opt_start_seq,
                "opt_start_time": opt_start_time,
                "ack_policy": ack_policy,
                "ack_wait": ack_wait,
                "max_deliver": max_deliver,
                "filter_subject": filter_subject,
                "replay_policy": replay_policy,
                "max_ack_pending": max_ack_pending,
                "num_waiting": num_waiting
            }

            # Cleanup empty values.
            for k, v in dict(config).items():
                if v is None:
                    del config[k]
            req = {"stream_name": stream, "config": config}
            req_data = json.dumps(req).encode()

            msg = None
            if durable is not None:
                msg = await self._nc.request(
                    f"{self._prefix}.CONSUMER.DURABLE.CREATE.{stream}.{durable}",
                    req_data
                )
            else:
                msg = await self._nc.request(
                    f"{self._prefix}.CONSUMER.CREATE.{stream}", req_data
                )

            response = json.loads(msg.data)
            if 'error' in response and response['error']['code'] >= 400:
                err = response['error']
                code = err['code']
                err_code = err['err_code']
                description = err['description']
                raise nats.aio.errors.JetStreamAPIError(
                    code=code,
                    err_code=err_code,
                    description=description,
                    )
            return response
