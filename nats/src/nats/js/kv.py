# Copyright 2021-2022 The NATS Authors
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

from __future__ import annotations

import asyncio
import datetime
import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional

import nats.errors
import nats.js.errors
from nats.js import api

if TYPE_CHECKING:
    from nats.js import JetStreamContext

KV_OP = "KV-Operation"
KV_DEL = "DEL"
KV_PURGE = "PURGE"
MSG_ROLLUP_SUBJECT = "sub"
# Introduced in nats-server 2.11: server-placed markers use this header instead of KV-Operation.
KV_MARKER_REASON = "Nats-Marker-Reason"

logger = logging.getLogger(__name__)


class KeyValueOp(str, Enum):
    """
    KeyValueOp is the operation of a KeyValue entry (nats.go KeyValueOp).

    The values are the KV-Operation header tokens, so they compare equal
    to Entry.operation; str() gives nats.go's names.
    """

    PUT = "PUT"
    DELETE = KV_DEL
    PURGE = KV_PURGE

    def __str__(self) -> str:
        if self is KeyValueOp.PUT:
            return "KeyValuePutOp"
        if self is KeyValueOp.DELETE:
            return "KeyValueDeleteOp"
        return "KeyValuePurgeOp"


VALID_KEY_RE = re.compile(r"^[-/_=\.a-zA-Z0-9]+$")
VALID_SEARCH_KEY_RE = re.compile(r"^[-/_=\.a-zA-Z0-9*]*[>]?$")

# ALL_KEYS is the key pattern that matches all the keys of a bucket.
ALL_KEYS = ">"


def _is_key_valid(key: str) -> bool:
    if len(key) == 0 or key[0] == "." or key[-1] == ".":
        return False
    return bool(VALID_KEY_RE.match(key))


def _is_search_key_valid(key: str) -> bool:
    if len(key) == 0 or key[0] == "." or key[-1] == ".":
        return False
    return bool(VALID_SEARCH_KEY_RE.match(key))


class StopIterSentinel:
    """A sentinel class used to indicate that iteration should stop."""

    pass


class KeyValue:
    """
    KeyValue uses the JetStream KeyValue functionality.

    ::

        import asyncio
        import nats

        async def main():
            nc = await nats.connect()
            js = nc.jetstream()

            # Create a KV
            kv = await js.create_key_value(bucket='MY_KV')

            # Set and retrieve a value
            await kv.put('hello', b'world')
            entry = await kv.get('hello')
            print(f'KeyValue.Entry: key={entry.key}, value={entry.value}')
            # KeyValue.Entry: key=hello, value=world

            await nc.close()

        if __name__ == '__main__':
            asyncio.run(main())

    """

    @dataclass
    class Entry:
        """
        An entry from a KeyValue store in JetStream.
        """

        bucket: str
        key: str
        value: Optional[bytes]
        revision: Optional[int]
        delta: Optional[int]
        created: Optional[int]
        operation: Optional[str]

        @property
        def op(self) -> KeyValueOp:
            """
            op returns the entry's operation as a KeyValueOp; unlike
            operation, which is None for a value, it is PUT then.
            """
            if self.operation == KV_DEL:
                return KeyValueOp.DELETE
            if self.operation == KV_PURGE:
                return KeyValueOp.PURGE
            return KeyValueOp.PUT

    @dataclass(frozen=True)
    class BucketStatus:
        """
        BucketStatus is the status of a KeyValue bucket.
        """

        stream_info: api.StreamInfo
        bucket: str

        @property
        def values(self) -> int:
            """
            values returns the number of stored messages in the stream.
            """
            return self.stream_info.state.messages

        @property
        def history(self) -> int:
            """
            history returns the max msgs per subject.
            """
            return self.stream_info.config.max_msgs_per_subject

        @property
        def ttl(self) -> Optional[float]:
            """
            ttl returns the max age in seconds.
            """
            if self.stream_info.config.max_age is None:
                return None
            return self.stream_info.config.max_age

        @property
        def marker_ttl(self) -> Optional[float]:
            """
            marker_ttl returns the subject delete marker TTL in seconds,
            or None if not set.
            """
            return self.stream_info.config.subject_delete_marker_ttl

        @property
        def backing_store(self) -> str:
            """
            backing_store returns the name of the store backing the bucket.
            """
            return "JetStream"

        @property
        def is_compressed(self) -> bool:
            """
            is_compressed tells whether the bucket's data is compressed.
            """
            compression = self.stream_info.config.compression
            return compression is not None and compression != api.StoreCompression.NONE

        @property
        def metadata(self) -> Optional[Dict[str, str]]:
            """
            metadata returns the user metadata of the bucket.
            """
            return self.stream_info.config.metadata

        @property
        def config(self) -> api.KeyValueConfig:
            """
            config returns the bucket's configuration, read back from its stream.
            """
            sc = self.stream_info.config
            return api.KeyValueConfig(
                bucket=self.bucket,
                description=sc.description,
                max_value_size=sc.max_msg_size,
                history=sc.max_msgs_per_subject,
                ttl=sc.max_age,
                max_bytes=sc.max_bytes,
                storage=sc.storage,
                replicas=sc.num_replicas if sc.num_replicas is not None else 1,
                placement=sc.placement,
                republish=sc.republish,
                direct=sc.allow_direct,
                limit_marker_ttl=sc.subject_delete_marker_ttl,
                compression=self.is_compressed,
                mirror=sc.mirror,
                sources=sc.sources,
                metadata=sc.metadata,
            )

        @property
        def bytes(self) -> int:
            """
            bytes returns the size of the bucket in bytes.
            """
            return self.stream_info.state.bytes

    def __init__(
        self,
        name: str,
        stream: str,
        pre: str,
        js: JetStreamContext,
        direct: bool,
        put_pre: Optional[str] = None,
        use_js_prefix: bool = True,
    ) -> None:
        self._name = name
        self._stream = stream
        self._pre = pre
        self._js = js
        self._direct = direct

        # KV mutations publish through the JetStream API prefix when the
        # context targets a non-default domain (nats.go useJSPfx behavior):
        # "$JS.<domain>.API.$KV.<bucket>.<key>". Reads and watchers keep the
        # local data subject since stream subjects are not prefixed.
        # A bucket that mirrors another one writes to the mirrored bucket
        # (put_pre), already qualified when that bucket is in another domain.
        self._mutation_pre = pre if put_pre is None else put_pre
        if use_js_prefix and js._prefix != api.DEFAULT_PREFIX:
            self._mutation_pre = f"{js._prefix}.{self._mutation_pre}"

    @property
    def bucket(self) -> str:
        """
        bucket returns the name of the bucket.
        """
        return self._name

    async def get(self, key: str, revision: Optional[int] = None, validate_keys: bool = True) -> Entry:
        """
        get returns the latest value for the key.
        """
        if validate_keys and not _is_key_valid(key):
            raise nats.js.errors.InvalidKeyError

        entry = None
        try:
            entry = await self._get(key, revision)
        except nats.js.errors.KeyDeletedError as err:
            raise nats.js.errors.KeyNotFoundError(err.entry, err.op)
        return entry

    async def _get(self, key: str, revision: Optional[int] = None) -> Entry:
        msg = None
        subject = f"{self._pre}{key}"
        try:
            if revision:
                msg = await self._js.get_msg(
                    self._stream,
                    seq=revision,
                    direct=self._direct,
                )
            else:
                msg = await self._js.get_msg(
                    self._stream,
                    subject=subject,
                    seq=revision,
                    direct=self._direct,
                )
        except nats.js.errors.NotFoundError:
            raise nats.js.errors.KeyNotFoundError

        # Check whether the revision from the stream does not match the key.
        if subject != msg.subject:
            raise nats.js.errors.KeyNotFoundError(message=f"expected '{subject}', but got '{msg.subject}'")

        entry = KeyValue.Entry(
            bucket=self._name,
            key=key,
            value=msg.data,
            revision=msg.seq,
            delta=0,
            created=msg.time,
            operation=None,
        )

        # Check headers to see if deleted or purged.
        if msg.headers:
            op = msg.headers.get(KV_OP, None)
            if op == KV_DEL or op == KV_PURGE:
                entry.operation = op
                raise nats.js.errors.KeyDeletedError(entry, op)

        return entry

    async def put(self, key: str, value: bytes, validate_keys: bool = True) -> int:
        """
        put will place the new value for the key into the store
        and return the revision number.

        Note: This method does not support TTL. Use create() if you need TTL support.

        :param key: The key to put
        :param value: The value to store
        :param validate_keys: Whether to validate the key format
        """
        if validate_keys and not _is_key_valid(key):
            raise nats.js.errors.InvalidKeyError(key)

        pa = await self._js.publish(f"{self._mutation_pre}{key}", value)
        return pa.seq

    async def put_string(self, key: str, value: str, validate_keys: bool = True) -> int:
        """
        put_string places a string value for the key, encoded as UTF-8,
        and returns the revision number.
        """
        return await self.put(key, value.encode(), validate_keys=validate_keys)

    async def create(self, key: str, value: bytes, validate_keys: bool = True, msg_ttl: Optional[float] = None) -> int:
        """
        create will add the key/value pair iff it does not exist.

        :param key: The key to create
        :param value: The value to store
        :param validate_keys: Whether to validate the key format
        :param msg_ttl: Optional TTL (time-to-live) in seconds for this specific message
        """
        if validate_keys and not _is_key_valid(key):
            raise nats.js.errors.InvalidKeyError(key)

        pa = None
        try:
            pa = await self._update(key, value, last=0, validate_keys=validate_keys, msg_ttl=msg_ttl)
        except nats.js.errors.KeyWrongLastSequenceError as err:
            # In case of attempting to recreate an already deleted key,
            # the client would get a KeyWrongLastSequenceError.  When this happens,
            # it is needed to fetch latest revision number and attempt to update.
            try:
                # NOTE: This reimplements the following behavior from Go client.
                #
                #   Since we have tombstones for DEL ops for watchers, this could be from that
                #   so we need to double check.
                #

                # Get latest revision to update in case it was deleted but if it was not
                await self._get(key)

                # No exception so not a deleted key, so reraise the original KeyWrongLastSequenceError.
                # If it was deleted then the error exception will contain metadata
                # to recreate using the last revision.
                raise err
            except nats.js.errors.KeyDeletedError as err:
                pa = await self._update(
                    key, value, last=err.entry.revision, validate_keys=validate_keys, msg_ttl=msg_ttl
                )

        return pa

    async def update(
        self,
        key: str,
        value: bytes,
        last: Optional[int] = None,
        validate_keys: bool = True,
    ) -> int:
        """
        update will update the value if the latest revision matches.

        Raises KeyRevisionMismatchError when ``last`` is not the latest
        revision of the key.
        """
        try:
            return await self._update(key, value, last=last, validate_keys=validate_keys)
        except nats.js.errors.KeyRevisionMismatchError:
            raise
        except nats.js.errors.KeyWrongLastSequenceError as err:
            raise nats.js.errors.KeyRevisionMismatchError(description=err.description) from err

    async def _update(
        self,
        key: str,
        value: bytes,
        last: Optional[int] = None,
        validate_keys: bool = True,
        msg_ttl: Optional[float] = None,
    ) -> int:
        if validate_keys and not _is_key_valid(key):
            raise nats.js.errors.InvalidKeyError(key)

        hdrs = {}
        if not last:
            last = 0
        hdrs[api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(last)

        pa = None
        try:
            pa = await self._js.publish(f"{self._mutation_pre}{key}", value, headers=hdrs, msg_ttl=msg_ttl)
        except nats.js.errors.APIError as err:
            # Check for a BadRequest::KeyWrongLastSequenceError error code.
            # 10071: JSStreamWrongLastSequenceErrF
            # 10164: JSStreamWrongLastSequenceConstantErr
            if err.err_code in (10071, 10164):
                raise nats.js.errors.KeyWrongLastSequenceError(description=err.description)
            else:
                raise err
        return pa.seq

    async def delete(
        self,
        key: str,
        last: Optional[int] = None,
        validate_keys: bool = True,
        msg_ttl: Optional[float] = None,
    ) -> bool:
        """
        delete will place a delete marker and remove all previous revisions.

        :param key: The key to delete
        :param last: Expected last revision number (for optimistic concurrency)
        :param validate_keys: Whether to validate the key format
        :param msg_ttl: Deprecated and ignored. TTL on a delete marker has no
            meaningful semantics in NATS KV; use ``create()`` or ``purge()``.
        """
        if msg_ttl is not None:
            import warnings

            warnings.warn(
                "msg_ttl on delete() is deprecated and ignored; use create() or purge() for TTL",
                DeprecationWarning,
                stacklevel=2,
            )
        if validate_keys and not _is_key_valid(key):
            raise nats.js.errors.InvalidKeyError(key)

        hdrs = {}
        hdrs[KV_OP] = KV_DEL

        if last and last > 0:
            hdrs[api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(last)

        await self._publish_marker(key, hdrs)
        return True

    async def purge(
        self,
        key: str,
        msg_ttl: Optional[float] = None,
        last: Optional[int] = None,
    ) -> bool:
        """
        purge will remove the key and all revisions.

        :param key: The key to purge
        :param msg_ttl: Optional TTL (time-to-live) in seconds for the purge marker
        :param last: Expected last revision number (for optimistic concurrency);
            raises KeyRevisionMismatchError when it is not the latest one
        """
        hdrs = {}
        hdrs[KV_OP] = KV_PURGE
        hdrs[api.Header.ROLLUP] = MSG_ROLLUP_SUBJECT
        if last and last > 0:
            hdrs[api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(last)
        await self._publish_marker(key, hdrs, msg_ttl=msg_ttl)
        return True

    async def _publish_marker(self, key: str, hdrs: Dict[str, str], msg_ttl: Optional[float] = None) -> None:
        try:
            await self._js.publish(f"{self._mutation_pre}{key}", headers=hdrs, msg_ttl=msg_ttl)
        except nats.js.errors.APIError as err:
            # A wrong last sequence on a delete or purge is a revision
            # mismatch (nats.go wraps ErrKeyRevisionMismatch).
            if err.err_code in (10071, 10164):
                raise nats.js.errors.KeyRevisionMismatchError(
                    description=err.description,
                    code=err.code,
                    err_code=err.err_code,
                    stream=err.stream,
                    seq=err.seq,
                ) from err
            raise

    async def purge_deletes(self, olderthan: int = 30 * 60) -> bool:
        """
        purge will remove all current delete markers older.
        :param olderthan: time in seconds
        """

        watcher = await self.watchall()
        delete_markers = []
        async for update in watcher:
            if update is None:
                break

            if update.operation == KV_DEL or update.operation == KV_PURGE:
                delete_markers.append(update)

        for entry in delete_markers:
            keep = 0
            subject = f"{self._pre}{entry.key}"
            duration = datetime.datetime.now(datetime.timezone.utc) - entry.created
            if olderthan > 0 and olderthan > duration.total_seconds():
                keep = 1
            await self._js.purge_stream(self._stream, subject=subject, keep=keep)
        return True

    async def status(self) -> BucketStatus:
        """
        status retrieves the status and configuration of a bucket.
        """
        info = await self._js.stream_info(self._stream)
        return KeyValue.BucketStatus(stream_info=info, bucket=self._name)

    class KeyWatcher:
        STOP_ITER = StopIterSentinel()

        def __init__(self, js):
            self._js = js
            self._updates: asyncio.Queue[KeyValue.Entry | None | StopIterSentinel] = asyncio.Queue(maxsize=256)
            self._sub = None
            self._pending: Optional[int] = None

            # init done means that the nil marker has been sent,
            # once this is sent it won't be sent anymore.
            self._init_done = False

        async def stop(self):
            """
            stop will stop this watcher.
            """
            await self._sub.unsubscribe()
            while True:
                try:
                    self._updates.put_nowait(KeyValue.KeyWatcher.STOP_ITER)
                    return
                except asyncio.QueueFull:
                    try:
                        self._updates.get_nowait()
                    except asyncio.QueueEmpty:
                        pass

        async def updates(self, timeout=5.0):
            """
            updates fetches the next update from a watcher.
            """
            try:
                return await asyncio.wait_for(self._updates.get(), timeout)
            except asyncio.TimeoutError:
                raise nats.errors.TimeoutError

        def __aiter__(self):
            return self

        async def __anext__(self):
            while True:
                entry = await self._updates.get()
                if isinstance(entry, StopIterSentinel):
                    raise StopAsyncIteration
                return entry

    class KeyLister:
        """
        KeyLister delivers the keys of a bucket as an async iterator
        (nats.go KeyLister); iterating ends once the keys present when the
        listing started have been delivered. stop() ends it early.

        ::

            lister = await kv.list_keys()
            async for key in lister:
                print(key)
        """

        def __init__(self, watcher: KeyValue.KeyWatcher) -> None:
            self._watcher = watcher
            self._done = False

        def keys(self) -> KeyValue.KeyLister:
            """
            keys returns the async iterator of the keys.
            """
            return self

        async def stop(self) -> None:
            """
            stop stops the listing.
            """
            if self._done:
                return
            self._done = True
            await self._watcher.stop()

        def __aiter__(self) -> KeyValue.KeyLister:
            return self

        async def __anext__(self) -> str:
            if self._done:
                raise StopAsyncIteration
            entry = await self._watcher._updates.get()
            if entry is None or isinstance(entry, StopIterSentinel):
                await self.stop()
                raise StopAsyncIteration
            return entry.key

    async def list_keys(self, **kwargs) -> KeyLister:
        """
        list_keys returns a KeyLister of the keys of the bucket that have
        a value (nats.go ListKeys). The keyword arguments are watch()
        options. Unlike keys(), an empty bucket lists no keys instead of
        raising NoKeysError.
        """
        kwargs.update(ignore_deletes=True, meta_only=True)
        watcher = await self.watchall(**kwargs)
        return KeyValue.KeyLister(watcher)

    async def list_keys_filtered(self, filters: List[str]) -> KeyLister:
        """
        list_keys_filtered returns a KeyLister of the keys that have a
        value and match any of the subject patterns in filters, e.g.
        ``["orders.*", "users.>"]`` (nats.go ListKeysFiltered). Unlike the
        substring filters of keys(), these are NATS subject wildcards.
        """
        watcher = await self.watch_filtered(filters, ignore_deletes=True, meta_only=True)
        return KeyValue.KeyLister(watcher)

    async def watchall(self, **kwargs) -> KeyWatcher:
        """
        watchall returns a KeyValue watcher that matches all the keys.
        """
        return await self.watch(ALL_KEYS, **kwargs)

    async def keys(self, filters: List[str] = None, **kwargs) -> List[str]:
        """
        Returns a list of the keys from a KeyValue store.
        Optionally filters the keys based on the provided filter list.
        """
        watcher = await self.watchall(
            ignore_deletes=True,
            meta_only=True,
        )
        keys = []

        # Check consumer info and make sure filters are applied correctly
        try:
            consumer_info = await watcher._sub.consumer_info()
            if consumer_info and filters:
                # If NATS server < 2.10, filters might be ignored.
                if consumer_info.config.filter_subject != ">":
                    logger.warning("Server may ignore filters if version is < 2.10.")
        except Exception as e:
            raise e

        async for key in watcher:
            # None entry is used to signal that there is no more info.
            if not key:
                break

            # Apply filters if any were provided
            if filters:
                if any(f in key.key for f in filters):
                    keys.append(key.key)
            else:
                # No filters provided, append all keys
                keys.append(key.key)

        await watcher.stop()

        if not keys:
            raise nats.js.errors.NoKeysError

        return keys

    async def history(self, key: str) -> List[Entry]:
        """
        history retrieves a list of the entries so far.
        """
        watcher = await self.watch(key, include_history=True)

        entries = []

        async for entry in watcher:
            # None entry is used to signal that there is no more info.
            if not entry:
                break
            entries.append(entry)

        await watcher.stop()

        if not entries:
            raise nats.js.errors.NoKeysError

        return entries

    async def watch(
        self,
        keys,
        headers_only=False,
        include_history=False,
        ignore_deletes=False,
        meta_only=False,
        inactive_threshold=None,
        updates_only=False,
        resume_from_revision: Optional[int] = None,
    ) -> KeyWatcher:
        """
        watch will fire a callback when a key that matches the keys
        pattern is updated.
        The first update after starting the watch is None in case
        there are no pending updates.

        :param updates_only: Only deliver updates made after the watch
            starts; no initial values and no None marker are delivered.
        :param resume_from_revision: Deliver the updates starting at this
            revision of the bucket.
        """
        return await self._watch(
            [f"{self._pre}{keys}"],
            include_history=include_history,
            ignore_deletes=ignore_deletes,
            meta_only=meta_only,
            inactive_threshold=inactive_threshold,
            updates_only=updates_only,
            resume_from_revision=resume_from_revision,
        )

    async def watch_filtered(
        self,
        keys: List[str],
        include_history: bool = False,
        ignore_deletes: bool = False,
        meta_only: bool = False,
        inactive_threshold: Optional[float] = None,
        updates_only: bool = False,
        resume_from_revision: Optional[int] = None,
    ) -> KeyWatcher:
        """
        watch_filtered watches the keys matching any of the given subject
        patterns (e.g. ``["orders.*", "users.>"]``); an empty list watches
        all the keys. The options are those of watch().
        """
        for key in keys:
            if not _is_search_key_valid(key):
                raise nats.js.errors.InvalidKeyError(key)
        if not keys:
            keys = [ALL_KEYS]
        return await self._watch(
            [f"{self._pre}{key}" for key in keys],
            include_history=include_history,
            ignore_deletes=ignore_deletes,
            meta_only=meta_only,
            inactive_threshold=inactive_threshold,
            updates_only=updates_only,
            resume_from_revision=resume_from_revision,
        )

    async def _watch(
        self,
        subjects: List[str],
        include_history=False,
        ignore_deletes=False,
        meta_only=False,
        inactive_threshold=None,
        updates_only=False,
        resume_from_revision: Optional[int] = None,
    ) -> KeyWatcher:
        watcher = KeyValue.KeyWatcher(self)
        # With updates only there are no initial values to signal the end of
        # (nats.go marks the initialization as done).
        if updates_only:
            watcher._init_done = True
        init_setup: asyncio.Future[bool] = asyncio.Future()

        async def watch_updates(msg):
            if not init_setup.done():
                await asyncio.wait_for(init_setup, timeout=self._js._timeout)

            meta = msg.metadata
            op = None
            if msg.header and KV_OP in msg.header:
                op = msg.header.get(KV_OP)
            elif msg.header and KV_MARKER_REASON in msg.header:
                # nats-server 2.11+: server-placed TTL/age expiry markers use
                # Nats-Marker-Reason instead of KV-Operation.
                reason = msg.header.get(KV_MARKER_REASON)
                if reason in ("MaxAge", "Purge"):
                    op = KV_PURGE
                elif reason == "Remove":
                    op = KV_DEL
                else:
                    # Unknown future reason — skip silently rather than emitting
                    # an entry with operation=None that callers can't distinguish
                    # from a regular value update.
                    if meta.num_pending == 0 and not watcher._init_done:
                        await watcher._updates.put(None)
                        watcher._init_done = True
                    return

            # keys() uses this
            if ignore_deletes and op in (KV_PURGE, KV_DEL):
                if meta.num_pending == 0 and not watcher._init_done:
                    await watcher._updates.put(None)
                    watcher._init_done = True
                return

            entry = KeyValue.Entry(
                bucket=self._name,
                key=msg.subject[len(self._pre) :],
                value=msg.data,
                revision=meta.sequence.stream,
                delta=meta.num_pending,
                created=meta.timestamp,
                operation=op,
            )
            await watcher._updates.put(entry)

            # When there are no more updates send an empty marker
            # to signal that it is done, this will unblock iterators
            if meta.num_pending == 0 and (not watcher._init_done):
                await watcher._updates.put(None)
                watcher._init_done = True

        # As nats.go, the last of these deliver policies applies.
        config = None
        deliver_policy = None
        if not include_history:
            deliver_policy = api.DeliverPolicy.LAST_PER_SUBJECT
        if updates_only:
            deliver_policy = api.DeliverPolicy.NEW
        if resume_from_revision is not None and resume_from_revision > 0:
            deliver_policy = api.DeliverPolicy.BY_START_SEQUENCE
            config = api.ConsumerConfig(opt_start_seq=resume_from_revision)
        if len(subjects) > 1:
            if config is None:
                config = api.ConsumerConfig()
            config.filter_subjects = subjects

        # Cleanup watchers after 5 minutes of inactivity by default.
        if not inactive_threshold:
            inactive_threshold = 5 * 60

        watcher._sub = await self._js.subscribe(
            subjects[0],
            stream=self._stream,
            cb=watch_updates,
            config=config,
            ordered_consumer=True,
            deliver_policy=deliver_policy,
            headers_only=meta_only,
            inactive_threshold=inactive_threshold,
        )
        await asyncio.sleep(0)

        # Check from consumer info what is the number of messages
        # awaiting to be consumed to send the initial signal marker.
        try:
            cinfo = await watcher._sub.consumer_info()
            watcher._pending = cinfo.num_pending

            # If no delivered and/or pending messages, then signal
            # that this is the start.
            # The consumer subscription will start receiving messages
            # so need to check those that have already made it.
            received = watcher._sub.delivered
            init_setup.set_result(True)
            if cinfo.num_pending == 0 and received == 0 and not watcher._init_done:
                await watcher._updates.put(None)
                watcher._init_done = True
        except Exception as err:
            init_setup.cancel()
            await watcher._sub.unsubscribe()
            raise err

        return watcher
