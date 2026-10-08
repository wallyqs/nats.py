import asyncio

import nats
import nats.errors
import nats.js.api
import nats.js.kv
import pytest
from nats.js.errors import (
    APIError,
    BadRequestError,
    BucketExistsError,
    BucketMalformedError,
    BucketNotFoundError,
    BucketRequiredError,
    InvalidBucketNameError,
    InvalidKeyError,
    KeyNotFoundError,
    KeyRevisionMismatchError,
    KeyValueConfigRequiredError,
    KeyWrongLastSequenceError,
    NotFoundError,
)

from tests.utils import (
    SingleJetStreamServerDomainTestCase,
    SingleJetStreamServerTestCase,
    async_test,
)


class KVErrorsTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_create_key_value_requires_config(self):
        nc = await nats.connect()
        js = nc.jetstream()

        with pytest.raises(KeyValueConfigRequiredError) as exc:
            await js.create_key_value()
        assert str(exc.value) == "nats: config required"

        # An empty bucket name is an invalid name, as in nats.go.
        with pytest.raises(InvalidBucketNameError):
            await js.create_key_value(nats.js.api.KeyValueConfig(bucket=""))

        await nc.close()

    @async_test
    async def test_update_revision_mismatch(self):
        nc = await nats.connect()
        js = nc.jetstream()
        kv = await js.create_key_value(bucket="MISMATCH")

        rev = await kv.put("a", b"1")
        await kv.put("a", b"2")

        with pytest.raises(KeyRevisionMismatchError) as exc:
            await kv.update("a", b"3", last=rev)
        err = exc.value
        # Still the error existing callers catch.
        assert isinstance(err, KeyWrongLastSequenceError)
        assert isinstance(err, BadRequestError)
        assert isinstance(err, APIError)
        assert str(err).startswith("nats: key revision mismatch")

        # A create over an existing key is not a revision mismatch
        # (nats.go reports ErrKeyExists for it).
        with pytest.raises(KeyWrongLastSequenceError) as exc:
            await kv.create("a", b"4")
        assert not isinstance(exc.value, KeyRevisionMismatchError)

        await nc.close()

    def test_bucket_error_identities(self):
        assert str(BucketRequiredError()) == "nats: bucket required"
        assert str(BucketMalformedError()) == "nats: bucket malformed"


class KVPurgeLastRevisionTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_purge_and_delete_with_last_revision(self):
        nc = await nats.connect()
        js = nc.jetstream()
        kv = await js.create_key_value(bucket="PURGELAST", history=5)

        first = await kv.put("a", b"1")
        latest = await kv.put("a", b"2")

        with pytest.raises(KeyRevisionMismatchError) as exc:
            await kv.purge("a", last=first)
        assert exc.value.err_code in (10071, 10164)
        assert isinstance(exc.value, BadRequestError)
        # Nothing was purged.
        assert (await kv.get("a")).value == b"2"

        assert await kv.purge("a", last=latest)
        with pytest.raises(KeyNotFoundError):
            await kv.get("a")
        history = await kv.history("a")
        assert len(history) == 1
        assert history[0].operation == "PURGE"

        latest = await kv.put("b", b"1")
        with pytest.raises(KeyRevisionMismatchError) as exc:
            await kv.delete("b", last=latest - 1)
        assert exc.value.err_code in (10071, 10164)
        assert await kv.delete("b", last=latest)

        await nc.close()


class KVManagerTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_create_key_value_bucket_exists(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.create_key_value(bucket="EXISTS", history=2)
        # Creating it again with the same config is fine.
        kv = await js.create_key_value(bucket="EXISTS", history=2)
        assert (await kv.status()).history == 2

        with pytest.raises(BucketExistsError) as exc:
            await js.create_key_value(bucket="EXISTS", history=5)
        err = exc.value
        assert isinstance(err, BadRequestError)
        assert err.err_code == 10058
        assert err.bucket == "EXISTS"
        assert str(err).startswith("nats: bucket name already in use: EXISTS")

        # A bucket stream that differs only in its discard policy is
        # updated instead (nats.go's upgrade of older buckets).
        si = await js.stream_info("KV_EXISTS")
        await js.update_stream(si.config.evolve(discard=nats.js.api.DiscardPolicy.OLD))
        assert (await js.stream_info("KV_EXISTS")).config.discard == nats.js.api.DiscardPolicy.OLD
        await js.create_key_value(bucket="EXISTS", history=2)
        assert (await js.stream_info("KV_EXISTS")).config.discard == nats.js.api.DiscardPolicy.NEW

        await nc.close()

    @async_test
    async def test_update_key_value(self):
        nc = await nats.connect()
        js = nc.jetstream()

        with pytest.raises(BucketNotFoundError) as exc:
            await js.update_key_value(bucket="UPDATED", history=5)
        assert isinstance(exc.value, NotFoundError)
        assert "UPDATED" in str(exc.value)

        with pytest.raises(KeyValueConfigRequiredError):
            await js.update_key_value()

        await js.create_key_value(bucket="UPDATED")
        kv = await js.update_key_value(nats.js.api.KeyValueConfig(bucket="UPDATED", history=5, description="up"))
        await kv.put("a", b"1")
        await kv.put("a", b"2")
        status = await kv.status()
        assert status.history == 5
        assert status.stream_info.config.description == "up"
        assert len(await kv.history("a")) == 2

        await nc.close()

    @async_test
    async def test_create_or_update_key_value(self):
        nc = await nats.connect()
        js = nc.jetstream()

        kv = await js.create_or_update_key_value(bucket="UPSERT", history=2)
        assert (await kv.status()).history == 2
        await kv.put("a", b"1")

        kv = await js.create_or_update_key_value(bucket="UPSERT", history=4)
        assert (await kv.status()).history == 4
        assert (await kv.get("a")).value == b"1"

        await nc.close()


async def _wait_for_messages(js, stream, count):
    for _ in range(100):
        si = await js.stream_info(stream)
        if si.state.messages >= count:
            return si
        await asyncio.sleep(0.05)
    raise AssertionError(f"{stream} did not reach {count} messages")


class KVConfigTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_compression_and_metadata(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.create_key_value(bucket="COMPRESSED", compression=True, metadata={"owner": "kv"})
        si = await js.stream_info("KV_COMPRESSED")
        assert si.config.compression == nats.js.api.StoreCompression.S2
        assert si.config.metadata["owner"] == "kv"

        await js.create_key_value(bucket="PLAIN")
        si = await js.stream_info("KV_PLAIN")
        assert si.config.compression in (None, nats.js.api.StoreCompression.NONE)

        await nc.close()

    @async_test
    async def test_mirror(self):
        nc = await nats.connect()
        js = nc.jetstream()

        origin = await js.create_key_value(bucket="ORIGIN", direct=True)
        await origin.put("a", b"1")

        # Direct gets are needed for the server to keep mirror_direct.
        mirror = await js.create_key_value(
            bucket="MIRROR",
            direct=True,
            mirror=nats.js.api.StreamSource(name="ORIGIN"),
        )
        si = await js.stream_info("KV_MIRROR")
        assert si.config.mirror.name == "KV_ORIGIN"
        assert si.config.mirror_direct is True
        assert not si.config.subjects
        await _wait_for_messages(js, "KV_MIRROR", 1)

        # Writes through the mirror go to the mirrored bucket.
        await mirror.put("b", b"2")
        assert (await origin.get("b")).value == b"2"
        await _wait_for_messages(js, "KV_MIRROR", 2)

        # Binding keeps the redirection.
        mirror = await js.key_value("MIRROR")
        await mirror.put("c", b"3")
        assert (await origin.get("c")).value == b"3"

        await nc.close()

    @async_test
    async def test_sources(self):
        nc = await nats.connect()
        js = nc.jetstream()

        one = await js.create_key_value(bucket="ONE")
        two = await js.create_key_value(bucket="TWO")
        await one.put("k1", b"1")
        await two.put("k2", b"2")

        sources = [
            nats.js.api.StreamSource(name="ONE"),
            nats.js.api.StreamSource(name="KV_TWO"),
        ]
        agg = await js.create_key_value(bucket="AGG", sources=sources)
        # The caller's sources are left untouched.
        assert sources[0].name == "ONE"
        assert sources[0].subject_transforms is None

        si = await js.stream_info("KV_AGG")
        assert si.config.subjects == ["$KV.AGG.>"]
        by_name = {s.name: s for s in si.config.sources}
        assert set(by_name) == {"KV_ONE", "KV_TWO"}
        assert by_name["KV_ONE"].subject_transforms[0].src == "$KV.ONE.>"
        assert by_name["KV_ONE"].subject_transforms[0].dest == "$KV.AGG.>"
        assert by_name["KV_TWO"].subject_transforms[0].src == "$KV.TWO.>"

        await _wait_for_messages(js, "KV_AGG", 2)
        assert (await agg.get("k1")).value == b"1"
        assert (await agg.get("k2")).value == b"2"

        await nc.close()


class KVMirrorDomainTest(SingleJetStreamServerDomainTestCase):
    @async_test
    async def test_mirror_of_bucket_in_other_domain(self):
        nc = await nats.connect()
        js = nc.jetstream()

        origin = await js.create_key_value(bucket="ORIGIN")
        await origin.put("a", b"1")

        api_prefix = "$JS.test-domain.API"
        mirror = await js.create_key_value(
            bucket="MIRROR",
            mirror=nats.js.api.StreamSource(
                name="ORIGIN",
                external=nats.js.api.ExternalStream(api=api_prefix),
            ),
        )
        # Reads use the mirrored bucket's subjects, writes go through the
        # other domain's API prefix.
        assert mirror._pre == "$KV.ORIGIN."
        assert mirror._mutation_pre == f"{api_prefix}.$KV.ORIGIN."

        # The write reaches the mirrored bucket through the domain's API.
        # (Syncing a mirror needs a leafnode to the other domain, so reads
        # are only checked through the subjects above.)
        await mirror.put("b", b"2")
        assert (await origin.get("b")).value == b"2"

        await nc.close()


class KVStatusTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_status_fields(self):
        nc = await nats.connect()
        js = nc.jetstream()

        kv = await js.create_key_value(
            bucket="STATUS",
            description="status bucket",
            history=3,
            ttl=3600,
            max_bytes=1024 * 1024,
            max_value_size=1024,
            compression=True,
            metadata={"team": "kv"},
        )
        await kv.put("a", b"hello")

        status = await kv.status()
        assert status.backing_store == "JetStream"
        assert status.is_compressed is True
        assert status.metadata["team"] == "kv"
        assert status.bytes == status.stream_info.state.bytes
        assert status.bytes > 0

        config = status.config
        assert isinstance(config, nats.js.api.KeyValueConfig)
        assert config.bucket == "STATUS"
        assert config.description == "status bucket"
        assert config.history == 3
        assert config.ttl == 3600
        assert config.max_bytes == 1024 * 1024
        assert config.max_value_size == 1024
        assert config.replicas == 1
        assert config.compression is True
        assert config.metadata["team"] == "kv"
        assert config.mirror is None

        plain = await js.create_key_value(bucket="STATUS_PLAIN")
        status = await plain.status()
        assert status.is_compressed is False
        assert status.config.compression is False
        assert status.bytes == 0

        await nc.close()


class KVListTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_key_value_store_names_and_stores(self):
        nc = await nats.connect()
        js = nc.jetstream()

        assert [name async for name in js.key_value_store_names()] == []
        assert [status async for status in js.key_value_stores()] == []

        buckets = {f"LIST_{i}" for i in range(5)}
        for bucket in buckets:
            kv = await js.create_key_value(bucket=bucket)
            await kv.put("k", b"v")
        # Streams that are not buckets are left out.
        await js.add_stream(name="NOT_A_KV", subjects=["foo"])
        await js.add_stream(name="KV_LOOKALIKE", subjects=["bar"])

        names = [name async for name in js.key_value_store_names()]
        assert sorted(names) == sorted(buckets)

        statuses = [status async for status in js.key_value_stores()]
        assert sorted(s.bucket for s in statuses) == sorted(buckets)
        for status in statuses:
            assert isinstance(status, nats.js.kv.KeyValue.BucketStatus)
            assert status.values == 1
            assert status.backing_store == "JetStream"

        await nc.close()


async def _initial(watcher):
    """The entries a watcher delivers before its None marker."""
    entries = []
    while True:
        entry = await watcher.updates(timeout=2)
        if entry is None:
            return entries
        entries.append(entry)


class KVWatchOptionsTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_watch_updates_only(self):
        nc = await nats.connect()
        js = nc.jetstream()
        kv = await js.create_key_value(bucket="UPDATES")
        await kv.put("a", b"1")
        await kv.put("b", b"2")

        watcher = await kv.watchall(updates_only=True)
        # No initial values and no None marker.
        with pytest.raises(nats.errors.TimeoutError):
            await watcher.updates(timeout=0.3)

        await kv.put("c", b"3")
        entry = await watcher.updates(timeout=2)
        assert entry.key == "c"
        assert entry.value == b"3"
        await watcher.stop()

        # Also when the bucket is empty.
        empty = await js.create_key_value(bucket="UPDATES_EMPTY")
        watcher = await empty.watch("x", updates_only=True)
        with pytest.raises(nats.errors.TimeoutError):
            await watcher.updates(timeout=0.3)
        await empty.put("x", b"1")
        assert (await watcher.updates(timeout=2)).key == "x"
        await watcher.stop()

        await nc.close()

    @async_test
    async def test_watch_resume_from_revision(self):
        nc = await nats.connect()
        js = nc.jetstream()
        kv = await js.create_key_value(bucket="RESUME", history=5)
        await kv.put("a", b"1")
        await kv.put("b", b"2")
        await kv.put("a", b"3")
        await kv.put("c", b"4")

        watcher = await kv.watchall(resume_from_revision=2, include_history=True)
        entries = await _initial(watcher)
        assert [(e.key, e.revision) for e in entries] == [("b", 2), ("a", 3), ("c", 4)]
        await kv.put("d", b"5")
        assert (await watcher.updates(timeout=2)).revision == 5
        await watcher.stop()

        await nc.close()

    @async_test
    async def test_watch_filtered(self):
        nc = await nats.connect()
        js = nc.jetstream()
        kv = await js.create_key_value(bucket="FILTERED")
        await kv.put("orders.1", b"o1")
        await kv.put("users.1", b"u1")
        await kv.put("users.2.name", b"n")
        await kv.put("other", b"x")

        watcher = await kv.watch_filtered(["orders.*", "users.>"])
        entries = await _initial(watcher)
        assert sorted(e.key for e in entries) == ["orders.1", "users.1", "users.2.name"]

        await kv.put("other", b"y")
        await kv.put("orders.2", b"o2")
        entry = await watcher.updates(timeout=2)
        assert entry.key == "orders.2"
        await watcher.stop()

        # An empty list watches every key.
        watcher = await kv.watch_filtered([])
        entries = await _initial(watcher)
        assert sorted(e.key for e in entries) == ["orders.1", "orders.2", "other", "users.1", "users.2.name"]
        await watcher.stop()

        # A single pattern works like watch().
        watcher = await kv.watch_filtered(["users.*"], updates_only=True)
        await kv.put("users.3", b"u3")
        assert (await watcher.updates(timeout=2)).key == "users.3"
        await watcher.stop()

        with pytest.raises(InvalidKeyError):
            await kv.watch_filtered(["bad key"])
        with pytest.raises(InvalidKeyError):
            await kv.watch_filtered(["orders."])

        assert nats.js.kv.ALL_KEYS == ">"

        await nc.close()


class KVListKeysTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_list_keys(self):
        nc = await nats.connect()
        js = nc.jetstream()
        kv = await js.create_key_value(bucket="LISTKEYS", history=3)

        lister = await kv.list_keys()
        assert [key async for key in lister] == []

        await kv.put("orders.1", b"1")
        await kv.put("orders.2", b"2")
        await kv.put("orders.1", b"3")
        await kv.put("users.1", b"u")
        await kv.put("gone", b"x")
        await kv.delete("gone")
        await kv.put("purged", b"x")
        await kv.purge("purged")

        lister = await kv.list_keys()
        keys = [key async for key in lister.keys()]
        assert sorted(keys) == ["orders.1", "orders.2", "users.1"]
        # Iterating again after the end yields nothing.
        assert [key async for key in lister] == []

        lister = await kv.list_keys_filtered(["orders.*"])
        assert sorted([key async for key in lister]) == ["orders.1", "orders.2"]

        lister = await kv.list_keys_filtered(["users.>", "orders.2"])
        assert sorted([key async for key in lister]) == ["orders.2", "users.1"]

        # Stopping the lister ends the iteration.
        lister = await kv.list_keys()
        first = await lister.__anext__()
        assert first in ("orders.1", "orders.2", "users.1")
        await lister.stop()
        assert [key async for key in lister] == []
        await lister.stop()

        # keys() keeps its substring filters.
        assert sorted(await kv.keys(filters=["ders"])) == ["orders.1", "orders.2"]

        await nc.close()
