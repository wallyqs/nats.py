import nats
import nats.js.api
import pytest
from nats.js.errors import (
    APIError,
    BadRequestError,
    BucketExistsError,
    BucketMalformedError,
    BucketNotFoundError,
    BucketRequiredError,
    InvalidBucketNameError,
    KeyNotFoundError,
    KeyRevisionMismatchError,
    KeyValueConfigRequiredError,
    KeyWrongLastSequenceError,
    NotFoundError,
)

from tests.utils import SingleJetStreamServerTestCase, async_test


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
