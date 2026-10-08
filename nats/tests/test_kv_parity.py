import nats
import nats.js.api
import pytest
from nats.js.errors import (
    APIError,
    BadRequestError,
    BucketMalformedError,
    BucketRequiredError,
    InvalidBucketNameError,
    KeyRevisionMismatchError,
    KeyValueConfigRequiredError,
    KeyWrongLastSequenceError,
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
