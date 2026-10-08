import base64
import binascii
import json
import unittest
from hashlib import sha256

import nats
import nats.js.api
import pytest
from nats.js.errors import (
    DigestMismatchError,
    InvalidBucketNameError,
    InvalidDigestFormatError,
    InvalidObjectNameError,
    InvalidStoreNameError,
    LinkNotAllowedError,
    NoObjectsFoundError,
    NotFoundError,
    ObjectDeletedError,
    ObjectNameRequiredError,
    ObjectNotFoundError,
    UpdateMetaDeletedError,
)
from nats.js.kv import MSG_ROLLUP_SUBJECT
from nats.js.object_store import (
    OBJ_META_PRE_TEMPLATE,
    decode_object_digest,
    get_object_digest_value,
)

from tests.utils import SingleJetStreamServerTestCase, async_test


async def _publish_meta(js, bucket, info):
    subject = OBJ_META_PRE_TEMPLATE.format(
        bucket=bucket,
        obj=base64.urlsafe_b64encode(info.name.encode()).decode(),
    )
    await js.publish(
        subject,
        json.dumps(info.as_dict()).encode(),
        headers={nats.js.api.Header.ROLLUP: MSG_ROLLUP_SUBJECT},
    )


class ObjectDigestHelpersTest(unittest.TestCase):
    def test_digest_helpers(self):
        h = sha256(b"A")
        digest = get_object_digest_value(h)
        assert digest == "SHA-256=VZrq0IJk1XldOQlxjN0Fq9SVcuhP5VWQ7vMaiKCP3_0="
        assert decode_object_digest(digest) == h.digest()

        # Like nats.go, only the text after the first "=" is the hash.
        assert decode_object_digest("sha-256=" + digest[len("SHA-256=") :]) == h.digest()

        with pytest.raises(InvalidDigestFormatError):
            decode_object_digest("no-separator")
        with pytest.raises(binascii.Error):
            decode_object_digest("SHA-256=not*base64")
        with pytest.raises(binascii.Error):
            # Missing padding.
            decode_object_digest("SHA-256=VZrq0IJk1XldOQlxjN0Fq9SVcuhP5VWQ7vMaiKCP3_0")


class ObjectDigestTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_digest_in_put_and_get(self):
        nc = await nats.connect()
        js = nc.jetstream()
        obs = await js.create_object_store("DIGEST")

        data = b"x" * 1000
        info = await obs.put("A", data)
        assert info.digest == get_object_digest_value(sha256(data))
        assert (await obs.get("A")).data == data

        # A stored digest with no separator is reported as such.
        stored = await obs.get_info("A")
        stored.digest = "bogus"
        await _publish_meta(js, "DIGEST", stored)
        with pytest.raises(InvalidDigestFormatError):
            await obs.get("A")

        # A well formed digest of other data is a mismatch.
        stored.digest = get_object_digest_value(sha256(b"other"))
        await _publish_meta(js, "DIGEST", stored)
        with pytest.raises(DigestMismatchError):
            await obs.get("A")

        await nc.close()


class ObjectStoreErrorsTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_invalid_store_name(self):
        nc = await nats.connect()
        js = nc.jetstream()

        for call in (
            lambda: js.create_object_store("bad.name"),
            lambda: js.object_store("bad name"),
            lambda: js.delete_object_store("bad*"),
            lambda: js.object_store(""),
        ):
            with pytest.raises(InvalidStoreNameError) as e:
                await call()
            # Still the error raised before.
            assert isinstance(e.value, InvalidBucketNameError)
            assert str(e.value) == "nats: invalid object-store name"

        await nc.close()

    @async_test
    async def test_name_required(self):
        nc = await nats.connect()
        js = nc.jetstream()
        obs = await js.create_object_store("NAMEREQ")

        for call in (
            lambda: obs.get_info(""),
            lambda: obs.get(""),
            lambda: obs.delete(""),
            lambda: obs.update_meta("", nats.js.api.ObjectMeta(name="X")),
        ):
            with pytest.raises(ObjectNameRequiredError) as e:
                await call()
            # Compatible with what an empty name lookup raised before.
            assert isinstance(e.value, ObjectNotFoundError)
            assert isinstance(e.value, InvalidObjectNameError)
            assert str(e.value) == "nats: name is required"

        await nc.close()

    @async_test
    async def test_no_objects_found(self):
        nc = await nats.connect()
        js = nc.jetstream()
        obs = await js.create_object_store("NOOBJS")

        with pytest.raises(NoObjectsFoundError) as e:
            await obs.list()
        assert isinstance(e.value, NotFoundError)
        assert str(e.value) == "nats: no objects found"

        await obs.put("A", b"A")
        await obs.delete("A")
        with pytest.raises(NoObjectsFoundError):
            await obs.list()
        assert [i.name for i in await obs.list(ignore_deletes=False)] == ["A"]

        await nc.close()

    @async_test
    async def test_update_meta_deleted(self):
        nc = await nats.connect()
        js = nc.jetstream()
        obs = await js.create_object_store("UPDDEL")

        await obs.put("A", b"A")
        await obs.delete("A")
        for name in ("A", "missing"):
            with pytest.raises(UpdateMetaDeletedError) as e:
                await obs.update_meta(name, nats.js.api.ObjectMeta(name=name))
            assert isinstance(e.value, ObjectDeletedError)
            assert str(e.value) == "nats: cannot update meta for a deleted object"

        await nc.close()

    @async_test
    async def test_put_link_not_allowed(self):
        nc = await nats.connect()
        js = nc.jetstream()
        obs = await js.create_object_store("PUTLINK")

        meta = nats.js.api.ObjectMeta(
            name="A",
            options=nats.js.api.ObjectMetaOptions(link=nats.js.api.ObjectLink(bucket="PUTLINK", name="B")),
        )
        with pytest.raises(LinkNotAllowedError) as e:
            await obs.put("A", b"A", meta=meta)
        assert str(e.value) == "nats: link cannot be set when putting the object in bucket"

        # Nothing was stored.
        status = await obs.status()
        assert status.stream_info.state.messages == 0

        await nc.close()
