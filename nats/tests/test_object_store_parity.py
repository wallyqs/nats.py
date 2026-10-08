import base64
import binascii
import io
import json
import unittest
from hashlib import sha256

import nats
import nats.js.api
import pytest
from nats.js.errors import (
    BucketMalformedError,
    BucketRequiredError,
    DigestMismatchError,
    InvalidBucketNameError,
    InvalidDigestFormatError,
    InvalidObjectNameError,
    InvalidStoreNameError,
    LinkIsABucketError,
    LinkNotAllowedError,
    NoLinkToDeletedError,
    NoLinkToLinkError,
    NoObjectsFoundError,
    NotFoundError,
    ObjectAlreadyExists,
    ObjectDeletedError,
    ObjectNameRequiredError,
    ObjectNotFoundError,
    ObjectRequiredError,
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


class ObjectMetadataTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_object_metadata(self):
        nc = await nats.connect()
        js = nc.jetstream()
        obs = await js.create_object_store("OBJMETA")

        meta = nats.js.api.ObjectMeta(name="A", description="d", metadata={"k": "v"})
        info = await obs.put("A", b"A", meta=meta)
        assert info.metadata == {"k": "v"}

        info = await obs.get_info("A")
        assert info.metadata == {"k": "v"}
        assert info.meta.metadata == {"k": "v"}
        assert (await obs.get("A")).info.metadata == {"k": "v"}

        # The stored meta uses nats.go's JSON field.
        raw = await js.get_last_msg("OBJ_OBJMETA", "$O.OBJMETA.M.QQ==")
        assert json.loads(raw.data)["metadata"] == {"k": "v"}

        # update_meta replaces the metadata too.
        meta = info.meta
        meta.metadata = {"x": "y"}
        await obs.update_meta("A", meta)
        assert (await obs.get_info("A")).metadata == {"x": "y"}

        # Without metadata the field is left out.
        await obs.put("B", b"B")
        raw = await js.get_last_msg("OBJ_OBJMETA", "$O.OBJMETA.M.Qg==")
        assert "metadata" not in json.loads(raw.data)
        assert (await obs.get_info("B")).metadata is None

        await nc.close()


class ObjectLinkTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_add_link(self):
        nc = await nats.connect()
        js = nc.jetstream()
        obs = await js.create_object_store("LINKS")
        other = await js.create_object_store("OTHER")

        a = await obs.put("A", b"AAA")
        b = await other.put("B", b"BBB")

        # Link in the same bucket.
        link = await obs.add_link("toA", a)
        assert link.name == "toA"
        assert link.bucket == "LINKS"
        assert link.is_link()
        assert link.options.link.bucket == "LINKS"
        assert link.options.link.name == "A"
        assert link.mtime is not None

        info = await obs.get_info("toA")
        assert info.is_link()
        assert info.size == 0
        res = await obs.get("toA")
        assert res.data == b"AAA"
        assert res.info.name == "A"

        # Link to another bucket, followed when writing into a file too.
        await obs.add_link("toB", b)
        assert (await obs.get("toB")).data == b"BBB"
        buf = io.BytesIO()
        await obs.get("toB", writeinto=buf)
        assert buf.getvalue() == b"BBB"

        # A link may replace a link.
        link = await obs.add_link("toA", b)
        assert (await obs.get("toA")).data == b"BBB"

        # But not an object, even a deleted one.
        with pytest.raises(ObjectAlreadyExists):
            await obs.add_link("A", b)
        await obs.put("D", b"D")
        await obs.delete("D")
        with pytest.raises(ObjectAlreadyExists):
            await obs.add_link("D", b)

        # The checks on the arguments.
        with pytest.raises(ObjectNameRequiredError):
            await obs.add_link("", a)
        with pytest.raises(ObjectRequiredError) as e:
            await obs.add_link("x", None)
        assert str(e.value) == "nats: object required"
        with pytest.raises(ObjectRequiredError):
            await obs.add_link("x", nats.js.api.ObjectInfo(name="", bucket="LINKS", nuid="n"))
        deleted = await obs.get_info("D", show_deleted=True)
        with pytest.raises(NoLinkToDeletedError) as e:
            await obs.add_link("x", deleted)
        assert str(e.value) == "nats: not allowed to link to a deleted object"
        with pytest.raises(NoLinkToLinkError) as e:
            await obs.add_link("x", await obs.get_info("toB"))
        assert str(e.value) == "nats: not allowed to link to another link"

        await nc.close()

    @async_test
    async def test_add_bucket_link(self):
        nc = await nats.connect()
        js = nc.jetstream()
        obs = await js.create_object_store("BLINKS")
        other = await js.create_object_store("BOTHER")

        link = await obs.add_bucket_link("dir", other)
        assert link.name == "dir"
        assert link.bucket == "BLINKS"
        assert link.is_link()
        assert link.options.link.bucket == "BOTHER"
        assert link.options.link.name is None

        info = await obs.get_info("dir")
        assert info.options.link.bucket == "BOTHER"
        with pytest.raises(LinkIsABucketError):
            await obs.get("dir")

        # A link may replace a link but not an object.
        await obs.add_bucket_link("dir", obs)
        assert (await obs.get_info("dir")).options.link.bucket == "BLINKS"
        await obs.put("A", b"A")
        with pytest.raises(ObjectAlreadyExists):
            await obs.add_bucket_link("A", other)

        with pytest.raises(ObjectNameRequiredError):
            await obs.add_bucket_link("", other)
        with pytest.raises(BucketRequiredError) as e:
            await obs.add_bucket_link("x", None)
        assert str(e.value) == "nats: bucket required"
        with pytest.raises(BucketMalformedError) as e:
            await obs.add_bucket_link("x", "BOTHER")
        assert str(e.value) == "nats: bucket malformed"

        await nc.close()
