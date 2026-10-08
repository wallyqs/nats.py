import base64
import binascii
import io
import json
import os
import tempfile
import unittest
from hashlib import sha256

import nats
import nats.js.api
import pytest
from nats.js.errors import (
    BucketMalformedError,
    BucketNotFoundError,
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
    ObjectConfigRequiredError,
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


class ObjectStoreConfigTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_compression_and_metadata(self):
        nc = await nats.connect()
        js = nc.jetstream()

        obs = await js.create_object_store(
            "COMPRESSED",
            config=nats.js.api.ObjectStoreConfig(compression=True, metadata={"team": "a"}),
        )
        status = await obs.status()
        assert status.is_compressed
        assert status.backing_store == "JetStream"
        assert status.metadata["team"] == "a"
        assert status.stream_info.config.compression == nats.js.api.StoreCompression.S2

        # Objects still round trip.
        await obs.put("A", b"A" * 10000)
        assert (await obs.get("A")).data == b"A" * 10000

        obs = await js.create_object_store("PLAIN")
        status = await obs.status()
        assert not status.is_compressed
        assert status.backing_store == "JetStream"
        assert "team" not in (status.metadata or {})

        await nc.close()


class ObjectStoreManagerTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_update_object_store(self):
        nc = await nats.connect()
        js = nc.jetstream()

        with pytest.raises(BucketNotFoundError):
            await js.update_object_store("MISSING", description="x")

        await js.create_object_store("UPD", description="before")
        obs = await js.update_object_store(
            config=nats.js.api.ObjectStoreConfig(bucket="UPD", description="after", metadata={"v": "2"})
        )
        assert obs._name == "UPD"
        assert obs._stream == "OBJ_UPD"
        status = await obs.status()
        assert status.description == "after"
        assert status.metadata["v"] == "2"

        with pytest.raises(InvalidStoreNameError):
            await js.update_object_store("bad.name")

        await nc.close()

    @async_test
    async def test_create_or_update_object_store(self):
        nc = await nats.connect()
        js = nc.jetstream()

        obs = await js.create_or_update_object_store("COU", description="one")
        assert (await obs.status()).description == "one"
        await obs.put("A", b"A")

        obs = await js.create_or_update_object_store("COU", description="two")
        assert (await obs.status()).description == "two"
        # The bucket was updated, not replaced.
        assert (await obs.get("A")).data == b"A"

        await nc.close()

    @async_test
    async def test_config_required(self):
        nc = await nats.connect()
        js = nc.jetstream()

        for call in (js.create_object_store, js.update_object_store, js.create_or_update_object_store):
            with pytest.raises(ObjectConfigRequiredError) as e:
                await call()
            assert str(e.value) == "nats: object-store config required"

        # The bucket may be given in the config alone.
        obs = await js.create_object_store(config=nats.js.api.ObjectStoreConfig(bucket="CFGONLY"))
        assert obs._name == "CFGONLY"
        assert (await js.object_store("CFGONLY"))._stream == "OBJ_CFGONLY"

        await nc.close()


class ObjectStoreListingTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_object_store_names_and_stores(self):
        nc = await nats.connect()
        js = nc.jetstream()

        assert [name async for name in js.object_store_names()] == []
        assert [status async for status in js.object_stores()] == []

        await js.create_object_store("ONE", description="first")
        two = await js.create_object_store("TWO")
        await two.put("A", b"AAA")
        # Neither a key value bucket nor a plain stream is listed.
        await js.create_key_value(bucket="KV")
        await js.add_stream(name="PLAIN", subjects=["plain"])

        names = [name async for name in js.object_store_names()]
        assert sorted(names) == ["ONE", "TWO"]

        statuses = {status.bucket: status async for status in js.object_stores()}
        assert sorted(statuses) == ["ONE", "TWO"]
        assert statuses["ONE"].description == "first"
        assert statuses["ONE"].backing_store == "JetStream"
        assert statuses["TWO"].size > 0
        assert statuses["TWO"].stream_info.config.name == "OBJ_TWO"

        await nc.close()

    @async_test
    async def test_object_stores_paging(self):
        nc = await nats.connect()
        js = nc.jetstream()

        # More than one page of STREAM.LIST (256 streams per page).
        expected = sorted(f"B{i:03d}" for i in range(260))
        for bucket in expected:
            await js.create_object_store(bucket, storage=nats.js.api.StorageType.MEMORY)

        assert sorted([name async for name in js.object_store_names()]) == expected
        assert sorted([status.bucket async for status in js.object_stores()]) == expected

        await nc.close()


class ObjectReaderTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_get_reader(self):
        nc = await nats.connect()
        js = nc.jetstream()
        obs = await js.create_object_store("READER")

        data = bytes(range(256)) * 40
        meta = nats.js.api.ObjectMeta(name="A", options=nats.js.api.ObjectMetaOptions(max_chunk_size=1000))
        await obs.put("A", data, meta=meta)

        # Chunk by chunk.
        async with await obs.get_reader("A") as reader:
            assert reader.info.name == "A"
            assert reader.info.chunks == 11
            chunks = [chunk async for chunk in reader]
            assert reader.error is None
        assert [len(c) for c in chunks] == [1000] * 10 + [240]
        assert b"".join(chunks) == data

        # Sized reads across chunks, then the rest.
        reader = await obs.get_reader("A")
        assert await reader.read(10) == data[:10]
        assert await reader.read(1500) == data[10:1000]
        assert await reader.read_chunk() == data[1000:2000]
        assert await reader.read() == data[2000:]
        assert await reader.read() == b""
        await reader.close()
        with pytest.raises(ValueError):
            await reader.read()

        # Closing before the end stops the download.
        reader = await obs.get_reader("A")
        assert await reader.read(1) == data[:1]
        await reader.close()
        assert reader._sub is None

        # Empty objects and links.
        await obs.put("E", b"")
        async with await obs.get_reader("E") as reader:
            assert await reader.read() == b""
        await obs.add_link("L", await obs.get_info("A"))
        async with await obs.get_reader("L") as reader:
            assert reader.info.name == "A"
            assert await reader.read() == data

        # A digest mismatch is raised by the last read and kept.
        stored = await obs.get_info("A")
        stored.digest = get_object_digest_value(sha256(b"other"))
        await _publish_meta(js, "READER", stored)
        reader = await obs.get_reader("A")
        with pytest.raises(DigestMismatchError):
            await reader.read()
        assert isinstance(reader.error, DigestMismatchError)
        with pytest.raises(DigestMismatchError):
            await reader.read()

        await nc.close()

    @async_test
    async def test_get_bytes_string_file(self):
        nc = await nats.connect()
        js = nc.jetstream()
        obs = await js.create_object_store("GETCONV")

        await obs.put("A", "héllo")
        assert await obs.get_bytes("A") == "héllo".encode()
        assert await obs.get_string("A") == "héllo"

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.write(b"a longer previous content")
        tmp.close()
        try:
            await obs.get_file("A", tmp.name)
            with open(tmp.name, "rb") as f:
                assert f.read() == "héllo".encode()
        finally:
            os.unlink(tmp.name)

        await obs.delete("A")
        with pytest.raises(ObjectNotFoundError):
            await obs.get_bytes("A")
        assert await obs.get_bytes("A", show_deleted=True) == b""

        await nc.close()
