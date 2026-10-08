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
    InvalidDigestFormatError,
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
