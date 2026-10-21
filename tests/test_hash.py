"""``stable_hash_hex`` is the data plane's stable hash, and ``blake3`` is the
reference BLAKE3: the vectors of the TypeScript SDK's ``hash.test.ts``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lbb import stable_hash_hex
from lbb._hash import blake3

# The official BLAKE3 test vectors (input byte i is i % 251), across the chunk
# and tree boundaries.
VECTORS = [
    (0, "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"),
    (1023, "10108970eeda3eb932baac1428c7a2163b0e924c9a9e25b35bba72b28f70bd11"),
    (1024, "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7"),
    (1025, "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444"),
    (2049, "5f4d72f40d7a5f82b15ca2b2e44b1de3c2ef86c426c95c1af0b6879522563030"),
    (3073, "7124b49501012f81cc7f11ca069ec9226cecb8a2c850cfe644e327d22d3e1cd3"),
    (4097, "9b4052b38f1c5fc8b1f9ff7ac7b27cd242487b3d890d15c96a1c25b8aa0fb995"),
    (8193, "bab6c09cb8ce8cf459261398d2e7aef35700bf488116ceb94a36d0f5f1b7bc3b"),
    (31744, "62b6960e1a44bcc1eb1a611a8d6235b6b4b78f32e7abc4fb4c6cdcce94895c47"),
]


@pytest.mark.parametrize(("length", "want"), VECTORS)
def test_blake3_gives_the_reference_hashes(length: int, want: str) -> None:
    assert blake3(bytes(i % 251 for i in range(length))).hex() == want


def test_blake3_of_text() -> None:
    assert (
        blake3(b"abc").hex()
        == "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
    )


def test_stable_hash_hex_ends_each_part_with_a_zero_byte() -> None:
    # `lbb_core::stable_hash_hex(["a", "b"])`, from the data plane.
    assert stable_hash_hex(["a", "b"]) == "55a01d7f389c7df5218c21c17594c483"
    assert stable_hash_hex([]) == blake3(b"").hex()[:32]
    assert stable_hash_hex(["ab"]) != stable_hash_hex(["a", "b"])
    # The UTF-8 bytes of a part: the data plane's hash of "Größe".
    assert stable_hash_hex(["Größe"]) == "c6ebb4d71d0a62acbab47e2292206757"
    with pytest.raises(TypeError):
        stable_hash_hex("ab")


def _rust_vectors() -> list[dict[str, object]]:
    for parent in Path(__file__).resolve().parents:
        fixture = parent / "templates" / "_evals" / "values-extract.rust.json"
        if fixture.is_file():
            vectors = json.loads(fixture.read_text(encoding="utf-8"))["stable_hash"]
            assert isinstance(vectors, list)
            return vectors
    return []


def test_stable_hash_hex_is_the_data_plane_s_on_its_recorded_vectors() -> None:
    # `lbb_core::stable_hash_hex` of these parts, written by lbb-server's
    # template_evals test (sdk/templates/_evals/values-extract.rust.json).
    vectors = _rust_vectors()
    if not vectors:
        pytest.skip("the template evals are not in this tree")
    assert len(vectors) >= 5
    for vector in vectors:
        parts = vector["parts"]
        assert isinstance(parts, list)
        assert stable_hash_hex([str(part) for part in parts]) == vector["hex"], parts
