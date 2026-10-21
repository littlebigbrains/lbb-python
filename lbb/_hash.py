"""The stable hash of the data plane: BLAKE3 over parts, each part followed by
a zero byte, cut to 16 bytes (``lbb_core::stable_hash_hex``).

The data plane builds deterministic ids and keys with it (a review item's key,
a request id); a worker that files the same items needs the same hash. The
BLAKE3 here is the plain hash of the reference implementation, for inputs of
any length, in one pass: no keyed mode, no extended output. The TypeScript SDK
has the same code (``sdk/typescript/src/hash.ts``).
"""

from __future__ import annotations

from collections.abc import Sequence

_IV = (
    0x6A09E667,
    0xBB67AE85,
    0x3C6EF372,
    0xA54FF53A,
    0x510E527F,
    0x9B05688C,
    0x1F83D9AB,
    0x5BE0CD19,
)
_PERMUTATION = (2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8)
_CHUNK_START = 1
_CHUNK_END = 2
_PARENT = 4
_ROOT = 8
_BLOCK_LEN = 64
_CHUNK_LEN = 1024
_MASK = 0xFFFFFFFF


def _rotr(x: int, n: int) -> int:
    return ((x >> n) | (x << (32 - n))) & _MASK


def _g(s: list[int], a: int, b: int, c: int, d: int, x: int, y: int) -> None:
    s[a] = (s[a] + s[b] + x) & _MASK
    s[d] = _rotr(s[d] ^ s[a], 16)
    s[c] = (s[c] + s[d]) & _MASK
    s[b] = _rotr(s[b] ^ s[c], 12)
    s[a] = (s[a] + s[b] + y) & _MASK
    s[d] = _rotr(s[d] ^ s[a], 8)
    s[c] = (s[c] + s[d]) & _MASK
    s[b] = _rotr(s[b] ^ s[c], 7)


def _compress(
    cv: Sequence[int], block: Sequence[int], counter: int, block_len: int, flags: int
) -> list[int]:
    """The compression function: 16 words of output."""
    s = [*cv, *_IV[:4], counter & _MASK, (counter >> 32) & _MASK, block_len, flags]
    m = list(block)
    for round_ in range(7):
        _g(s, 0, 4, 8, 12, m[0], m[1])
        _g(s, 1, 5, 9, 13, m[2], m[3])
        _g(s, 2, 6, 10, 14, m[4], m[5])
        _g(s, 3, 7, 11, 15, m[6], m[7])
        _g(s, 0, 5, 10, 15, m[8], m[9])
        _g(s, 1, 6, 11, 12, m[10], m[11])
        _g(s, 2, 7, 8, 13, m[12], m[13])
        _g(s, 3, 4, 9, 14, m[14], m[15])
        if round_ < 6:
            m = [m[i] for i in _PERMUTATION]
    for i in range(8):
        s[i] ^= s[i + 8]
        s[i + 8] ^= cv[i]
    return s


def _words(data: bytes) -> list[int]:
    """A block of up to 64 bytes as 16 little-endian words, zero padded."""
    padded = data.ljust(_BLOCK_LEN, b"\0")
    return [int.from_bytes(padded[i : i + 4], "little") for i in range(0, _BLOCK_LEN, 4)]


# What a node puts out before the caller decides whether it is the root:
# (chaining value, block words, counter, block length, flags).
_Output = tuple[list[int], list[int], int, int, int]


def _chaining_value(output: _Output) -> list[int]:
    return _compress(*output)[:8]


def _chunk_output(chunk: bytes, counter: int) -> _Output:
    """The output of one chunk (at most 1,024 bytes) with its number."""
    cv = list(_IV)
    blocks = max(1, -(-len(chunk) // _BLOCK_LEN))
    for b in range(blocks - 1):
        block = _words(chunk[b * _BLOCK_LEN : (b + 1) * _BLOCK_LEN])
        cv = _compress(cv, block, counter, _BLOCK_LEN, _CHUNK_START if b == 0 else 0)[:8]
    last = chunk[(blocks - 1) * _BLOCK_LEN :]
    flags = (_CHUNK_START if blocks == 1 else 0) | _CHUNK_END
    return cv, _words(last), counter, len(last), flags


def _parent_output(left: list[int], right: list[int]) -> _Output:
    return list(_IV), [*left, *right], 0, _BLOCK_LEN, _PARENT


def blake3(data: bytes) -> bytes:
    """The 32-byte BLAKE3 hash of ``data``."""
    chunks = max(1, -(-len(data) // _CHUNK_LEN))
    stack: list[list[int]] = []
    for i in range(chunks - 1):
        cv = _chaining_value(_chunk_output(data[i * _CHUNK_LEN : (i + 1) * _CHUNK_LEN], i))
        # Merge the subtrees this chunk completes: one per trailing zero bit of the count.
        total = i + 1
        while total & 1 == 0:
            cv = _chaining_value(_parent_output(stack.pop(), cv))
            total >>= 1
        stack.append(cv)
    output = _chunk_output(data[(chunks - 1) * _CHUNK_LEN :], chunks - 1)
    while stack:
        output = _parent_output(stack.pop(), _chaining_value(output))
    cv, block, _counter, block_len, flags = output
    root = _compress(cv, block, 0, block_len, flags | _ROOT)
    return b"".join(word.to_bytes(4, "little") for word in root[:8])


def stable_hash_hex(parts: Sequence[str]) -> str:
    """The data plane's stable hash of ``parts`` as 32 hexadecimal characters
    (``lbb_core::stable_hash_hex``): BLAKE3 over the UTF-8 bytes of each part,
    each followed by a zero byte, cut to its first 16 bytes.

    The keys LBB's own workflows give their review items are made with it, so
    a worker that files the same items under the same keys uses it too::

        key = f"starter:{workflow}:value:{stable_hash_hex([entity_id, prop, value])[:24]}"
    """
    if isinstance(parts, str):
        raise TypeError("parts is a sequence of strings, not one string")
    return blake3(b"".join(part.encode() + b"\0" for part in parts))[:16].hex()


__all__ = ["blake3", "stable_hash_hex"]
