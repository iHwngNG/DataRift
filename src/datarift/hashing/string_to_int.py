"""Deterministic string-to-integer conversion using MD5."""

import hashlib


def to_int(value: str) -> int:
    """Convert a string to a deterministic integer using MD5.

    Computes the MD5 hash of the UTF-8 encoded bytes and extracts the first
    8 bytes as a big-endian unsigned integer. This provides deterministic,
    consistent hashing that is not affected by Python's randomized hash seed
    (PYTHONHASHSEED).

    Args:
        value: The string value to hash.

    Returns:
        A non-negative integer derived from the first 8 bytes of the MD5 hash.

    """
    hash_bytes = hashlib.md5(value.encode("utf-8")).digest()
    return int.from_bytes(hash_bytes[:8], byteorder="big")
