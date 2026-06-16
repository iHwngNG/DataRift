"""Shard assignment utilities for DataRift."""


def assign_shard(int_value: int, shard_count: int) -> int:
    """Assign a shard index based on an integer value.

    Uses modulo arithmetic to deterministically map an integer to one of
    `shard_count` shards. The same `int_value` will always return the same
    shard, making this suitable for consistent routing.

    Args:
        int_value: A non-negative integer derived from a hash function.
        shard_count: The total number of shards (must be positive).

    Returns:
        A shard index in the range [0, shard_count - 1].

    Raises:
        ValueError: If `shard_count` is less than 1.

    """
    if shard_count < 1:
        raise ValueError("shard_count must be at least 1")
    return int_value % shard_count
