import math
from typing import Any, Optional, Tuple

from .building_blocks import LoadBalancingTable, PowerOfTwoChoicesDerefTable

class AllocationFailed(Exception):
    """Custom exception for when the top-level allocation fails."""
    pass

class FixedSizeDerefTable:
    """
    An implementation of a dereference table for fixed-size tiny pointers,
    as described in Theorem 1 of the "Tiny Pointers" paper.

    It uses a primary LoadBalancingTable and a secondary PowerOfTwoChoicesDerefTable
    to handle overflows.
    """
    def __init__(self, n: int, delta: float):
        if not (0 < delta < 1):
            raise ValueError("delta must be between 0 and 1")
        if n < 10: # Ensure n is large enough for partitioning
            raise ValueError("n must be reasonably large")

        self.n = n
        self.delta = delta

        # Partition the total slots `n` between the primary and secondary tables.
        # Primary table gets the majority of the space.
        secondary_slots = math.ceil(n * delta / 2)
        primary_slots = n - secondary_slots

        # Primary table handles the bulk of allocations with a load factor of 1 - delta^2.
        # It will overflow approximately delta^2 of its items.
        self.primary = LoadBalancingTable(primary_slots, delta=delta**2)
        
        # Secondary table handles overflows from the primary table.
        self.secondary = PowerOfTwoChoicesDerefTable(secondary_slots)

        # The tiny pointer needs to encode which table it belongs to.
        # We use the MSB for this: 0 for primary, 1 for secondary.
        self.primary_p_bits = self.primary.p_bits
        self.secondary_p_bits = 1 + self.secondary.p_slot_bits
        
        # The max size of a tiny pointer is determined by the larger of the two tables' pointers.
        self.max_p_bits = max(self.primary_p_bits, self.secondary_p_bits) + 1

    def allocate(self, k: Any, value: Any) -> int:
        """
        Allocates a value for key k and returns a tiny pointer.
        First attempts allocation in the primary table. If that fails (overflow),
        it attempts allocation in the secondary table.
        """
        # Try primary table first
        p_primary = self.primary.allocate(k, value)
        if p_primary is not None:
            # Success in primary table. The pointer is `p` itself (MSB is 0).
            return p_primary

        # Primary table failed, try secondary table
        p_secondary = self.secondary.allocate(k, value)
        if p_secondary is not None:
            # Success in secondary table. Set MSB to 1 to indicate it's a secondary pointer.
            return (1 << self.max_p_bits -1) | p_secondary

        # Both tables failed, which should be rare w.h.p.
        raise AllocationFailed(f"Allocation failed for key {k} in both primary and secondary tables.")

    def _decode_pointer(self, p: int) -> Tuple[bool, int]:
        """
        Decodes a pointer to determine if it's for the secondary table
        and its actual value.
        Returns: (is_secondary, pointer_value)
        """
        is_secondary = (p >> (self.max_p_bits - 1)) == 1
        if is_secondary:
            mask = (1 << (self.max_p_bits - 1)) - 1
            return True, p & mask
        return False, p

    def dereference(self, k: Any, p: int) -> Any:
        is_secondary, p_val = self._decode_pointer(p)
        if is_secondary:
            return self.secondary.dereference(k, p_val)
        else:
            return self.primary.dereference(k, p_val)

    def free(self, k: Any, p: int):
        is_secondary, p_val = self._decode_pointer(p)
        if is_secondary:
            self.secondary.free(k, p_val)
        else:
            self.primary.free(k, p_val)

    def get_tiny_pointer_size_bits(self) -> int:
        """
        Returns the required number of bits for a tiny pointer.
        O(log(log(log(n))) + log(1/delta)) as per the paper.
        """
        # This is a practical calculation based on our implementation
        return self.max_p_bits
