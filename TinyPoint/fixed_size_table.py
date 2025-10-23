import math
from typing import Any, Optional, Tuple

from .building_blocks import LoadBalancingTable, PowerOfTwoChoicesDerefTable

class AllocationFailed(Exception):
    pass

class FixedSizeDerefTable:
    def __init__(self, n: int, delta: float):
        if not (0 < delta < 1):
            raise ValueError("delta must be between 0 and 1")
        if n < 10:
            raise ValueError("n must be reasonably large")

        self.n = n
        self.delta = delta

        secondary_slots = math.ceil(n * delta / 2)
        primary_slots = n - secondary_slots

        self.primary = LoadBalancingTable(primary_slots, delta=delta**2)
        
        self.secondary = PowerOfTwoChoicesDerefTable(secondary_slots)

        self.primary_p_bits = self.primary.p_bits
        self.secondary_p_bits = 1 + self.secondary.p_slot_bits
        
        self.max_p_bits = max(self.primary_p_bits, self.secondary_p_bits) + 1

    def allocate(self, k: Any, value: Any) -> int:
        p_primary = self.primary.allocate(k, value)
        if p_primary is not None:
            return p_primary

        p_secondary = self.secondary.allocate(k, value)
        if p_secondary is not None:
            return (1 << self.max_p_bits -1) | p_secondary

        raise AllocationFailed(f"Allocation failed for key {k} in both primary and secondary tables.")

    def _decode_pointer(self, p: int) -> Tuple[bool, int]:
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
        return self.max_p_bits
