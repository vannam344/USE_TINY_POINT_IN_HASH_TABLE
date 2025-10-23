"""
This module provides a factory function to create dereference tables
based on the "Tiny Pointers" paper by Bender et al.
"""

from enum import Enum
from typing import Any

from .fixed_size_table import FixedSizeDerefTable, AllocationFailed as FixedAllocFailed
from .variable_size_table import VariableSizeDerefTable, AllocationFailed as VarAllocFailed

class PointerType(Enum):
    FIXED = "fixed"
    VARIABLE = "variable"

class DerefTable:
    """
    A wrapper class that provides a unified interface for different
    tiny pointer schemes.
    """
    def __init__(self, implementation: Any):
        self._impl = implementation

    def allocate(self, k: Any, value: Any) -> int:
        return self._impl.allocate(k, value)

    def dereference(self, k: Any, p: int) -> Any:
        return self._impl.dereference(k, p)

    def free(self, k: Any, p: int):
        self._impl.free(k, p)

    @property
    def n(self):
        return self._impl.n

def create_deref_table(
    n: int,
    pointer_type: PointerType = PointerType.FIXED,
    delta: float = 0.1
) -> DerefTable:
    """
    Factory function to create a dereference table.

    Args:
        n: The capacity (number of slots) for the table.
        pointer_type: The type of tiny pointers to use (FIXED or VARIABLE).
        delta: The desired wasted space ratio (1 - load_factor).
               Only used for FIXED pointer type.

    Returns:
        An instance of a dereference table wrapped in a unified interface.
    """
    if pointer_type == PointerType.FIXED:
        impl = FixedSizeDerefTable(n, delta)
    elif pointer_type == PointerType.VARIABLE:
        # For variable size, n is the number of items, not slots.
        # We adjust to create a table with roughly n slots.
        # delta is implicitly handled by the construction which has O(n) slots for n items.
        impl = VariableSizeDerefTable(n)
    else:
        raise ValueError(f"Unknown pointer type: {pointer_type}")
        
    return DerefTable(impl)

# Expose custom exceptions
AllocationFailed = (FixedAllocFailed, VarAllocFailed)
