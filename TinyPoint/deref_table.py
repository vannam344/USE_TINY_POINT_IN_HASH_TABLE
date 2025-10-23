from enum import Enum
from typing import Any

from .fixed_size_table import FixedSizeDerefTable, AllocationFailed as FixedAllocFailed
from .variable_size_table import VariableSizeDerefTable, AllocationFailed as VarAllocFailed

class PointerType(Enum):
    FIXED = "fixed"
    VARIABLE = "variable"

class DerefTable:
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
    if pointer_type == PointerType.FIXED:
        impl = FixedSizeDerefTable(n, delta)
    elif pointer_type == PointerType.VARIABLE:
        impl = VariableSizeDerefTable(n)
    else:
        raise ValueError(f"Unknown pointer type: {pointer_type}")
        
    return DerefTable(impl)

AllocationFailed = (FixedAllocFailed, VarAllocFailed)
