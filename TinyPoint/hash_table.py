from .deref_table import create_deref_table, PointerType, AllocationFailed
from typing import Any, Optional

class TinyHashTable:
    def __init__(self, initial_capacity: int = 256, pointer_type: PointerType = PointerType.FIXED):
        self.deref = create_deref_table(n=initial_capacity, pointer_type=pointer_type)
        self.index_table = {}

    def insert(self, key: Any, value: Any) -> int:
        try:
            p = self.deref.allocate(key, value)
            self.index_table[key] = p
            return p
        except AllocationFailed as e:
            print(f"Warning: DerefTable allocation failed: {e}")
            raise

    def get(self, key: Any) -> Optional[Any]:
        p = self.index_table.get(key)
        if p is None:
            return None
        try:
            return self.deref.dereference(key, p)
        except KeyError:
            return None

    def remove(self, key: Any) -> bool:
        p = self.index_table.pop(key, None)
        if p is None:
            return False
        try:
            self.deref.free(key, p)
            return True
        except KeyError:
            return False

    def __len__(self):
        return len(self.index_table)

    def __repr__(self):
        return f"TinyHashTable(size={len(self)}, deref_size={self.deref.n})"
