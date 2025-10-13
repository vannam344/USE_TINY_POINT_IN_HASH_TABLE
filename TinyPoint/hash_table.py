# tinypoint/hash_table.py
from deref_table import DerefTable
from typing import Any, Optional

class TinyHashTable:
    """
    Hash table sử dụng Tiny Pointer scheme (DerefTable).
    Mỗi key được ánh xạ tới một tiny pointer p trong DerefTable,
    thay vì lưu trực tiếp value trong bảng chính.
    """

    def __init__(self, initial_capacity: int = 256):
        self.deref = DerefTable(n=initial_capacity)
        self.index_table = {}   # ánh xạ key -> tiny pointer p

    def insert(self, key: Any, value: Any) -> int:
        """
        Lưu value thông qua DerefTable, nhận tiny pointer p.
        """
        p = self.deref.allocate(key, value)
        self.index_table[key] = p
        return p

    def get(self, key: Any) -> Optional[Any]:
        """
        Truy xuất value qua dereference(k, p)
        """
        p = self.index_table.get(key)
        if p is None:
            return None
        try:
            return self.deref.dereference(key, p)
        except KeyError:
            return None

    def remove(self, key: Any) -> bool:
        """
        Giải phóng con trỏ tiny pointer của key.
        """
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
