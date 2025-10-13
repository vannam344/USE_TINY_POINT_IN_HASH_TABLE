# tinypoint/tiny_pool.py
from typing import Any, Dict

class TinyMemoryPool:
    """Bộ nhớ mô phỏng vùng lưu trữ giá trị thật."""
    def __init__(self):
        self._pool: Dict[int, Any] = {}
        self._next_id = 0

    def store(self, value: Any) -> int:
        """Lưu giá trị và trả về chỉ mục (tiny pointer)."""
        ptr = self._next_id
        self._pool[ptr] = value
        self._next_id += 1
        return ptr

    def retrieve(self, ptr: int) -> Any:
        """Giải nén pointer để đọc giá trị."""
        return self._pool.get(ptr, None)

    def delete(self, ptr: int) -> None:
        """Xóa giá trị khỏi pool."""
        if ptr in self._pool:
            del self._pool[ptr]
