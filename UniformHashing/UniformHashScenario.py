# UniformHashScenario.py
import hashlib
import random
from typing import Any, Iterator, List


def generate_permutation(key: Any, table_size: int) -> Iterator[int]:
    """
    Sinh ra hoán vị ngẫu nhiên của [0, 1, ..., table_size-1]
    cho mỗi key, mô phỏng uniform hashing.
    """
    # Tạo seed cố định dựa trên hash SHA-256 của key
    seed_bytes = hashlib.sha256(str(key).encode()).digest()[:8]
    seed = int.from_bytes(seed_bytes, 'little')

    # Dùng random.Random(seed) để đảm bảo tái lập hoán vị
    rng = random.Random(seed)
    indices: List[int] = list(range(table_size))
    rng.shuffle(indices)  # sinh hoán vị ngẫu nhiên

    # Trả ra iterator để duyệt lần lượt các vị trí
    return iter(indices)
