# CollisionHandlers.py
import hashlib
from typing import Any, Iterator, Protocol
from UniformHashScenario import generate_permutation  

class CollisionStrategy(Protocol):
    def get_probe_sequence(self, key: Any, table_size: int) -> Iterator[int]: ...

class UniformHashStrategy:
    def get_probe_sequence(self, key: Any, table_size: int) -> Iterator[int]:
        return generate_permutation(key, table_size)

class DoubleHashStrategy:
    def __init__(self):
        self._prime = 1099511628211  # 64-bit prime
    
    def get_probe_sequence(self, key: Any, table_size: int) -> Iterator[int]:
        h1 = self._hash1(key, table_size)
        h2 = self._hash2(key, table_size)
        h2 = h2 if h2 % 2 == 1 else h2 + 1  # ensure odd
        
        for i in range(table_size):
            yield (h1 + i * h2) % table_size
    
    def _hash1(self, key: Any, size: int) -> int:
        key_bytes = str(key).encode()
        return int.from_bytes(hashlib.md5(key_bytes).digest(), 'little') % size
    
    def _hash2(self, key: Any, size: int) -> int:
        key_bytes = str(key).encode()
        return int.from_bytes(hashlib.sha1(key_bytes).digest(), 'little') % self._prime
