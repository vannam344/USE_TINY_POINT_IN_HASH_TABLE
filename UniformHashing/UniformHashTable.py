# UniformHashTable.py
from UniformHashScenario import generate_permutation  
from CollisionHandlers import CollisionStrategy, UniformHashStrategy
from typing import Any, Iterator, Optional

class UniformHashTable:
    def __init__(
        self, 
        initial_size: int = 64,
        strategy: CollisionStrategy = UniformHashStrategy()
    ):
        self.size = initial_size
        self.table = [None] * self.size
        self.strategy = strategy
        self.load_factor = 0.75
        self.count = 0

    def _probe_sequence(self, key: Any) -> Iterator[int]:
        return self.strategy.get_probe_sequence(key, self.size)

    def _resize(self):
        old_size = self.size
        self.size *= 2
        old_table = self.table
        self.table = [None] * self.size
        self.count = 0
        
        for entry in old_table:
            if entry:
                key, value = entry
                for pos in self._probe_sequence(key):
                    if self.table[pos] is None:
                        self.table[pos] = (key, value)
                        self.count += 1
                        break

    def insert(self, key: Any, value: Any) -> int:
        if self.count / self.size >= self.load_factor:
            self._resize()
            
        # probe_seq = list(self._probe_sequence(key))
        # print(f"Probe sequence for {key}: {probe_seq}")

        for pos in self._probe_sequence(key):
            if self.table[pos] is None:
                self.table[pos] = (key, value)
                self.count += 1
                return pos
        raise RuntimeError("Hash table full")
