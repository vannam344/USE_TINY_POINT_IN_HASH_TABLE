import random

class TinyAllocator:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.used = set()

    def allocate(self, k) -> int:
        """Chọn vị trí trống ngẫu nhiên"""
        while True:
            pos = hash((k, random.random())) % self.capacity
            if pos not in self.used:
                self.used.add(pos)
                return pos

    def free(self, pos: int):
        self.used.discard(pos)
