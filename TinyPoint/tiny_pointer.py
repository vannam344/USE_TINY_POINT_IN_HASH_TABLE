import random
import math

class TinyPointer:
    def __init__(self, index: int, bits: int):
        self.bits = bits
        self.value = index & ((1 << bits) - 1)

    def encode(self) -> int:
        return self.value

    @staticmethod
    def decode(k: int, p: int, n: int) -> int:
        """Dùng hash kết hợp key để xác định slot thực tế"""
        seed = hash((k, p)) & ((1 << 64) - 1)
        return seed % n
