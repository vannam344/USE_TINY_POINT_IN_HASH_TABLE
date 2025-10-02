"""
Prototype implementation of a *dereference table* (tiny-pointer scheme)
Extended: candidates always set to sqrt(n), auto-resize when table is too full.
"""

import hashlib
import math
import struct
import random
from typing import Optional, Tuple, Dict, Any


def _hash_u64(x: bytes) -> int:
    h = hashlib.sha256(x).digest()
    return struct.unpack_from('<Q', h, 0)[0]


class AllocationError(Exception):
    """Raised when allocate() cannot find a free slot (after max_probes attempts)."""
    def __init__(self, owner: Any = None, probes: int = 0, size: int = 0, max_probes: int = 0):
        super().__init__(f"allocation failed after {probes}/{max_probes} probes")
        self.owner = owner
        self.probes = probes
        self.size = size
        self.max_probes = max_probes

    def __str__(self):
        return (f"allocation failed after {self.probes}/{self.max_probes} probes "
                f"[owner={self.owner}, table_size={self.size}]")


class DerefTable:
    def __init__(self, n: int, max_probes: Optional[int] = None):
        assert n > 0
        self.n = n
        self._update_candidates()
        self.max_probes = self.max_candidates if max_probes is None else min(self.max_candidates, max_probes)

        self.A = [None] * n
        self.owner_info = [None] * n
        self.owner_to_ps = {}

    def _update_candidates(self):
        """Update candidates = sqrt(n) and recompute p_bits, p_mask."""
        self.max_candidates = int(math.isqrt(self.n))
        if self.max_candidates < 1:
            self.max_candidates = 1
        self.p_bits = math.ceil(math.log2(self.max_candidates))
        self.p_mask = (1 << self.p_bits) - 1

    def _owner_seeds(self, k: Any) -> Tuple[int, int]:
        kb = str(k).encode("utf-8")
        seed1 = _hash_u64(b"1:" + kb)
        seed2 = _hash_u64(b"2:" + kb) | 1  # odd step
        return seed1, seed2

    def _pos_for(self, k: Any, p: int) -> int:
        seed1, step = self._owner_seeds(k)
        return (seed1 + p * step) % self.n

    def _p_sequence(self, k: Any):
        kb = str(k).encode('utf-8')
        seed_bytes = hashlib.sha256(b'seq:' + kb).digest()[:8]
        seed = int.from_bytes(seed_bytes, 'little')
        rng = random.Random(seed)
        arr = list(range(self.max_candidates))
        rng.shuffle(arr)
        for p in arr:
            yield p

    def _rehash_all(self, old_A, old_owner_info):
        for pos, entry in enumerate(old_owner_info):
            if entry is not None:
                owner, p = entry
                value = old_A[pos]
                new_pos = self._pos_for(owner, p)
                self.A[new_pos] = value
                self.owner_info[new_pos] = (owner, p)

    def _resize(self):
        old_A = self.A
        old_owner_info = self.owner_info
        old_n = self.n

        self.n *= 2
        self._update_candidates()
        self.max_probes = self.max_candidates

        self.A = [None] * self.n
        self.owner_info = [None] * self.n

        self._rehash_all(old_A, old_owner_info)
        print(f"[Resize] Table doubled from {old_n} -> {self.n}, max_candidates={self.max_candidates}")

    def allocate(self, k: Any, value: Any) -> int:
        if k not in self.owner_to_ps:
            self.owner_to_ps[k] = set()

        for attempt in range(2):  # thử 2 lần: trước và sau resize
            seq = self._p_sequence(k)
            tried = 0
            for p in seq:
                if tried >= self.max_probes:
                    break
                tried += 1

                pos = self._pos_for(k, p)
                if self.owner_info[pos] is None:
                    self.A[pos] = value
                    self.owner_info[pos] = (k, p)
                    self.owner_to_ps[k].add(p)
                    return p

            if attempt == 0:
                self._resize()
                continue

            raise AllocationError(owner=k, probes=tried, size=self.n, max_probes=self.max_probes)

    def dereference(self, k: Any, p: int) -> Any:
        if p < 0 or p >= self.max_candidates:
            raise KeyError("invalid tiny pointer")
        pos = self._pos_for(k, p)
        info = self.owner_info[pos]
        if info is None:
            raise KeyError("pointer not allocated")
        owner, stored_p = info
        if owner != k or stored_p != p:
            raise KeyError("ownership mismatch")
        return self.A[pos]

    def free(self, k: Any, p: int) -> None:
        if p < 0 or p >= self.max_candidates:
            raise KeyError("invalid tiny pointer")
        pos = self._pos_for(k, p)
        info = self.owner_info[pos]
        if info is None:
            raise KeyError("slot already free")
        owner, stored_p = info
        if owner != k or stored_p != p:
            raise KeyError("ownership mismatch on free")
        self.owner_info[pos] = None
        self.A[pos] = None
        self.owner_to_ps.get(k, set()).discard(p)


if __name__ == "__main__":
    dt = DerefTable(n=256)
    p = dt.allocate("alice", "Alice_value")
    print("alice pointer =", p)
    print("alice value =", dt.dereference("alice", p))
    dt.free("alice", p)

    print("\n--- Testing resize ---")
    for i in range(600):
        u = f"user{i}"
        p = dt.allocate(u, f"value{i}")
        print(f"{u} -> pointer {p}")
