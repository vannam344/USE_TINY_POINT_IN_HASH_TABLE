import random
import hashlib
from typing import Any, Iterator

def generate_permutation(key: Any, table_size: int) -> Iterator[int]:
    if isinstance(key, int):
        h = key
    else:
        ks = str(key).encode('utf-8')
        h = int(hashlib.sha256(ks).hexdigest()[:16], 16)
    
    rng = random.Random(h)
    perm = list(range(table_size))
    for i in range(table_size - 1, 0, -1):
        j = rng.randint(0, i)
        perm[i], perm[j] = perm[j], perm[i]
    
    for p in perm:
        yield p

_TOMBSTONE = object()

class SimulationHashTable:
    def __init__(self, m):
        self.m = int(m)
        self.table = [None] * self.m
        self.size = 0

    def _permutation(self, key):
        return generate_permutation(key, self.m)

    def insert(self, key, value):
        if self.size >= self.m:
            raise RuntimeError('table full')
        perm = self._permutation(key)
        probes = 0
        first_tombstone = None
        for idx in perm:
            probes += 1
            slot = self.table[idx]
            if slot is None:
                if first_tombstone is not None:
                    self.table[first_tombstone] = (key, value)
                else:
                    self.table[idx] = (key, value)
                self.size += 1
                return probes
            if slot is _TOMBSTONE:
                if first_tombstone is None:
                    first_tombstone = idx
                continue
            if slot[0] == key:
                self.table[idx] = (key, value)
                return probes
        if first_tombstone is not None:
            self.table[first_tombstone] = (key, value)
            self.size += 1
            return probes
        raise RuntimeError('no slot found')

    def search(self, key):
        perm = self._permutation(key)
        probes = 0
        for idx in perm:
            probes += 1
            slot = self.table[idx]
            if slot is None:
                return None, probes
            if slot is _TOMBSTONE:
                continue
            if slot[0] == key:
                return slot[1], probes
        return None, probes

    def delete(self, key):
        perm = self._permutation(key)
        probes = 0
        for idx in perm:
            probes += 1
            slot = self.table[idx]
            if slot is None:
                return False, probes
            if slot is _TOMBSTONE:
                continue
            if slot[0] == key:
                self.table[idx] = _TOMBSTONE
                self.size -= 1
                return True, probes
        return False, probes

    def load_factor(self):
        return self.size / self.m

def simulate_expected_retrieval(m, alpha, trials=1000, seed=0):
    rng = random.Random(seed)
    n = int(m * alpha)
    total_probes = 0
    for t in range(trials):
        ht = SimulationHashTable(m)
        keys = [rng.randrange(1 << 60) for _ in range(n)]
        for k in keys:
            ht.insert(k, k)
        sample = rng.choice(keys)
        _, probes = ht.search(sample)
        total_probes += probes
    return total_probes / trials

if __name__ == '__main__':
    import time
    m = 1009
    for alpha in [0.1, 0.3, 0.5, 0.7, 0.9]:
        start = time.time()
        avg = simulate_expected_retrieval(m, alpha, trials=200, seed=42)
        print(alpha, avg)
        print('done in', time.time() - start)
