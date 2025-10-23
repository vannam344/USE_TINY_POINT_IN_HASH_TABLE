import math
import random
from typing import Optional, Tuple, Any

def _hash_to_int(data: bytes) -> int:
    """Helper to hash bytes to a 64-bit integer."""
    import hashlib
    import struct
    h = hashlib.sha256(data).digest()
    return struct.unpack_from('<Q', h, 0)[0]

class LoadBalancingTable:
    """
    Building Block 1: A simple dereference table based on load balancing.
    As described in Section 3 of the paper "Tiny Pointers".

    The store is partitioned into buckets. A key `k` is hashed to a single bucket.
    If the bucket has a free slot, the allocation succeeds. Otherwise, it fails.
    The tiny pointer is the index of the slot within the bucket.
    """
    def __init__(self, num_slots: int, delta: float):
        if num_slots == 0:
            raise ValueError("Number of slots must be positive.")
        
        # b = Theta(delta^-2 * log(delta^-1))
        # For simplicity, we use a simpler heuristic, ensuring bucket_size is at least 2.
        self.delta = delta
        bucket_size_float = (1 / delta**2) * math.log2(1/delta) if delta > 0 else 2
        self.bucket_size = max(2, int(math.ceil(bucket_size_float)))
        
        self.num_buckets = math.ceil(num_slots / self.bucket_size)
        if self.num_buckets == 0:
            self.num_buckets = 1
        
        self.n = self.num_buckets * self.bucket_size
        
        # The store contains the actual values
        self.store = [None] * self.n
        # owner_info tracks (key, tiny_pointer) for each slot
        self.owner_info = [None] * self.n
        
        self.p_bits = math.ceil(math.log2(self.bucket_size))

    def _get_bucket_index(self, k: Any) -> int:
        """Hash a key to a bucket index."""
        kb = str(k).encode("utf-8")
        return _hash_to_int(b"lbt:" + kb) % self.num_buckets

    def allocate(self, k: Any, value: Any) -> Optional[int]:
        """
        Tries to allocate a value for key k.
        Returns the tiny pointer (slot index within bucket) on success, None on failure.
        """
        bucket_idx = self._get_bucket_index(k)
        start_pos = bucket_idx * self.bucket_size
        
        for i in range(self.bucket_size):
            pos = start_pos + i
            if self.owner_info[pos] is None:
                self.store[pos] = value
                p = i  # Tiny pointer is the slot index
                self.owner_info[pos] = (k, p)
                return p
        
        return None # Allocation fails if bucket is full

    def dereference(self, k: Any, p: int) -> Any:
        """
        Dereferences a tiny pointer p for a key k to get the value.
        """
        if not (0 <= p < self.bucket_size):
            raise KeyError("Invalid tiny pointer value.")
            
        bucket_idx = self._get_bucket_index(k)
        pos = bucket_idx * self.bucket_size + p
        
        info = self.owner_info[pos]
        if info is None:
            raise KeyError("Pointer not allocated.")
        
        owner, stored_p = info
        if owner != k or stored_p != p:
            # This indicates a hash collision or incorrect key/pointer pair
            raise KeyError("Ownership mismatch during dereference.")
            
        return self.store[pos]

    def free(self, k: Any, p: int):
        """
        Frees the slot associated with key k and tiny pointer p.
        """
        if not (0 <= p < self.bucket_size):
            raise KeyError("Invalid tiny pointer value.")

        bucket_idx = self._get_bucket_index(k)
        pos = bucket_idx * self.bucket_size + p

        info = self.owner_info[pos]
        if info is None:
            raise KeyError("Slot is already free.")
            
        owner, stored_p = info
        if owner != k or stored_p != p:
            raise KeyError("Ownership mismatch on free.")
            
        self.store[pos] = None
        self.owner_info[pos] = None

class PowerOfTwoChoicesDerefTable:
    """
    Building Block 2: A dereference table using power-of-two-choices.
    As described in Lemma 2 of the paper "Tiny Pointers".

    A key `k` is hashed to two buckets. It is allocated in the less full bucket.
    The tiny pointer encodes which of the two buckets and which slot within it.
    """
    def __init__(self, num_slots: int):
        if num_slots == 0:
            raise ValueError("Number of slots must be positive.")

        # According to the paper, bucket size b = Theta(log log n)
        # and load factor is Theta(1 / log log n).
        # We'll use log log n for bucket size, clamped to a minimum of 2.
        log_n = math.log2(num_slots) if num_slots > 1 else 1
        self.bucket_size = max(2, int(math.ceil(math.log2(log_n))))
        
        self.num_buckets = math.ceil(num_slots / self.bucket_size)
        if self.num_buckets < 2:
            self.num_buckets = 2 # Need at least 2 buckets for power-of-two-choices
        
        self.n = self.num_buckets * self.bucket_size
        
        self.store = [None] * self.n
        self.owner_info = [None] * self.n
        self.bucket_occupancy = [0] * self.num_buckets

        # Tiny pointer needs 1 bit for bucket choice + bits for slot index
        self.p_slot_bits = math.ceil(math.log2(self.bucket_size))
        self.p_slot_mask = (1 << self.p_slot_bits) - 1

    def _get_bucket_indices(self, k: Any) -> Tuple[int, int]:
        """Hash a key to two different bucket indices."""
        kb = str(k).encode("utf-8")
        h1 = _hash_to_int(b"p2c1:" + kb) % self.num_buckets
        h2 = _hash_to_int(b"p2c2:" + kb) % self.num_buckets
        if h1 == h2: # Ensure buckets are distinct
            h2 = (h1 + 1) % self.num_buckets
        return h1, h2

    def allocate(self, k: Any, value: Any) -> Optional[int]:
        """
        Tries to allocate a value for key k.
        Returns the tiny pointer on success, None on failure.
        """
        b1_idx, b2_idx = self._get_bucket_indices(k)

        # Choose the bucket with more free slots
        if self.bucket_occupancy[b1_idx] <= self.bucket_occupancy[b2_idx]:
            chosen_bucket_idx = b1_idx
            p_bucket_choice = 0
        else:
            chosen_bucket_idx = b2_idx
            p_bucket_choice = 1

        if self.bucket_occupancy[chosen_bucket_idx] >= self.bucket_size:
            return None # Chosen bucket is full

        start_pos = chosen_bucket_idx * self.bucket_size
        for i in range(self.bucket_size):
            pos = start_pos + i
            if self.owner_info[pos] is None:
                p_slot = i
                p = (p_bucket_choice << self.p_slot_bits) | p_slot
                
                self.store[pos] = value
                self.owner_info[pos] = (k, p)
                self.bucket_occupancy[chosen_bucket_idx] += 1
                return p
        
        return None # Should not be reached if occupancy is correct

    def _decode_pointer(self, k: Any, p: int) -> int:
        """Decodes a tiny pointer to find the absolute position in the store."""
        p_bucket_choice = p >> self.p_slot_bits
        p_slot = p & self.p_slot_mask

        if p_slot >= self.bucket_size:
             raise KeyError("Invalid slot index in tiny pointer.")

        b1_idx, b2_idx = self._get_bucket_indices(k)
        chosen_bucket_idx = b1_idx if p_bucket_choice == 0 else b2_idx
        
        return chosen_bucket_idx * self.bucket_size + p_slot

    def dereference(self, k: Any, p: int) -> Any:
        pos = self._decode_pointer(k, p)
        
        info = self.owner_info[pos]
        if info is None:
            raise KeyError("Pointer not allocated.")
            
        owner, stored_p = info
        if owner != k or stored_p != p:
            raise KeyError("Ownership mismatch during dereference.")
            
        return self.store[pos]

    def free(self, k: Any, p: int):
        pos = self._decode_pointer(k, p)
        
        info = self.owner_info[pos]
        if info is None:
            raise KeyError("Slot is already free.")
            
        owner, stored_p = info
        if owner != k or stored_p != p:
            raise KeyError("Ownership mismatch on free.")
            
        self.store[pos] = None
        self.owner_info[pos] = None
        
        bucket_idx = pos // self.bucket_size
        self.bucket_occupancy[bucket_idx] -= 1
