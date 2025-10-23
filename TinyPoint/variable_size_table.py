import math
from typing import Any, Optional, List, Tuple

from .building_blocks import LoadBalancingTable, _hash_to_int

class AllocationFailed(Exception):
    pass

class _Container:
    """
    Manages allocations for a subset of keys, as described in Prop. 1.
    Contains multiple levels of LoadBalancingTables and overflow arrays.
    """
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.num_items = 0
        
        num_levels = int(math.log2(capacity)) + 1
        self.levels: List[LoadBalancingTable] = []
        self.overflow_arrays: List[List[Optional[Tuple[Any, Any]]]] = []
        self.level_slots: List[int] = []

        s = capacity
        for _ in range(num_levels):
            if s == 0: break
            # Each level is a load balancing table. Delta is constant (e.g., 1/2)
            # so bucket size `b` is constant.
            self.levels.append(LoadBalancingTable(num_slots=s, delta=0.5))
            self.overflow_arrays.append([None] * s)
            self.level_slots.append(s)
            s //= 2
            
        self.level_occupancy = [0] * num_levels

    def allocate(self, k: Any, value: Any) -> Optional[Tuple[int, int]]:
        if self.num_items >= self.capacity:
            return None

        self.num_items += 1
        
        for i in range(len(self.levels)):
            self.level_occupancy[i] += 1
            
            # Try to allocate in the i-th load balancing table
            p = self.levels[i].allocate(k, value)
            if p is not None:
                # Success! Pointer encodes level and the LBT pointer.
                return (i, p)

            # LBT failed, check if we must use the overflow array
            next_level_occupancy = self.level_occupancy[i+1] if i + 1 < len(self.levels) else 0
            next_level_slots = self.level_slots[i+1] if i + 1 < len(self.levels) else 0

            if next_level_occupancy >= next_level_slots:
                # Use the overflow array for level i
                for j in range(len(self.overflow_arrays[i])):
                    if self.overflow_arrays[i][j] is None:
                        self.overflow_arrays[i][j] = (k, value)
                        # Pointer encodes level and overflow index, with a flag.
                        return (i | (1<<31), j) # Use MSB as overflow flag
                # Should not happen due to occupancy checks
                raise RuntimeError("Overflow array is full unexpectedly.")
        
        # Should not be reached if container has capacity
        raise RuntimeError("Failed to allocate in a container with available capacity.")

    def dereference(self, k: Any, p_container: Tuple[int, int]) -> Any:
        level, p_level = p_container
        
        is_overflow = (level & (1<<31)) != 0
        level_idx = level & ~(1<<31)

        if is_overflow:
            entry = self.overflow_arrays[level_idx][p_level]
            if entry is None:
                raise KeyError("Pointer (overflow) not allocated.")
            owner, value = entry
            if owner != k:
                raise KeyError("Ownership mismatch (overflow).")
            return value
        else:
            return self.levels[level_idx].dereference(k, p_level)

    def free(self, k: Any, p_container: Tuple[int, int]):
        level, p_level = p_container
        
        is_overflow = (level & (1<<31)) != 0
        level_idx = level & ~(1<<31)

        if is_overflow:
            entry = self.overflow_arrays[level_idx][p_level]
            if entry is None:
                raise KeyError("Pointer (overflow) already free.")
            owner, _ = entry
            if owner != k:
                raise KeyError("Ownership mismatch on free (overflow).")
            self.overflow_arrays[level_idx][p_level] = None
        else:
            self.levels[level_idx].free(k, p_level)

        self.num_items -= 1
        for i in range(level_idx + 1):
            self.level_occupancy[i] -= 1


class VariableSizeDerefTable:
    """
    An implementation of a dereference table for variable-size tiny pointers,
    as described in Theorem 2 and Proposition 1 of the "Tiny Pointers" paper.
    """
    def __init__(self, n: int):
        # n is the max number of items, not slots.
        # Total slots will be O(n).
        self.n_items = n
        
        # Each container holds log(n) items on average.
        # We set container capacity to c*log(n) for some constant c.
        self.container_capacity = max(16, int(4 * math.log2(n)) if n > 1 else 16)
        self.num_containers = math.ceil(n / math.log2(n)) if n > 1 else 1
        
        self.containers = [_Container(self.container_capacity) for _ in range(self.num_containers)]

    def _get_container_index(self, k: Any) -> int:
        kb = str(k).encode("utf-8")
        return _hash_to_int(b"vst_container:" + kb) % self.num_containers

    def allocate(self, k: Any, value: Any) -> int:
        container_idx = self._get_container_index(k)
        container = self.containers[container_idx]
        
        p_container = container.allocate(k, value)
        if p_container is None:
            raise AllocationFailed(f"Container {container_idx} is full.")
            
        level, p_level = p_container
        
        # The final tiny pointer must encode the container index and the pointer from the container.
        # This is a simplification; a real implementation would use bit packing.
        return (container_idx << 48) | (level << 32) | p_level

    def _decode_pointer(self, p: int) -> Tuple[int, Tuple[int, int]]:
        """Decodes a global pointer into (container_idx, container_pointer)."""
        container_idx = p >> 48
        level = (p >> 32) & 0xFFFF
        p_level = p & 0xFFFFFFFF
        return container_idx, (level, p_level)

    def dereference(self, k: Any, p: int) -> Any:
        container_idx, p_container = self._decode_pointer(p)
        if container_idx >= self.num_containers:
            raise KeyError("Invalid container index in pointer.")
        return self.containers[container_idx].dereference(k, p_container)

    def free(self, k: Any, p: int):
        container_idx, p_container = self._decode_pointer(p)
        if container_idx >= self.num_containers:
            raise KeyError("Invalid container index in pointer.")
        self.containers[container_idx].free(k, p_container)
