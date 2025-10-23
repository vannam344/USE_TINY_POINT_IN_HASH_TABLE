import unittest
from .deref_table import create_deref_table, PointerType, AllocationFailed

class TestTinyPointerImplementations(unittest.TestCase):

    def test_fixed_size_table_basic(self):
        print("\n--- Testing FixedSizeDerefTable ---")
        dt = create_deref_table(n=256, pointer_type=PointerType.FIXED, delta=0.1)
        
        p_alice = dt.allocate("alice", "value_for_alice")
        self.assertIsInstance(p_alice, int)
        print(f"Allocated 'alice' with pointer: {p_alice}")

        value = dt.dereference("alice", p_alice)
        self.assertEqual(value, "value_for_alice")
        print(f"Dereferenced pointer {p_alice} for 'alice' to get value: '{value}'")

        dt.free("alice", p_alice)
        print(f"Freed pointer {p_alice} for 'alice'")

        with self.assertRaises(KeyError):
            dt.dereference("alice", p_alice)
        print("Dereference after free correctly raised KeyError.")

    def test_variable_size_table_basic(self):
        print("\n--- Testing VariableSizeDerefTable ---")
        dt = create_deref_table(n=200, pointer_type=PointerType.VARIABLE)
        
        p_bob = dt.allocate("bob", "value_for_bob")
        self.assertIsInstance(p_bob, int)
        print(f"Allocated 'bob' with pointer: {p_bob}")

        value = dt.dereference("bob", p_bob)
        self.assertEqual(value, "value_for_bob")
        print(f"Dereferenced pointer {p_bob} for 'bob' to get value: '{value}'")

        dt.free("bob", p_bob)
        print(f"Freed pointer {p_bob} for 'bob'")

        with self.assertRaises(KeyError):
            dt.dereference("bob", p_bob)
        print("Dereference after free correctly raised KeyError.")

    def test_fixed_size_table_overflow(self):
        print("\n--- Testing FixedSizeDerefTable Overflow ---")
        dt = create_deref_table(n=50, pointer_type=PointerType.FIXED, delta=0.2)
        pointers = {}
        
        try:
            for i in range(45):
                key = f"user_{i}"
                value = f"value_{i}"
                p = dt.allocate(key, value)
                pointers[key] = p
            print(f"Successfully allocated {len(pointers)} items.")
        except Exception as e:
            self.fail(f"Allocation failed unexpectedly: {e}")

        for key, p in pointers.items():
            self.assertEqual(dt.dereference(key, p), f"value_{key.split('_')[1]}")
        print("All allocated items verified successfully.")

if __name__ == '__main__':
    unittest.main()
