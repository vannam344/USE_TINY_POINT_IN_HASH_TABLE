import math
import random
import os

def generate_data_for_hashing(n=1000, load_factor=0.8, output_file="data/elasticHashingData.txt"):
    num_items = int(n * load_factor)
    data = [f"Item_{i}" for i in range(num_items)]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# Elastic Hashing Data for n={n}, load_factor={load_factor}\n")
        f.write(f"# Generated {num_items} items\n")
        f.write("# Each line contains one data item\n\n")

        for item in data:
            f.write(f"{item}\n")

    print(f"Generated {num_items} data items for hash table of size {n}")
    print(f"Data saved to: {os.path.abspath(output_file)}")

    return data

if __name__ == "__main__":
    data = generate_data_for_hashing()
    print(f"\nData generation complete. Generated {len(data)} items.")
