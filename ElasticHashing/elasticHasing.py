import math
import random
import os

class ElasticHashing:
    def __init__(self, n, delta=0.05):
        self.n = n
        self.delta = delta
        self.sub_arrays = []
        self.current_sizes = []
        
        # Bước 1: Chia bảng băm thành các mảng nhỏ (Phân tầng logarit)
        remaining_n = n
        current_size = n // 2
        while remaining_n > 0 and current_size >= 1:
            size = min(remaining_n, current_size)
            self.sub_arrays.append([None] * size)
            self.current_sizes.append(0)
            remaining_n -= size
            current_size //= 2
        
        if remaining_n > 0: # Thêm phần dư nếu có
            self.sub_arrays.append([None] * remaining_n)
            self.current_sizes.append(0)

    # Bước 2: Hàm phi(i, j) - Ánh xạ 2D -> 1D bằng cách ghép bit
    def phi(self, i, j):
        bin_i = bin(i)[2:]  # Bỏ '0b'
        bin_j = bin(j)[2:]
        
        # Phần đầu: xen kẽ 1 và các bit của j
        head = "".join(["1" + bit for bit in bin_j])
        # Kết quả: head + "00" + bin_i
        phi_bin = head + "00" + bin_i
        return int(phi_bin, 2)

    # Hàm băm mô phỏng chuỗi dò tìm hi,j(x)
    def hash_probe(self, x, i_idx, j_idx):
        # Sử dụng seed dựa trên phi(i, j) để tạo tính ngẫu nhiên nhưng nhất quán
        combined_seed = self.phi(i_idx + 1, j_idx + 1)
        random.seed(hash(x) + combined_seed)
        return random.randint(0, len(self.sub_arrays[i_idx]) - 1)

    # Bước 3: Thuật toán chèn theo logic 3 trường hợp
    def insert(self, x, i_idx):
        # i_idx là chỉ số mảng đang xét (từ Batch Bi)
        # Giả sử ta đang xét cặp mảng Ai và Ai+1
        if i_idx >= len(self.sub_arrays) - 1:
            i_idx = len(self.sub_arrays) - 2

        arr_i = self.sub_arrays[i_idx]
        arr_next = self.sub_arrays[i_idx + 1]
        
        # Tính toán epsilon (độ trống)
        eps1 = 1.0 - (self.current_sizes[i_idx] / len(arr_i))
        eps2 = 1.0 - (self.current_sizes[i_idx + 1] / len(arr_next))

        # f(epsilon) đơn giản hóa cho mục đích minh họa
        f_eps1 = max(1, int(math.log2(1/(eps1 + 1e-9))**2))

        # Trường hợp 2: Ai quá đầy -> Bắt buộc vào Ai+1
        if eps1 <= self.delta / 2:
            self._probe_and_insert(x, i_idx + 1)
        
        # Trường hợp 3: Ai+1 quá đầy -> Bắt buộc vào Ai
        elif eps2 <= 0.25:
            self._probe_and_insert(x, i_idx)
            
        # Trường hợp 1: Bình thường -> Ưu tiên Ai, sau đó tới Ai+1
        else:
            inserted = False
            # Thử f(eps1) lần trong Ai
            for j in range(1, f_eps1 + 1):
                pos = self.hash_probe(x, i_idx, j)
                if arr_i[pos] is None:
                    arr_i[pos] = x
                    self.current_sizes[i_idx] += 1
                    inserted = True
                    break
            
            if not inserted:
                self._probe_and_insert(x, i_idx + 1)

    def _probe_and_insert(self, x, array_idx):
        """Dò tìm vị trí trống đầu tiên trong một mảng cụ thể"""
        j = 1
        while True:
            pos = self.hash_probe(x, array_idx, j)
            if self.sub_arrays[array_idx][pos] is None:
                self.sub_arrays[array_idx][pos] = x
                self.current_sizes[array_idx] += 1
                return
            j += 1
            if j > 1000: # Cắt đuôi để tránh lặp vô hạn nếu mảng quá đầy
                break

def read_data_from_file(file_path):
    """
    Read data from file and return list of items.

    Args:
        file_path (str): Path to the data file

    Returns:
        list: List of data items
    """
    data = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#'):
                    data.append(line)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []
    except Exception as e:
        print(f"Error reading file: {e}")
        return []

    return data

def calculate_optimal_hash_size(data_size, target_load_factor=0.8):
    """
    Calculate optimal hash table size based on data size and target load factor.

    Args:
        data_size (int): Number of data items
        target_load_factor (float): Desired load factor

    Returns:
        int: Optimal hash table size
    """
    if data_size == 0:
        return 1000  # Default size

    optimal_n = math.ceil(data_size / target_load_factor)

    # Round up to a nice number (multiple of 100 or 1000)
    if optimal_n < 1000:
        optimal_n = math.ceil(optimal_n / 100) * 100
    else:
        optimal_n = math.ceil(optimal_n / 1000) * 1000

    return optimal_n

def run_elastic_hashing_simulation(data_file="../data/elasticHashingData.txt", target_load_factor=0.8):
    """
    Run elastic hashing simulation with data from file.

    Args:
        data_file (str): Path to data file
        target_load_factor (float): Target load factor for hash table sizing

    Returns:
        dict: Simulation results
    """
    # Read data from file
    data = read_data_from_file(data_file)

    if not data:
        print("No data found. Please check the data file.")
        return None

    print(f"Read {len(data)} items from {data_file}")

    # Calculate optimal hash table size
    optimal_n = calculate_optimal_hash_size(len(data), target_load_factor)
    print(f"Optimal hash table size: {optimal_n} (for {len(data)} items at {target_load_factor:.1f} load factor)")

    # Create elastic hash table
    eh = ElasticHashing(optimal_n)

    print(f"\n--- Cấu trúc các mảng con (A1 -> Ak) ---")
    for idx, arr in enumerate(eh.sub_arrays):
        print(f"Mảng A{idx+1}: kích thước {len(arr)}")

    # Mô phỏng chèn theo từng Batch
    # Số lượng batch phụ thuộc vào số lượng mảng con
    num_batches = min(4, len(eh.sub_arrays) - 1)  # Tối đa 4 batch hoặc số mảng - 1
    batch_size = len(data) // num_batches

    for b in range(num_batches):
        start_idx = b * batch_size
        end_idx = (b + 1) * batch_size if b < num_batches - 1 else len(data)
        current_batch = data[start_idx:end_idx]

        # Mỗi batch sẽ tập trung chèn vào cặp mảng Ai, Ai+1 tương ứng
        target_i = min(b, len(eh.sub_arrays) - 2)

        for item in current_batch:
            eh.insert(item, target_i)

        print(f"\nSau Batch {b} (chèn vào A{target_i+1} và A{target_i+2}):")
        for idx, count in enumerate(eh.current_sizes):
            capacity = len(eh.sub_arrays[idx])
            load = (count/capacity)*100 if capacity > 0 else 0
            print(f"  A{idx+1}: Đã chứa {count}/{capacity} ({load:.1f}%)")

    # Thống kê cuối cùng
    total_stored = sum(eh.current_sizes)
    actual_load_factor = total_stored / optimal_n

    print(f"\n--- Kết quả cuối cùng ---")
    print(f"Tổng số phần tử đã chèn thành công: {total_stored}/{len(data)}")
    print(f"Load factor thực tế: {actual_load_factor:.3f}")

    # Trả về kết quả
    return {
        'hash_table': eh,
        'data': data,
        'total_items': total_stored,
        'hash_table_size': optimal_n,
        'load_factor': actual_load_factor,
        'array_structure': [(len(arr), count) for arr, count in zip(eh.sub_arrays, eh.current_sizes)]
    }

# --- CHƯƠNG TRÌNH CHÍNH ---

if __name__ == "__main__":
    # Chạy simulation với file data mặc định
    result = run_elastic_hashing_simulation()

    if result:
        print(f"\n--- Chi tiết cấu trúc bảng băm ---")
        for i, (size, count) in enumerate(result['array_structure']):
            print(f"Mảng A{i+1}: kích thước {size}, đã sử dụng {count}, còn trống {size - count}")
