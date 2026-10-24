import os
import re
from concurrent.futures import ThreadPoolExecutor

# ==========================================
# 1. MAPPER PHASE
# ==========================================
def mapper(file_path):
    """
    Reads a split file and emits (word, 1) pairs for each token.
    """
    pairs = []
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read().lower()
        # Tokenize words using alphanumeric characters
        words = re.findall(r'\b[a-z0-9]+\b', text)
        for word in words:
            pairs.append((word, 1))
    return pairs

# ==========================================
# 2. COMBINER PHASE (Local Aggregation)
# ==========================================
def combiner(pairs):
    """
    Computes local subtotals for each mapper output before partitioning.
    Reduces data transferred to the reducers.
    """
    local_counts = {}
    for word, count in pairs:
        local_counts[word] = local_counts.get(word, 0) + count
    return list(local_counts.items())

# ==========================================
# 3. PARTITIONER PHASE
# ==========================================
def partitioner(word, num_reducers=2):
    """
    Hashes the word to consistently route it to one of the reducers (0 or 1).
    """
    return abs(hash(word)) % num_reducers

# ==========================================
# 4. SHUFFLE & SORT PHASE
# ==========================================
def shuffle_and_sort(intermediate_pairs):
    """
    Groups all values for each key and sorts keys alphabetically.
    Example:
      Input:  [('car', 1), ('car', 2), ('damage', 1)]
      Output: {'car': [1, 2], 'damage': [1]}
    """
    grouped_data = {}
    for word, count in intermediate_pairs:
        if word not in grouped_data:
            grouped_data[word] = []
        grouped_data[word].append(count)
    
    # Sort keys alphabetically
    sorted_grouped_data = {k: grouped_data[k] for k in sorted(grouped_data.keys())}
    return sorted_grouped_data

# ==========================================
# 5. REDUCER PHASE
# ==========================================
def reducer(word, counts):
    """
    Aggregates list of counts into a single final sum.
    """
    return word, sum(counts)

# ==========================================
# MAIN MAPREDUCE ENGINE
# ==========================================
def run_mapreduce(input_dir, output_dir, num_reducers=2):
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    os.makedirs(output_dir, exist_ok=True)
    
    # Collect all text files (splits)
    split_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.txt')]
    print(f"[*] Reading splits from: {input_dir}")
    print(f"[*] Found {len(split_files)} split(s): {[os.path.basename(f) for f in split_files]}")
    
    if len(split_files) < 2:
        raise ValueError(f"Error: Found only {len(split_files)} split(s). At least 2 splits are required.")

    # Step 1: Run Mappers + Combiner in parallel using threads
    combined_mapper_outputs = []
    def process_split(file_path):
        return combiner(mapper(file_path))

    print("[*] Running Mappers in parallel...")
    with ThreadPoolExecutor(max_workers=len(split_files)) as executor:
        for res in executor.map(process_split, split_files):
            combined_mapper_outputs.append(res)

    # Step 2: Partition intermediate records into reducer buckets
    reducers_buckets = [[] for _ in range(num_reducers)]
    for mapper_result in combined_mapper_outputs:
        for word, count in mapper_result:
            r_id = partitioner(word, num_reducers)
            reducers_buckets[r_id].append((word, count))

    # Step 3: Shuffle & Sort, then Reduce per reducer bucket
    print("[*] Shuffling, Sorting, and Reducing for each Reducer...")
    for r_id in range(num_reducers):
        sorted_grouped = shuffle_and_sort(reducers_buckets[r_id])
        output_file_path = os.path.join(output_dir, f"reducer_{r_id}_output.txt")
        
        with open(output_file_path, 'w', encoding='utf-8') as out_f:
            for word, counts in sorted_grouped.items():
                w, total_sum = reducer(word, counts)
                out_f.write(f"{w}\t{total_sum}\n")
                print(f"  [Reducer {r_id}] {w}: {total_sum}")
                
        print(f"[+] Output written to: {output_file_path}")

if __name__ == "__main__":
    # Base path of your project
    BASE_DIR = r"C:\Users\Hp\OneDrive\Desktop\My_Assignment"
    INPUT_FOLDER = os.path.join(BASE_DIR, "input_splits")
    OUTPUT_FOLDER = os.path.join(BASE_DIR, "output")
    
    run_mapreduce(INPUT_FOLDER, OUTPUT_FOLDER, num_reducers=2)