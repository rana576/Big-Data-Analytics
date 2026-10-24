
## Student Information
- Name: Rana Saied Ismael
- Student ID: 231006205

# Custom MapReduce Engine (Bonus Assignment)

## Overview
A lightweight MapReduce engine implemented in Python from scratch (no Hadoop, no Spark) satisfying all assignment requirements:
1. **Parallel Mapping:** Uses `ThreadPoolExecutor` to process multiple input splits concurrently.
2. **Map Phase:** Emits `(word, 1)` pairs.
3. **Combiner:** Computes local subtotals for each split.
4. **Partitioner:** Routes keys to 2 reducers using `abs(hash(key)) % 2`.
5. **Shuffle & Sort:** Groups and sorts keys per reducer.
6. **Reduce Phase:** Computes the final word counts and writes one output file per reducer.

---

## How to Run
1. Place text splits inside `input_splits/` (at least 2 files).
2. Run the script:
   ```bash
   python mapreduce.py
