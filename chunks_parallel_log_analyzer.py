from mpi4py import MPI
from collections import defaultdict
import os, sys, time


SEQ_TIME = 0.289

def analyse_log_file(filepath):
    counts = defaultdict(int)
    try:
        with open(filepath, "r") as f:
            for line in f:
                if "[INFO]" in line:
                    counts["INFO"] += 1
                elif "[WARN]" in line or "[WARNING]" in line:
                    counts["WARN"] += 1
                elif "[ERROR]" in line:
                    counts["ERROR"] += 1
                elif "[DEBUG]" in line:
                    counts["DEBUG"] += 1
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return counts

def merge_counts(total, new):
    for level, count in new.items():
        total[level] += count


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    if len(sys.argv) < 2:
        print("Usage: mpirun -np <n> python3 parallel_log_analyser.py <log_dir>")
        sys.exit(1)
    log_dir = sys.argv[1]
    log_files = [os.path.join(log_dir, f) for f in os.listdir(log_dir) if f.endswith(".log")]
else:
    log_files = None

global_start = MPI.Wtime()


log_files = comm.bcast(log_files, root=0)
num_files = len(log_files)

#Chunking
chunk_size = (num_files + size - 1) // size  # ceiling division
start = rank * chunk_size
end = min(start + chunk_size, num_files)
my_files = log_files[start:end]


local_counts = defaultdict(int)
for f in my_files:
    counts = analyse_log_file(f)
    merge_counts(local_counts, counts)


all_counts = comm.gather(local_counts, root=0)

if rank == 0:
    total_counts = defaultdict(int)
    for c in all_counts:
        merge_counts(total_counts, c)
    
    global_end = MPI.Wtime()
    global_time = global_end - global_start

    print("\n=== FINAL RESULTS ===")
    for lvl in sorted(total_counts.keys()):
        print(f"{lvl}: {total_counts[lvl]}")
    
    print(f"Global time: {global_time:.3f}s")
    print(f"Speedup (sequential_time / global_parallel_time): {SEQ_TIME/global_time:.2f}x")
    speedup = SEQ_TIME/global_time
    print(f"Efficiency analysis (speedup / number_of_processes): {speedup/size:.3f}")
    print("="*50)

MPI.Finalize()