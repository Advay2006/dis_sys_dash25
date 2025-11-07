#!/usr/bin/env python3
"""
Parallel Log Analyser using MPI (mpi4py)
Each process analyzes a subset of log files in parallel.
"""

from mpi4py import MPI
import os
import sys
import time
from collections import defaultdict

SEQ_TIME = 0.289



def analyse_log_file(filepath):
    """Analyse a single log file and count occurrences of each log level."""
    counts = defaultdict(int)
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if '[INFO]' in line:
                    counts['INFO'] += 1
                elif '[WARN]' in line or '[WARNING]' in line:
                    counts['WARN'] += 1
                elif '[ERROR]' in line:
                    counts['ERROR'] += 1
                elif '[DEBUG]' in line:
                    counts['DEBUG'] += 1
    except Exception as e:
        print(f"Rank {comm.Get_rank()}: Error reading {filepath}: {e}")
    return counts


def merge_counts(total_counts, new_counts):
    """Merge counts from one process into the total."""
    for level, count in new_counts.items():
        total_counts[level] += count



def main():
    # Initialise the MPI environment:
    # COMM_WORLD is the default communicator that includes all processes.
    comm = MPI.COMM_WORLD

    rank = comm.Get_rank()

    # Get the total number of running processes:
    size = comm.Get_size()

    if len(sys.argv) < 2:
        if rank == 0:
            print("Usage: mpirun -np <num_procs> python3 parallel_log_analyser.py <log_directory>")
        sys.exit(1)

    log_dir = sys.argv[1]

    if rank == 0:
        # all .log files
        if not os.path.isdir(log_dir):
            print(f"Error: {log_dir} is not a valid directory")
            sys.exit(1)

        all_files = [os.path.join(log_dir, f)
                     for f in os.listdir(log_dir)
                     if f.endswith('.log')]

        if not all_files:
            print(f"No .log files found in {log_dir}")
            sys.exit(1)

        print(f"Found {len(all_files)} log file(s) to analyse using {size} processes.")
    else:
        all_files = None

    global_start = MPI.Wtime()

   
    all_files = comm.bcast(all_files, root=0)

    # Round robin splice
    my_files = all_files[rank::size]

    # each rank does this
    local_counts = defaultdict(int)
    start = MPI.Wtime()
    for log_file in my_files:
        counts = analyse_log_file(log_file)
        merge_counts(local_counts, counts)
    end = MPI.Wtime()
    local_time = end - start

    # Gather everything at rnk 0
    gathered_counts = comm.gather(local_counts, root=0)
    gathered_times = comm.gather(local_time, root=0)

    
    if rank == 0:
        total_counts = defaultdict(int)
        for c in gathered_counts:
            merge_counts(total_counts, c)
        
        global_end = MPI.Wtime()
        global_time = global_end-global_start

        print("\n" + "="*50)
        print("PARALLEL ANALYSIS RESULTS")
        print("="*50)
        for level in sorted(total_counts.keys()):
            print(f"{level}: {total_counts[level]}")
        print("="*50)
        print("Avg Sequential time: ", SEQ_TIME, "sec")
        print(f"Total time (wall clock): {max(gathered_times):.3f}s")
        print(f"Global time: {global_time:.3f}s")
        print(f"Average rank processing time: {sum(gathered_times)/len(gathered_times):.3f}s")
        print(f"Speedup (sequential_time / global_parallel_time): {SEQ_TIME/global_time:.2f}")
        speedup = SEQ_TIME/global_time
        print(f"Efficiency analysis (speedup / number_of_processes): {speedup/size:.3f}")
        print("="*50)



if __name__ == "__main__":
    main()

MPI.Finalize()

'''
2. **`report_stage1.txt`** containing:
   - **Execution times** for 1, 2, 4, and 8 processes (if possible)
   - **Speedup calculations** (sequential_time / parallel_time)
   - **Efficiency analysis** (speedup / number_of_processes)
   - **Work distribution strategy** explanation
   - **Challenges faced** and how you solved them
   - **Observations** about scalability

'''