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

        local_counts = defaultdict(int)
        local_time =0

        global_start = MPI.Wtime()

        

        for i in range(0,len(all_files)):
            data = comm.recv(source=MPI.ANY_SOURCE, tag=0)
            comm.send(all_files[i], dest=data, tag=1)

        for i in range(0,size):
            comm.send("DONE", dest=i, tag=1)
        
        
  


    else:
        all_files = None

        #all_files = comm.bcast(all_files, root=0)

        #Use message tags to distinguish between work requests, work assignments, and result submissions.
        #tag=0 is status(idle/busy); tag=1 is the log_file_name

        local_counts = defaultdict(int)
        
        while True:

            worker_state = "idle" #idle or busy

            if worker_state == "idle":
                comm.send(rank, dest=0, tag=0)
                log_file = comm.recv(source=0, tag=1)

                if log_file == "DONE":
                    break

                
                start = MPI.Wtime()
                
                counts = analyse_log_file(log_file)
                merge_counts(local_counts, counts)

                #print(log_file, rank )
                
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
        print("DYNAMIC SCHEDULING PARALLEL ANALYSIS RESULTS")
        print("="*50)
        for level in sorted(total_counts.keys()):
            print(f"{level}: {total_counts[level]}")
        print("="*50)
        print("Avg Sequential time: ", SEQ_TIME, "sec")
        #print(f"Total time (wallclock) : {max(gathered_times):.3f}s")
        print(f"Global time: {global_time:.3f}s")
        #print(f"Average rank proceSssing time: {sum(gathered_times)/len(gathered_times):.3f}s")
        print(f"Last file proceSssing time for each rank avg: {sum(gathered_times)/len(gathered_times):.3f}s")
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