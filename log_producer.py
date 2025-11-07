#!/usr/bin/env python3
import os, random, datetime, argparse

TEMPLATE = [
    "[{ts}] [INFO] Service started",
    "[{ts}] [DEBUG] Cache size set to {num}",
    "[{ts}] [INFO] User logged in: user{num}",
    "[{ts}] [WARN] Latency above threshold: {num}ms",
    "[{ts}] [ERROR] Operation failed: code {num}",
    "[{ts}] [WARNING] Deprecated call used",
]

def make_line():
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    tpl = random.choice(TEMPLATE)
    return tpl.format(ts=ts, num=random.randint(1,9999))

def main(outdir="sample_logs", nfiles=64, lines_per_file=100000):
    os.makedirs(outdir, exist_ok=True)
    for i in range(nfiles):
        fname = os.path.join(outdir, f"server_{i+1:03d}.log")
        with open(fname, "w") as f:
            for _ in range(lines_per_file):
                f.write(make_line() + "\n")
    print("Done:", outdir, "files=", nfiles, "lines/file=", lines_per_file)

if __name__ == "__main__":
    import sys
    nfiles = int(sys.argv[1]) if len(sys.argv)>1 else 64
    lines = int(sys.argv[2]) if len(sys.argv)>2 else 100000
    main("sample_logs", nfiles, lines)
