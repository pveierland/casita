"""Replay partitioned producer-buffer comparisons without overlapping builds or benchmarks."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cpus", default="0,1,2,3")
    parser.add_argument("--repetitions", type=int, default=5)
    args = parser.parse_args()
    assert args.repetitions > 0
    report = Path(__file__).resolve().parent
    root = report.parents[2]
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    ledger = out / "matrix-commands.json"
    assert not ledger.exists(), "use a new output directory"
    base = [sys.executable, str(report / "profile.py"), "--probe-binary", str(args.candidate.resolve()),
            "--baseline-binary", str(args.baseline.resolve()), "--no-build", "--backend", "local",
            "--cpu-affinity", args.cpus, "--counts", "16", "--content", "random",
            "--decode-workers", "4", "--imports", "4", "--shared-cpu-limit", "0", "--buffer-budget", "16777216:16777216",
            "--file-bytes", "1048577", "--repetitions", str(args.repetitions)]
    cases = [
        ("disabled-small", ["--imports", "1,4", "--decode-workers", "1", "--buffer-budget", "0:0", "--file-bytes", "1024"]),
        ("disabled-large", ["--imports", "1,4", "--buffer-budget", "0:0"]),
        ("cpu-only", ["--shared-cpu-limit", "4", "--buffer-budget", "0:0"]),
        ("buffers-only", ["--imports", "1,2,4"]),
        ("both", ["--shared-cpu-limit", "4"]),
        ("over-cpu", ["--shared-cpu-limit", "4", "--baseline-shared-cpu-limit", "4"]),
        ("same-binary", ["--baseline-binary", str(args.candidate.resolve())]),
        ("existing-controls", ["--decode-workers", "1", "--chunk-upload-concurrency", "1,4", "--max-buffered-bytes", "4194304"]),
        ("capacity", ["--buffer-budget", "16777216:11534336,33554432:33554432,67108864:67108864"]),
        ("source-boundary", ["--imports", "2", "--file-bytes", "1048575,1048576"]),
        ("tiny", ["--counts", "64", "--file-bytes", "1024", "--decode-workers", "1"]),
        ("large", ["--counts", "4", "--file-bytes", "4194304", "--decode-workers", "1"]),
        ("clustered", ["--content", "clustered", "--delta-metrics"]),
        ("memory", ["--backend", "memory"]),
    ]
    commands = [dict(name=name, command=base + options + ["--output", str(out / (name + ".json"))])
                for name, options in cases]
    ledger.write_text(json.dumps(commands, indent=2) + "\n")
    for case in commands:
        print("Running " + case["name"], flush=True)
        subprocess.run(case["command"], cwd=root, env={**os.environ, "PYTHONPATH": str(root)}, check=True)


if __name__ == "__main__":
    main()
