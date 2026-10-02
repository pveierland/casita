"""Partitioned producer-buffer admission with independent shared CPU controls."""
from __future__ import annotations
import argparse
import sys
from benchmarks.suites.git_closure_import import main as run


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    options, _ = parser.parse_known_args(arguments)
    smoke = options.profile == "smoke"
    return run([
        "--counts", "4" if smoke else "16,64",
        "--file-bytes", "1024,1048576" if smoke else "1024,1048575,1048576,1048577,4194304",
        "--imports", "1,2" if smoke else "1,2,4",
        "--buffer-budget", "0:0,16777216:16777216" if smoke else "0:0,16777216:16777216,33554432:33554432,67108864:67108864,140509183:119537663,140509184:119537664,140509185:119537665",
        "--shared-cpu-limit", "1" if smoke else "0,4",
        "--backend", "local", "--layout", "packed", "--content", "mixed",
        "--max-buffered-bytes", "67108864", "--decode-workers", "1,4",
        "--match-baseline-decode-workers", "--bounded-fixture", "--cpu-metrics",
        *arguments,
    ])


if __name__ == "__main__":
    raise SystemExit(main())
