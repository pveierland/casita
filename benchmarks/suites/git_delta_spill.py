"""File-backed Git delta reconstruction with bounded fixture/readback memory."""
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
        "--counts", "16" if smoke else "16,64",
        "--file-bytes", "1024,65536,1048576" if smoke else "1024,65536,1048576,16777216",
        "--backend", "local", "--layout", "packed", "--bounded-fixture",
        "--pack-window", "16", "--content", "clustered", "--delta-spilling",
        "--max-buffered-bytes", "67108864", "--decode-workers", "1,4",
        "--match-baseline-decode-workers", "--concurrency", "16", *arguments,
    ])


if __name__ == "__main__":
    raise SystemExit(main())
