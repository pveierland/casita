"""Incremental source inflation with bounded fixtures and pre-audit parent RSS."""
from __future__ import annotations
import argparse
import sys
from benchmarks.suites.git_closure_import import main as run


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    profile, _ = parser.parse_known_args(arguments)
    smoke = profile.profile == "smoke"
    return run([
        "--counts", "1" if smoke else "1,4",
        "--file-bytes", "1048575,1048576,1048577" if smoke else "1048575,1048576,1048577,16777216,67108864",
        "--backend", "local", "--layout", "loose", "--bounded-fixture", "--pack-window", "0",
        "--max-buffered-bytes", "16777216" if smoke else "1048575,1048576,1048577,67108864",
        "--decode-workers", "1,4", "--match-baseline-decode-workers", "--concurrency", "16",
        "--content", "random", *arguments,
    ])


if __name__ == "__main__":
    raise SystemExit(main())
