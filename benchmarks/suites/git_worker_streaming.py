"""Mixed Git object sizes across serial, oversized, and streaming windows."""
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
        "--counts", "17" if smoke else "16,17,64",
        "--file-bytes", "65536" if smoke else "65536,4194304",
        "--max-buffered-bytes", ("65535,65536,65537,1048576" if smoke else
                                 "65535,65536,65537,4194303,4194304,4194305,67108864"),
        "--decode-workers", "1,4", "--concurrency", "16",
        "--content", "mixed", *arguments,
    ])


if __name__ == "__main__":
    raise SystemExit(main())
