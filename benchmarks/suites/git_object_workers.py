"""Exercise bounded Git CPU workers around window and batch boundaries."""
from __future__ import annotations
import sys
import argparse
from benchmarks.suites.git_closure_import import main as run


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    profile, _ = parser.parse_known_args(arguments)
    sizes = "65536" if profile.profile == "smoke" else "1024,65536,4194304"
    windows = "131071,131072,131073,1048576"
    if profile.profile == "standard":
        windows += ",67108864"
    return run(["--counts", "15,16,17", "--file-bytes", sizes,
                "--max-buffered-bytes", windows,
                "--decode-workers", "1,2,4,8", "--concurrency", "16",
                "--content", "random", *arguments])


if __name__ == "__main__":
    raise SystemExit(main())
