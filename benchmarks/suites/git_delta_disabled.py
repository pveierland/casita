"""Default-disabled Git delta control with observed deltas and import I/O."""
from __future__ import annotations
import sys
from benchmarks.suites.git_delta_spill import main as run


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    return run(["--counts", "16", "--file-bytes", "1048576", *arguments,
                "--no-delta-spilling", "--delta-metrics"])


if __name__ == "__main__":
    raise SystemExit(main())
