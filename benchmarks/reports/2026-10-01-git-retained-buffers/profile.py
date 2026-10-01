import json, os, sys, threading, time
from pathlib import Path
from benchmarks.suites import git_retained_buffers
from benchmarks.host_activity import activity, processes
args=sys.argv[1:]
if "--spill-comparison" in args:
    args.remove("--spill-comparison")
    from benchmarks.suites import git_delta_spill
    main=git_delta_spill.main
else:
    main=git_retained_buffers.main
if "--help" in args or "-h" in args:
    sys.exit(main(args))
stop=threading.Event(); rows=[]
def sample():
    before=processes(); start=time.monotonic()
    while not stop.wait(1):
        now=time.monotonic(); after=processes()
        rows.append(dict(activity(before,after,now-start,os.getpid()), elapsed_seconds=now-start))
        before=after; start=now
thread=threading.Thread(target=sample, daemon=True); thread.start()
try:
    status=main(args)
finally:
    stop.set(); thread.join()
    if '--output' in args:
        output=Path(args[args.index('--output')+1])
        Path(str(output)+'.host.json').write_text(json.dumps(dict(samples=rows),indent=2)+'\n')
sys.exit(status)
