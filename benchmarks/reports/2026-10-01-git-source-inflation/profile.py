import json, os, sys, threading, time
from pathlib import Path
from benchmarks.suites.git_source_inflation import main
from benchmarks.host_activity import activity, processes
stop=threading.Event(); rows=[]
def sample():
    before=processes(); start=time.monotonic()
    while not stop.wait(1):
        now=time.monotonic(); after=processes()
        rows.append(dict(activity(before,after,now-start,os.getpid()), elapsed_seconds=now-start))
        before=after; start=now
thread=threading.Thread(target=sample, daemon=True); thread.start()
args=sys.argv[1:]
try:
    status=main(args)
finally:
    stop.set(); thread.join()
    if '--output' in args:
        output=Path(args[args.index('--output')+1])
        Path(str(output)+'.host.json').write_text(json.dumps(dict(samples=rows),indent=2)+'\n')
sys.exit(status)
