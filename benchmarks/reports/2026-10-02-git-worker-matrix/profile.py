import hashlib, json, os, sys, threading, time
from pathlib import Path
from benchmarks.suites import git_worker_matrix
from benchmarks.host_activity import activity, processes
args=sys.argv[1:]
main=git_worker_matrix.main
if "--help" in args or "-h" in args:
    sys.exit(main(args))
stop=threading.Event(); rows=[]
def sample():
    before=processes(); start=time.monotonic()
    while not stop.wait(1):
        now=time.monotonic(); after=processes()
        row = dict(activity(before,after,now-start,os.getpid()), elapsed_seconds=now-start)
        # Retain identities and CPU counts without unrelated application names.
        for field in ('top_external_processes', 'competing_processes', 'paused_build_processes'):
            for process in row[field]:
                process['name'] = 'process-' + hashlib.sha256(process['name'].encode()).hexdigest()[:12]
        rows.append(row)
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
