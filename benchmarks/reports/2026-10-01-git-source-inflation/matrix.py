import argparse, os, subprocess, sys, json
from pathlib import Path
from benchmarks.host_activity import QuietHost
from benchmarks.suites.repository import BenchmarkError
parser=argparse.ArgumentParser()
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--candidate', type=Path, required=True)
parser.add_argument('--output-dir', type=Path, required=True)
args=parser.parse_args()
root=Path(__file__).resolve().parents[3]
out=args.output_dir.resolve()
out.mkdir(parents=True, exist_ok=False)
print('Waiting up to five minutes for a quiet interval before the comparison matrix.', flush=True)
try:
    with QuietHost(timeout=300, quiet_seconds=5, max_cpu_fraction=0.10) as quiet:
        pass
    (out/'matrix-start-host.json').write_text(json.dumps(quiet.report(),indent=2)+'\n')
except BenchmarkError as error:
    print(str(error)+'; memory comparisons will still run, with CPU timing marked exploratory.',flush=True)
    (out/'matrix-start-host.json').write_text(json.dumps(dict(error=str(error)),indent=2)+'\n')
base=[sys.executable,str(Path(__file__).with_name('profile.py')), '--probe-binary',str(args.candidate.resolve()),
      '--baseline-binary',str(args.baseline.resolve()),'--no-build',
      '--backend','local','--repetitions','5','--cpu-affinity','0,1,2,3']
cases=[
 ('thresholds',['--counts','4','--file-bytes','1048575,1048576,1048577','--max-buffered-bytes','16777216','--decode-workers','1,4','--layout','both']),
 ('large-64m',['--counts','1','--file-bytes','67108864','--max-buffered-bytes','67108864','--decode-workers','1','--layout','both']),
 ('parallel-16m',['--counts','4','--file-bytes','16777216','--max-buffered-bytes','67108864','--decode-workers','1,4','--layout','both']),
 ('small-control',['--counts','16','--file-bytes','65536','--max-buffered-bytes','16777216','--decode-workers','1,4','--layout','both']),
 ('delta-fallback',['--counts','16','--file-bytes','1048576','--max-buffered-bytes','16777216','--decode-workers','1,4','--layout','packed','--content','clustered','--pack-window','16']),
 ('compressed-64m',['--counts','1','--file-bytes','67108864','--max-buffered-bytes','67108864','--decode-workers','1','--layout','both','--content','repeated']),
 ('byte-budget',['--counts','4','--file-bytes','1048576','--max-buffered-bytes','1048575,1048576,1048577','--decode-workers','1,4','--layout','loose']),
]
commands=[]
for name,args in cases:
    command=base+args+['--output',str(out/(name+'.json'))]
    commands.append(dict(name=name,command=command))
(out/'matrix-commands.json').write_text(json.dumps(commands,indent=2)+'\n')
for case in commands:
    print('Running '+case['name'],flush=True)
    subprocess.run(case['command'],cwd=root,env={**os.environ,'PYTHONPATH':str(root)},check=True)
    print('Completed '+case['name'],flush=True)
