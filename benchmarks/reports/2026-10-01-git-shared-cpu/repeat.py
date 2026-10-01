"""Repeat the disabled tiny control and the concurrent memory result."""
import argparse,json,os,subprocess,sys
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--baseline',type=Path,required=True)
p.add_argument('--candidate',type=Path,required=True)
p.add_argument('--output-dir',type=Path,required=True)
p.add_argument('--cpus',default='0,1,2,3')
a=p.parse_args()
report=Path(__file__).resolve().parent
root=report.parents[2]
out=a.output_dir.resolve(); out.mkdir(exist_ok=True,parents=True)
ledger=out/'supplemental-commands.json'; assert not ledger.exists()
base=[sys.executable,str(report/'profile.py'),'--probe-binary',str(a.candidate.resolve()),'--baseline-binary',str(a.baseline.resolve()),'--no-build','--backend','local','--cpu-affinity',a.cpus,'--counts','16','--content','random','--decode-workers','4','--imports','4','--shared-cpu-limit','4','--file-bytes','1048577','--repetitions','5']
cases=[('disabled-tiny-repeat',['--decode-workers','1','--imports','1','--shared-cpu-limit','0','--file-bytes','1024']),('shared-four-repeat',[]),('shared-serial-control',['--decode-workers','1']),('shared-four-same-binary',['--baseline-binary',str(a.candidate.resolve())])]
commands=[dict(name=n,command=base+opts+['--output',str(out/(n+'.json'))]) for n,opts in cases]
ledger.write_text(json.dumps(commands,indent=2)+'\n')
for case in commands:
 print('Running '+case['name'],flush=True)
 subprocess.run(case['command'],cwd=root,env={**os.environ,'PYTHONPATH':str(root)},check=True)
