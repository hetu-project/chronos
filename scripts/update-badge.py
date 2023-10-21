from subprocess import run, PIPE
from json import loads
from datetime import datetime

p = run(['tokei', '--exclude=assets', '--output=json'], stdout=PIPE, check=True)
sloc = loads(p.stdout)['Total']['code']
p = run(['git', 'branch', '--show-current'], stdout=PIPE, check=True)
branch = p.stdout.decode().strip()
assert branch != 'asset-badge'
run(['git', 'checkout', 'asset-badge'], check=True)
run(['curl', f'https://img.shields.io/badge/total_lines-{sloc / 1000:.1f}k-blue', '-o', 'sloc.svg'], check=True)
run(['git', 'add', 'sloc.svg'], check=True)
run(['git', 'commit', '-m', str(datetime.now())])  # may fail if badge is identical
run(['git', 'push', 'origin', 'asset-badge'], check=True)
run(['git', 'checkout', branch], check=True)