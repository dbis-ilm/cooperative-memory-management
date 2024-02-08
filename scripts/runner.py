import os
import subprocess
from ExperimentConfiguration import ExperimentConfiguration

# note: all code below assumes that the current working directory is the repository's base directory

# performs a single experiment run and places all output data into the specified 'output_path' directory
def run_experiment(config: ExperimentConfiguration, output_path: str):
    if os.path.exists(output_path):
        raise RuntimeError('output_path already exists!')
    os.mkdir(output_path)
    cmd = config.to_commandline(output_path)
    with open(os.path.join(output_path, 'config.json'), 'w') as file:
        file.write(config.serialize())
        file.write('\n')
    with open(os.path.join(output_path, 'command'), 'w') as file:
        file.write(cmd)
        file.write('\n')
    # run experiment
    print(f'$ {cmd}')
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf8')
    # save results
    with open(os.path.join(output_path, 'returncode'), 'w') as file:
        file.write(f'{result.returncode}\n')
    with open(os.path.join(output_path, 'stdout'), 'w') as file:
        file.write(result.stdout)
    with open(os.path.join(output_path, 'stderr'), 'w') as file:
        file.write(result.stderr)