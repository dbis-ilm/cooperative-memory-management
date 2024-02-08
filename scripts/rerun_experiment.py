import argparse
from ExperimentConfiguration import ExperimentConfiguration
import os
import runner
import shutil

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='rerun_experiment.py',
        description="Reruns the experiment with the given result path, place results of the rerun in subdirectory 'rerun'")
    parser.add_argument('path')
    parser.add_argument('-f', '--force', action='store_true', help='Force rerun even if the experiment has already been rerun. Caution: This will override the result of the previous rerun.')
    args = parser.parse_args()

    result_path = os.path.join(args.path, 'rerun')
    if os.path.exists(result_path):
        if args.force:
            shutil.rmtree(result_path)
        else:
            print(f'Experiment was already rerun, aborting')
            exit(-1)
    config_path = os.path.join(args.path, 'config.json')
    if not os.path.exists(config_path):
        print(f"Could not find an experiment configuration at '{args.path}'")
        exit(-1)
    try:
        with open(config_path, 'r') as config_file:
            config = ExperimentConfiguration.deserialize(config_file.read())
    except:
        print(f"Failed to load experiment configuration from '{args.path}'")
        exit(-1)
    runner.run_experiment(config, result_path)