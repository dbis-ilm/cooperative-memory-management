import argparse
import os
import parameters
import scalability
import shutil
import subprocess
import working_set
import trad_coop

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='run_experiments.py',
        description="Run all experiments and store results at the given path. Note: This should be run from the repository's base directory. Otherwise some experiments may fail due to missing relative paths.")
    parser.add_argument('path')
    args = parser.parse_args()

    if not os.path.exists(args.path):
        os.mkdir(args.path)

    # import database
    first_db_path = None
    for db_path in parameters.disks.values():
        if not os.path.exists(db_path):
            dir = os.path.dirname(db_path)
            if not os.path.exists(dir):
                os.makedirs(dir)
            if first_db_path != None:
                print(f'Copying database from {first_db_path} to {db_path} ...')
                shutil.copy(first_db_path, db_path)
            else:
                print(f'Importing database to {db_path} ...')
                # Note: We are using only a single thread for importing the database since multi-threaded CSV import got broken at some point - TODO: Fix this
                result = subprocess.run(['numactl', '-c', '0', 'build/frontend/tpcch', db_path, '--ch_path=data/tpcch/100', '--import_only', '--parallel=1'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf8')
                with open(os.path.join(args.path, 'import_stdout'), 'w') as file:
                    file.write(result.stdout)
                with open(os.path.join(args.path, 'import_stderr'), 'w') as file:
                    file.write(result.stderr)
                if result.returncode != 0:
                    print(f'Database import failed with returncode {result.returncode}, aborting ...')
                    exit(-1)
        if first_db_path == None:
            first_db_path = db_path

    # run experiments
    print('Running scalability experiments ...')
    scalability.run_experiments(args.path)
    print('Running working-set experiments ...')
    working_set.run_experiments(args.path)
    print('Running traditional-cooperative comparison experiments ...')
    trad_coop.run_experiments(args.path)
    print('Done')