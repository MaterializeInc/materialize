#!/usr/bin/env python3

import argparse
import csv
import itertools
import multiprocessing
import os
import subprocess
import sys

def rev_parse(git_ref):
    if not git_ref:
        return git_ref
    return subprocess.check_output(['git', 'rev-parse', git_ref]).strip().decode()

def main(composition, worker_counts, git_revisions):

    if multiprocessing.cpu_count() > 8:
        benchmark = 'benchmark'
    elif multiprocessing.cpu_count() > 4:
        benchmark = 'benchmark-medium'
    else:
        benchmark = 'benchmark-ci'

    setup_benchmark = ['./bin/mzcompose', '--mz-find', composition, 'run', f'setup-{benchmark}']
    run_benchmark = ['./bin/mzcompose', '--mz-find', composition, 'run', f'run-{benchmark}']

    field_names = ["git_revision", "num_workers", "seconds_taken", "rows_per_second", "grafana_url"]
    results_writer = csv.DictWriter(sys.stdout, field_names)
    results_writer.writeheader()

    # We use check_output because check_call does not capture output
    subprocess.check_output(setup_benchmark, stderr=subprocess.STDOUT)

    for (worker_count, git_revision) in itertools.product(worker_counts, git_revisions):

        child_env = os.environ.copy()
        child_env['MZ_WORKERS'] = str(worker_count)
        child_env['MZ_QUIET'] = "true"
        if git_revision:
            child_env['MZBUILD_MATERIALIZED_TAG'] = f'unstable-{git_revision}'

        output = subprocess.check_output(run_benchmark, env=child_env, stderr=subprocess.STDOUT)

        # TODO: Replace parsing output from mzcompose with reading from a well known file or topic
        for line in output.decode().splitlines():
            if line.startswith("SUCCESS!"):
                for token in line.split(' '):
                    if token.startswith('seconds_taken='):
                        seconds_taken = token[len('seconds_taken=')]
                    elif token.startswith('rows_per_sec='):
                        rows_per_second = token[len('rows_per_sec=')]
            elif line.startswith("Grafana URL: "):
                grafana_url = line[len("Grafana URL: "):]

        results_writer.writerow({'git_revision': git_revision if git_revision else 'NONE',
                                  'num_workers': worker_count,
                                  'seconds_taken': seconds_taken,
                                  'rows_per_second': rows_per_second,
                                  'grafana_url': grafana_url})

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("composition",
                        type=str,
                        help="Name of the mzcompose composition to run",
                       )

    worker_counts = [24, 20, 16, 15, 12, 10, 8, 4, 2, 1]
    git_revisions = [rev_parse(ref) for ref in ['origin/main', None]]

    args = parser.parse_args()
    main(args.composition, worker_counts, git_revisions)
