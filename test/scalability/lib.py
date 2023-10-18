# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

import pandas as pd
from matplotlib import pyplot as plt  # type: ignore

from materialize import MZ_ROOT, git

RESULTS_DIR = MZ_ROOT / "test" / "scalability" / "results"


def plotit(csv_file_name: str) -> None:
    targets = next(os.walk(RESULTS_DIR))[1]
    legend = []
    plt.rcParams["figure.figsize"] = (16, 10)
    fig, (summary_subplot, details_subplot) = plt.subplots(2, 1)
    for i, target in enumerate(targets):
        target_sha = target.split(" ")[1].strip("()")

        # empty when mz_version() reports a Git SHA that is not available in the current repository
        target_comment = git.get_commit_message(target_sha)

        legend.append(f"{target} - {target_comment}")

        df = pd.read_csv(RESULTS_DIR / target / f"{csv_file_name}.csv")
        summary_subplot.scatter(df["concurrency"], df["tps"], label="tps")

        df_details = pd.read_csv(RESULTS_DIR / target / f"{csv_file_name}_details.csv")
        details_subplot.scatter(
            df_details["concurrency"] + i, df_details["wallclock"], alpha=0.25
        )

    summary_subplot.set_ylabel("Transactions Per Second")
    summary_subplot.set_xlabel("Concurrent SQL Connections")
    summary_subplot.legend(legend)

    details_subplot.set_ylabel("Latency in Seconds")
    details_subplot.set_xlabel("Concurrent SQL Connections")
    details_subplot.legend(legend)
