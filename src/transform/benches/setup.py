# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from setuptools import find_packages, setup

setup(
    name="mzbench-opt",
    version="0.0.1",
    description="Materialize scripts for benchmarking the transform crate",
    packages=["mzbench_opt"],
    package_data={
        "mzbench_opt": ["schema/*.sql", "workload/*.sql"],
    },
    install_requires=[
        "numpy==1.21.4",
        "psycopg2==2.9.1",
        "pandas==1.3.4",
        "typer==0.4.0",
        "sqlparse==0.4.2",
    ],
    entry_points={
        "console_scripts": [
            "mzbench-opt=mzbench_opt.cli:app",
        ],
    },
)
