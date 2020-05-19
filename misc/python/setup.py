# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path
from typing import List

from setuptools import setup, find_packages  # type: ignore

# stub setup.py that allows running `pip install -e .` to install into a virtualenv

HERE = Path(__file__).parent


def requires(fname: str) -> List[str]:
    return [l for l in HERE.joinpath(fname).open().read().splitlines() if l]


setup(
    name="materialize",
    packages=find_packages(),
    install_requires=requires("requirements.txt"),
    extras_require={
        "dev": requires("requirements-dev.txt"),
        "ci": requires("requirements-ci.txt"),
    },
    package_data={"materialize": ["py.typed"]},
    include_package_data=True,
)
