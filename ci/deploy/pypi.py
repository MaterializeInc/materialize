# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import distutils.core  # pyright: ignore
import os
import sys
from pathlib import Path

import requests

from materialize import spawn

PACKAGE_PATHS = [
    "misc/dbt-materialize",
]


def main() -> None:
    for path_str in PACKAGE_PATHS:
        path = Path(path_str)
        distribution = distutils.core.run_setup(str(path / "setup.py"))
        name = distribution.metadata.name
        version = distribution.metadata.version
        assert name
        released_versions = get_released_versions(name)
        if version in released_versions:
            print(f"{name} {version} already released, continuing...")
            continue
        else:
            print(f"Releasing {name} {version}")
            spawn.runv(
                [sys.executable, "setup.py", "build", "sdist"],
                cwd=path,
                env={**os.environ, "RELEASE_BUILD": "1"},
            )
            dist_files = (path / "dist").iterdir()
            spawn.runv(
                ["twine", "upload", *dist_files],
                env={
                    **os.environ,
                    "TWINE_USERNAME": "__token__",
                    "TWINE_PASSWORD": os.environ["PYPI_TOKEN"],
                },
            )


def get_released_versions(name: str) -> set[str]:
    res = requests.get(f"https://pypi.org/pypi/{name}/json")
    res.raise_for_status()
    return set(res.json()["releases"])


if __name__ == "__main__":
    main()
