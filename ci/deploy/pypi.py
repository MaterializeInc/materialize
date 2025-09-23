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
import tarfile
import tempfile
from email.parser import Parser
from pathlib import Path
from typing import Literal

import requests

from materialize import spawn

PACKAGE_PATHS: dict[str, Literal["setup.py", "pyproject.toml"]] = {
    "misc/dbt-materialize": "setup.py",
    "misc/mcp-materialize": "pyproject.toml",
    "misc/mcp-materialize-agents": "pyproject.toml",
}


def main() -> None:
    for path_str, build_type in PACKAGE_PATHS.items():
        path = Path(path_str)

        if build_type == "setup.py":
            name, version = get_metadata_from_setup_py(path)
            build_package_setup_py(path)
        elif build_type == "pyproject.toml":
            name, version = get_metadata_from_pyproject(path)
            build_package_pyproject(path)
        else:
            raise ValueError(f"Unknown build type: {build_type}")

        assert name and version
        released_versions = get_released_versions(name)
        if version in released_versions:
            print(f"{name} {version} already released, continuing...")
            continue

        print(f"Releasing {name} {version}")
        upload_to_pypi(path)


def get_metadata_from_setup_py(path: Path) -> tuple[str, str]:
    distribution = distutils.core.run_setup(str(path / "setup.py"))
    return distribution.metadata.name, distribution.metadata.version


def get_metadata_from_pyproject(path: Path) -> tuple[str, str]:
    import build

    with tempfile.TemporaryDirectory() as out_dir:
        builder = build.ProjectBuilder(path)
        sdist_path = Path(builder.build("sdist", out_dir))

        with tarfile.open(sdist_path) as tar:
            pkg_info = next(m for m in tar.getmembers() if m.name.endswith("PKG-INFO"))
            f = tar.extractfile(pkg_info)
            if not f:
                raise RuntimeError("Failed to extract PKG-INFO")
            metadata = Parser().parsestr(f.read().decode())

        return metadata["Name"], metadata["Version"]


def build_package_setup_py(path: Path) -> None:
    spawn.runv([sys.executable, "setup.py", "build", "sdist"], cwd=path)


def build_package_pyproject(path: Path) -> None:
    spawn.runv([sys.executable, "-m", "build"], cwd=path)


def upload_to_pypi(path: Path) -> None:
    dist_files = list((path / "dist").iterdir())
    spawn.runv(
        ["twine", "upload", *dist_files],
        env={
            **os.environ,
            "TWINE_USERNAME": "__token__",
            "TWINE_PASSWORD": os.environ["PYPI_TOKEN"],
        },
    )


def get_released_versions(name: str) -> set[str]:
    # Default user client currently fails:
    # > python-requests/2.32.3 User-Agents are currently blocked from accessing
    # > JSON release resources. A cluster is apparently crawling all
    # > project/release resources resulting in excess cache misses. Please
    # > contact admin@pypi.org if you have information regarding what this
    # > software may be.
    res = requests.get(
        f"https://pypi.org/pypi/{name}/json",
        headers={"User-Agent": "Materialize Version Check"},
    )
    if res.status_code == 404:
        # First release, no versions exist yet
        return set()
    res.raise_for_status()
    return set(res.json()["releases"])


if __name__ == "__main__":
    main()
