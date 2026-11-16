# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import os
import shutil
import sys
import tarfile
import tempfile
from email.parser import Parser
from pathlib import Path
from typing import Literal

import distutils.core  # pyright: ignore
import requests

from materialize import spawn

PACKAGE_PATHS: dict[str, Literal["setup.py", "pyproject.toml"]] = {
    "misc/dbt-materialize": "setup.py",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        "pypi.py", description="build PyPI packages that are not yet released"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("pypi-dist"),
        help="Where to collect the distributions to publish",
    )
    args = parser.parse_args()

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

        print(f"Staging {name} {version} for release")
        stage_dists(path, args.out_dir)


def get_metadata_from_setup_py(path: Path) -> tuple[str, str]:
    distribution = distutils.core.run_setup(str(path / "setup.py"))
    name = distribution.metadata.name
    version = distribution.metadata.version
    assert name is not None, f"missing name in {path}/setup.py"
    assert version is not None, f"missing version in {path}/setup.py"
    return name, version


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
    spawn.runv(
        [sys.executable, "setup.py", "build", "sdist"],
        cwd=path,
        env={**os.environ, "RELEASE_BUILD": "1"},
    )


def build_package_pyproject(path: Path) -> None:
    spawn.runv([sys.executable, "-m", "build"], cwd=path)


def stage_dists(path: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for dist_file in (path / "dist").iterdir():
        shutil.copy(dist_file, out_dir)


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
