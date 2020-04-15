#!/usr/bin/env python3

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# activate.py — runs a script in the Materialize Python virtualenv.

from typing import List
from pathlib import Path
import logging
import os
import subprocess
import sys
import venv  # type: ignore

logger = logging.getLogger("bootstrap")


def main(args: List[str]) -> int:
    logging.basicConfig(level=os.environ.get("MZ_DEV_LOG", "WARNING").upper())
    logger.debug("args={}".format(args))

    # Validate Python version.
    if sys.hexversion < 0x03050000:
        print("fatal: python v3.5.0+ required", file=sys.stderr)
        print(" hint: you have v{}.{}.{}".format(sys.version_info.major,
                                                 sys.version_info.minor,
                                                 sys.version_info.micro),
              file=sys.stderr)
        return 1

    py_dir = Path(__file__).parent.parent
    logger.debug("py_dir={}".format(py_dir))

    # If we're not in the CI builder container, activate a virtualenv with the
    # necessary dependencies.
    if os.environ.get("MZ_DEV_CI_BUILDER", False):
        python = "python3"
    else:
        python = str(activate_venv(py_dir))
    logger.debug("python={}".format(python))

    # Reinvoke with the interpreter from the virtualenv.
    os.environ["PYTHONPATH"] = str(py_dir.resolve())
    os.environ["MZ_ROOT"] = str(py_dir.parent.parent.resolve())
    os.execvp(python, [python, *args])


def activate_venv(py_dir: Path) -> Path:
    """Bootstrap and activate a virtualenv at py_dir/venv"""
    venv_dir = py_dir / "venv"
    stamp_path = venv_dir / "dep_stamp"
    logger.debug("venv_dir={}".format(venv_dir))

    # Create a virtualenv, if necessary. virtualenv creation is not atomic, so
    # we don't want to assume the presence of a `venv` directory means that we
    # have a working virtualenv. Instead we use the presence of the
    # `stamp_path`, as that indicates the virtualenv was once working enough to
    # have dependencies installed into it.
    if not stamp_path.exists():
        print("==> Initializing virtualenv in {}".format(venv_dir))
        venv.create(str(py_dir / "venv"), with_pip=True, clear=True)

    # Check when dependencies were last installed.
    try:
        stamp_mtime = os.path.getmtime(str(stamp_path))
    except FileNotFoundError:
        stamp_mtime = 0
    logger.debug("stamp_path={} stamp_mtime={}".format(stamp_path,
                                                       stamp_mtime))

    # Check when requirements file was last modified.
    requirements_path = py_dir / "requirements.txt"
    requirements_mtime = os.path.getmtime(str(requirements_path))
    logger.debug("requirements_path={} requirements_mtime={}".format(
        requirements_path, requirements_mtime))

    # Update dependencies, if necessary.
    if stamp_mtime <= requirements_mtime:
        print("==> Updating dependencies with pip")
        subprocess.check_call([
            str(venv_dir / "bin" / "pip"), "install", "-r",
            str(requirements_path)
        ])
        stamp_path.touch()

    return venv_dir / "bin" / "python"


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
