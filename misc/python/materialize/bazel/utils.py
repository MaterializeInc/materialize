# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pathlib
import subprocess

"""Utilities for interacting with Bazel from python scripts"""


def output_paths(target, options=[]) -> list[pathlib.Path]:
    """Returns the absolute path of outputs from the built Bazel target."""

    cmd_args = ["bazel", "cquery", f"{target}", *options, "--output=files"]
    paths = subprocess.check_output(
        cmd_args, text=True, stderr=subprocess.DEVNULL
    ).splitlines()
    return [pathlib.Path(path) for path in paths]
