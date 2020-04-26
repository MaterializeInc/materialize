# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Utilities for interacting with humans
"""
import sys


def warn(msg: str, *fmts: str) -> None:
    """Print a warning
    """
    print("WARNING: {}".format(msg.format(*fmts)), file=sys.stderr)
