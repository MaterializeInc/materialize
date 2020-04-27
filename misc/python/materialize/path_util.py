# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import contextlib
import pathlib
from typing import Union


@contextlib.contextmanager
def cd(path: Union[pathlib.Path, str]) -> None:
    """Temporarily cd into a directory
    """
    odir = os.getcwdb()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(odir)
