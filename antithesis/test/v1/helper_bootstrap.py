#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import os
import sys
from pathlib import Path


def add_materialize_python_to_path() -> None:
    mz_root = Path(os.environ.get("MZ_ROOT", "/workdir"))
    sys.path.insert(0, str(mz_root / "misc" / "python"))
