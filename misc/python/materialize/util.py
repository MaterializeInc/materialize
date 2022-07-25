# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Various utilities"""

import os
import random
from pathlib import Path
from typing import List

from semver import Version

ROOT = Path(os.environ["MZ_ROOT"])


def nonce(digits: int) -> str:
    return "".join(random.choice("0123456789abcdef") for _ in range(digits))


def known_materialize_versions() -> List[Version]:
    """Returns all known Materialize versions.

    The list is returned in version order with newest versions first.
    """
    files = Path(ROOT / "doc" / "user" / "content" / "releases").glob("*.md")
    return [Version.parse(f.stem.lstrip("v")) for f in files if f.stem.startswith("v")]
