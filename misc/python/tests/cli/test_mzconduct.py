# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from materialize.cli import mzconduct


def test_bash_subst() -> None:
    step = {"step": "run", "cmd": "${EXAMPLE}"}
    one = {"mzconduct": {"workflows": {"ci": {"steps": [step]}}}}
    os.environ["EXAMPLE"] = "foo"
    mzconduct._substitute_env_vars(one)
    assert step["cmd"] == "foo"

    step = {"step": "run", "cmd": "${EXAMPLE:-bar}"}
    one = {"mzconduct": {"workflows": {"ci": {"steps": [step]}}}}
    del os.environ["EXAMPLE"]
    mzconduct._substitute_env_vars(one)
    assert step["cmd"] == "bar"

    step = {"step": "run", "cmd": "${NOPE NOPE}"}
    one = {"mzconduct": {"workflows": {"ci": {"steps": [step]}}}}
    mzconduct._substitute_env_vars(one)
    assert step["cmd"] == "${NOPE NOPE}"
