# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from materialize import mzcompose


def test_bash_subst() -> None:
    step = {"step": "run", "cmd": "${EXAMPLE}"}
    one = {"mzconduct": {"workflows": {"ci": {"steps": [step]}}}}
    env = {"EXAMPLE": "foo"}
    mzcompose._substitute_env_vars(one, env)
    assert step["cmd"] == "foo"

    step = {"step": "run", "cmd": "${EXAMPLE:-bar}"}
    one = {"mzconduct": {"workflows": {"ci": {"steps": [step]}}}}
    del env["EXAMPLE"]
    mzcompose._substitute_env_vars(one, env)
    assert step["cmd"] == "bar"

    step = {"step": "run", "cmd": "${NOPE NOPE}"}
    one = {"mzconduct": {"workflows": {"ci": {"steps": [step]}}}}
    mzcompose._substitute_env_vars(one, env)
    assert step["cmd"] == "${NOPE NOPE}"


def test_bash_alt_subst() -> None:
    step = {"step": "run", "cmd": "${EXAMPLE:+baz}"}
    one = {"mzconduct": {"workflows": {"ci": {"steps": [step]}}}}
    env = {"EXAMPLE": "foo"}
    mzcompose._substitute_env_vars(one, env)
    assert step["cmd"] == "baz"

    step = {"step": "run", "cmd": "${EXAMPLE:+baz}"}
    one = {"mzconduct": {"workflows": {"ci": {"steps": [step]}}}}
    del env["EXAMPLE"]
    mzcompose._substitute_env_vars(one, env)
    assert step["cmd"] == ""

    step = {"step": "run", "cmd": "${EXAMPLE}${EXAMPLE:+baz}"}
    one = {"mzconduct": {"workflows": {"ci": {"steps": [step]}}}}
    env = {"EXAMPLE": "foo"}
    mzcompose._substitute_env_vars(one, env)
    assert step["cmd"] == "foobaz"

    step = {"step": "run", "cmd": "${EXAMPLE:-bar}${EXAMPLE:+baz}"}
    one = {"mzconduct": {"workflows": {"ci": {"steps": [step]}}}}
    del env["EXAMPLE"]
    mzcompose._substitute_env_vars(one, env)
    assert step["cmd"] == "bar"
