# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
End-to-end tests of the Materialize SDK against Materialize: the integration
tests of each SDK package that need a running server.
"""

import os
import tempfile

from materialize import MZ_ROOT, spawn
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(
        additional_system_parameter_defaults={"enable_alter_table_add_column": "true"}
    )
]


def workflow_default(c: Composition) -> None:
    c.up("materialized")
    with tempfile.TemporaryDirectory() as state:
        state_file = os.path.join(state, "restart.json")
        cargo_test(c, {}, "--skip", "before_a_restart", "--skip", "after_a_restart")
        phase = {"MATERIALIZE_SDK_RESTART_STATE": state_file}
        cargo_test(
            c,
            {**phase, "MATERIALIZE_SDK_RESTART_PHASE": "before"},
            "--exact",
            "before_a_restart",
        )
        # A filter that matches no test passes, so check that each phase ran.
        assert os.path.exists(state_file), "the first restart phase did not run"
        c.kill("materialized")
        c.up("materialized")
        cargo_test(
            c,
            {**phase, "MATERIALIZE_SDK_RESTART_PHASE": "after"},
            "--exact",
            "after_a_restart",
        )
        assert not os.path.exists(state_file), "the second restart phase did not run"


def cargo_test(c: Composition, env: dict[str, str], *filters: str) -> None:
    """Runs the ignored tests that match `filters`. The ports are looked up on
    each run, because a restart can change them."""
    url = "host=localhost port={} user={} dbname=materialize"
    spawn.runv(
        # Only the Rust package's: the turbopuffer sink's ignored tests need
        # an API key, and run by hand.
        [
            "cargo",
            "test",
            "--locked",
            "-p",
            "materialize-sdk",
            "--tests",
            "--",
            "--ignored",
            *filters,
        ],
        cwd=MZ_ROOT / "misc" / "materialize-sdk",
        env={
            **os.environ,
            "MATERIALIZE_URL": url.format(
                c.default_port("materialized"), "materialize"
            ),
            "MATERIALIZE_SYSTEM_URL": url.format(
                c.port("materialized", 6877), "mz_system"
            ),
            **env,
        },
    )
