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
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from materialize import MZ_ROOT, spawn
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(
        additional_system_parameter_defaults={"enable_alter_table_add_column": "true"}
    ),
    Service(name="valkey", config={"image": "valkey/valkey:9.0.6", "ports": [6379]}),
]


def workflow_default(c: Composition) -> None:
    c.up("materialized")
    with tempfile.TemporaryDirectory() as state:
        state_file = os.path.join(state, "restart.json")
        cargo_test(c, {}, "--skip", "before_a_restart", "--skip", "after_a_restart")
        python_test(c)
        node_test(c)
        valkey_test(c)
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
        ["cargo", "test", "--locked", "--tests", "--", "--ignored", *filters],
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


def python_test(c: Composition) -> None:
    """Builds the Python package into a fresh virtual environment and runs its
    tests, the live ones included."""
    package = MZ_ROOT / "misc" / "materialize-sdk" / "python"
    with tempfile.TemporaryDirectory() as venv:
        bin = Path(venv) / "bin"
        env = {
            **os.environ,
            "VIRTUAL_ENV": venv,
            "PATH": f"{bin}:{os.environ['PATH']}",
            "MATERIALIZE_URL": "postgres://materialize@localhost:{}/materialize".format(
                c.default_port("materialized")
            ),
        }
        spawn.runv([sys.executable, "-m", "venv", venv])
        spawn.runv([str(bin / "pip"), "install", "--quiet", "maturin>=1.9,<2"])
        spawn.runv(
            ["maturin", "develop", "--locked", "--quiet", "--extras", "test"],
            cwd=package,
            env=env,
        )
        spawn.runv(["python", "-m", "pytest", "--quiet", "tests"], cwd=package, env=env)


def node_test(c: Composition) -> None:
    """Builds the Node package and runs its tests, the live ones included."""
    package = MZ_ROOT / "misc" / "materialize-sdk" / "node"
    env = {
        **os.environ,
        "MATERIALIZE_URL": "postgres://materialize@localhost:{}/materialize".format(
            c.default_port("materialized")
        ),
    }
    spawn.runv(["npm", "ci", "--quiet"], cwd=package, env=env)
    spawn.runv(["npm", "run", "build"], cwd=package, env=env)
    spawn.runv(["npm", "test"], cwd=package, env=env)


def valkey_test(c: Composition) -> None:
    """Runs the Valkey example and checks that the hash follows the table, and
    that a restart of the example replaces the hash with a new snapshot."""
    sdk = MZ_ROOT / "misc" / "materialize-sdk"
    spawn.runv(["cargo", "build", "--locked", "--example", "valkey"], cwd=sdk)
    target = Path(os.environ.get("CARGO_TARGET_DIR", sdk / "target"))
    c.up("valkey")
    c.sql(
        "DROP TABLE IF EXISTS valkey_orders;"
        "CREATE TABLE valkey_orders (id int, item text, qty int);"
        "INSERT INTO valkey_orders VALUES (1, 'apple', 3), (2, 'pear', NULL);"
    )
    env = {
        **os.environ,
        "MATERIALIZE_URL": "host=localhost port={} user=materialize".format(
            c.default_port("materialized")
        ),
        "VALKEY_URL": f"redis://localhost:{c.default_port('valkey')}",
        "SOURCE": "valkey_orders",
        "KEY": "id",
        "HASH": "orders",
    }
    example = [str(target / "debug" / "examples" / "valkey")]

    sync = subprocess.Popen(example, env=env)
    try:
        wait_for_hash(
            c,
            {
                '["1"]': '{"item":"apple","qty":"3"}',
                '["2"]': '{"item":"pear","qty":null}',
            },
        )
        c.sql(
            "INSERT INTO valkey_orders VALUES (3, 'plum', 1);"
            "UPDATE valkey_orders SET qty = 4 WHERE id = 1;"
            "DELETE FROM valkey_orders WHERE id = 2;"
        )
        wait_for_hash(
            c,
            {
                '["1"]': '{"item":"apple","qty":"4"}',
                '["3"]': '{"item":"plum","qty":"1"}',
            },
        )
    finally:
        sync.kill()
        sync.wait()

    c.exec("valkey", "valkey-cli", "HSET", "orders", '["99"]', "stale")
    c.sql("INSERT INTO valkey_orders VALUES (4, 'fig', 2)")
    sync = subprocess.Popen(example, env=env)
    try:
        wait_for_hash(
            c,
            {
                '["1"]': '{"item":"apple","qty":"4"}',
                '["3"]': '{"item":"plum","qty":"1"}',
                '["4"]': '{"item":"fig","qty":"2"}',
            },
        )
    finally:
        sync.kill()
        sync.wait()


def wait_for_hash(c: Composition, expected: dict[str, str]) -> None:
    """Waits for the example's hash to equal `expected`."""
    actual: dict[str, str] = {}
    for _ in range(60):
        lines = c.exec(
            "valkey", "valkey-cli", "HGETALL", "orders", capture=True
        ).stdout.splitlines()
        actual = dict(zip(lines[::2], lines[1::2]))
        if actual == expected:
            return
        time.sleep(1)
    raise AssertionError(f"the hash is {actual}, not {expected}")
