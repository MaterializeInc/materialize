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
    url = (
        f"host=localhost port={c.default_port('materialized')} "
        "user=materialize dbname=materialize"
    )
    spawn.runv(
        ["cargo", "test", "--locked", "--", "--ignored"],
        cwd=MZ_ROOT / "misc" / "materialize-sdk",
        env={**os.environ, "MATERIALIZE_URL": url},
    )
