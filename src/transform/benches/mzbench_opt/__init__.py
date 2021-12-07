# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum
from pathlib import Path
from typing import List

from pkg_resources import resource_filename


def resource_path(name: str) -> Path:
    return Path(resource_filename(__name__, name))


def scenarios() -> List[str]:
    schema_files = {
        p.stem
        for p in resource_path("schema").iterdir()
        if p.is_file() and p.suffix == ".sql"
    }
    workload_files = {
        p.stem
        for p in resource_path("workload").iterdir()
        if p.is_file() and p.suffix == ".sql"
    }

    return sorted(schema_files.intersection(workload_files))


Scenario = Enum("Scenario", {scenario: scenario for scenario in scenarios()})
Scenario.__str__ = lambda self: self.value
Scenario.schema_path = lambda self: resource_path(f"schema/{self}.sql")
Scenario.workload_path = lambda self: resource_path(f"workload/{self}.sql")
