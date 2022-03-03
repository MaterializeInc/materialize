# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path
from typing import List

from pkg_resources import resource_filename


def resource_path(name: str) -> Path:
    return Path(resource_filename(__name__, name))


def scenarios() -> List[str]:
    """
    Determines a list of avilable scenarios based on the intersection
    of files located in both the `schema` and `workload` resource paths.
    """
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


class Scenario:
    def __init__(self, value: str) -> None:
        self.value = value

    def schema_path(self) -> Path:
        return resource_path(f"schema/{self}.sql")

    def workload_path(self) -> Path:
        return resource_path(f"workload/{self}.sql")

    def __str__(self) -> str:
        return self.value
