# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from os import environ
from typing import List
from unittest.mock import patch

from semver import Version, VersionInfo

from materialize.git import get_version_tags
from materialize.mzcompose import (
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
    Testdrive,
    Workflow,
    Zookeeper,
)

#
# Determine the list of versions to be tested
#

min_tested_tag = VersionInfo.parse("0.8.0")
all_tags = get_version_tags(fetch=False)
all_tested_tags = []

all_tested_tags = sorted(
    [tag for tag in all_tags if tag >= min_tested_tag and tag.prerelease is None]
)

#
# Construct Materialized service objects for all participating versions
#

mz_versioned = dict(
    (
        tag,
        Materialized(
            name=f"materialized_v{tag}",
            image=f"materialize/materialized:v{tag}",
            hostname="materialized",
        ),
    )
    for tag in all_tested_tags
)

mz_versioned["current_source"] = Materialized(
    name="materialized_current_source", hostname="materialized"
)

prerequisites = [Zookeeper(), Kafka(), SchemaRegistry(), Postgres()]

services = [*prerequisites, *(mz_versioned.values()), Testdrive()]


def workflow_upgrade(w: Workflow):
    w.start_and_wait_for_tcp(services=prerequisites)

    for tag in all_tested_tags:
        priors = [f"v{prior_tag}" for prior_tag in all_tags if prior_tag < tag]
        test_upgrade_from_version(w, f"v{tag}", priors)

    test_upgrade_from_version(w, "current_source", ["*"])


def test_upgrade_from_version(w: Workflow, from_version: str, priors: List[str]):
    print(f"===>>> Testing upgrade from Materialize {from_version} to current_source.")

    glob = "|".join(["any_version", *priors, from_version])
    print(">>> Glob pattern: " + glob)

    mz_from = f"materialized_{from_version}"
    w.start_services(services=[mz_from])
    w.wait_for_mz(service=mz_from)

    temp_dir = f"--temp-dir=/share/tmp/upgrade-from-{from_version}"
    with patch.dict(environ, {"UPGRADE_FROM_VERSION": from_version}):
        w.run_service(
            service="testdrive-svc",
            command=f"--seed=1 --no-reset {temp_dir} create-in-@({glob})*.td",
        )

    w.kill_services(services=[mz_from])
    w.remove_services(services=[mz_from, "testdrive-svc"])

    mz_to = "materialized_current_source"
    w.start_services(services=[mz_to])
    w.wait_for_mz(service=mz_to)

    with patch.dict(environ, {"UPGRADE_FROM_VERSION": from_version}):
        w.run_service(
            service="testdrive-svc",
            command=f"--seed=1 --no-reset {temp_dir} check-from-@({glob})*.td",
        )

    w.kill_services(services=[mz_to])
    w.remove_services(services=[mz_to, "testdrive-svc"])
    w.remove_volumes(volumes=["mzdata", "tmp"])
