# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from typing import List
from unittest.mock import patch

from semver import Version, VersionInfo

from materialize import errors
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

min_tested_tag = VersionInfo.parse(os.getenv("MIN_TESTED_TAG", "0.8.0"))

all_tags = [tag for tag in get_version_tags(fetch=False) if tag.prerelease is None]
if not all_tags:
    raise error.MzRuntimeError(
        "No tags found in current repository. Please run git fetch --all to obtain tags."
    )

all_tested_tags = sorted([tag for tag in all_tags if tag >= min_tested_tag])

# The Mz options that are valid only at or above a certain version
mz_options = {VersionInfo.parse("0.9.2"): "--persistent-user-tables"}

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
            options="".join(
                option
                for starting_version, option in mz_options.items()
                if tag >= starting_version
            ),
        ),
    )
    for tag in all_tested_tags
)

mz_versioned["current_source"] = Materialized(
    name="materialized_current_source",
    hostname="materialized",
    options="".join(mz_options.values()),
)

prerequisites = [Zookeeper(), Kafka(), SchemaRegistry(), Postgres()]

TESTDRIVE_VALIDATE_CATALOG_SVC_NAME = "testdrive-svc"
TESTDRIVE_NOVALIDATE_CATALOG_SVC_NAME = "testdrive-svc-novalidate"

services = [
    *prerequisites,
    *(mz_versioned.values()),
    Testdrive(name=TESTDRIVE_VALIDATE_CATALOG_SVC_NAME),
    # N.B.: we need to use `validate_catalog=False` because testdrive uses HEAD version to read
    # from disk to compare to the in-memory representation.  Doing this invokes SQL planning.
    # However, the SQL planner cannot be guaranteed to handle all old syntaxes directly. For
    # example, HEAD will panic when attempting to plan the <= v0.9.12 representation of protobufs
    # because it expects them to be either be:
    #   1) purified (input from the console)
    #   2) upgraded (reading from old catalog)
    #
    # Therefore, we use this version when running against instances of materailized that have not
    # been upgraded.
    #
    # Disabling catalog validation is preferable to using a versioned testdrive because that would
    # involve maintaining backwards compatibility for all testdrive commands.
    Testdrive(name=TESTDRIVE_NOVALIDATE_CATALOG_SVC_NAME, validate_catalog=False),
]


def workflow_upgrade(w: Workflow):
    w.start_and_wait_for_tcp(services=prerequisites)

    for tag in all_tested_tags:
        priors = [f"v{prior_tag}" for prior_tag in all_tags if prior_tag < tag]
        test_upgrade_from_version(w, f"v{tag}", priors)

    test_upgrade_from_version(w, "current_source", ["*"])


def test_upgrade_from_version(w: Workflow, from_version: str, priors: List[str]):
    print(f"===>>> Testing upgrade from Materialize {from_version} to current_source.")

    version_glob = "|".join(["any_version", *priors, from_version])
    print(">>> Version glob pattern: " + version_glob)
    td_glob = os.getenv("TD_GLOB", "*")

    mz_from = f"materialized_{from_version}"
    w.start_services(services=[mz_from])
    w.wait_for_mz(service=mz_from)

    temp_dir = f"--temp-dir=/share/tmp/upgrade-from-{from_version}"
    with patch.dict(os.environ, {"UPGRADE_FROM_VERSION": from_version}):
        w.run_service(
            service=TESTDRIVE_NOVALIDATE_CATALOG_SVC_NAME,
            command=f"--seed=1 --no-reset {temp_dir} create-in-@({version_glob})-{td_glob}.td",
        )

    w.kill_services(services=[mz_from])
    w.remove_services(services=[mz_from, TESTDRIVE_NOVALIDATE_CATALOG_SVC_NAME])

    mz_to = "materialized_current_source"
    w.start_services(services=[mz_to])
    w.wait_for_mz(service=mz_to)

    with patch.dict(os.environ, {"UPGRADE_FROM_VERSION": from_version}):
        w.run_service(
            service=TESTDRIVE_VALIDATE_CATALOG_SVC_NAME,
            command=f"--seed=1 --no-reset {temp_dir} check-from-@({version_glob})-{td_glob}.td",
        )

    w.kill_services(services=[mz_to])
    w.remove_services(services=[mz_to, TESTDRIVE_VALIDATE_CATALOG_SVC_NAME])
    w.remove_volumes(volumes=["mzdata", "tmp"])
