# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Minio(setup_materialize=True),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(external_minio=True, catalog_store="stash"),
    Testdrive(no_reset=True, external_minio=True, consistent_seed=True),
]

PRE_INCIDENT_MZ = Materialized(
    image="materialize/materialized:v0.68.0",
    external_minio=True,
    catalog_store="stash",
)

REJECT_LEGACY_MZ = Materialized(
    external_minio=True,
    environment_extra=["FAILPOINTS=reject_legacy_upsert_errors=panic"],
    catalog_store="stash",
)


def workflow_default(c: Composition) -> None:
    """This test can no longer run as it requires the use of v0.68.0
    Upgrades from v0.68.0 to HEAD are no longer supported.
    """
    c.up("zookeeper", "kafka", "schema-registry")

    with c.override(PRE_INCIDENT_MZ):
        c.up("materialized")
        c.run("testdrive", "produce-error.td")
        c.kill("materialized")

    # Run the new version that will produce the legacy error retractions to
    # heal the shards.
    c.up("materialized")
    c.run("testdrive", "verify-heal-in-memory.td")
    c.kill("materialized")

    # Now run a version of Materialize that panics if it ever encounters a
    # legacy error to ensure that the legacy errors are gone from both
    # in-memory traces and the on-disk blobs.
    with c.override(REJECT_LEGACY_MZ):
        c.up("materialized")
        c.run("testdrive", "verify-heal-on-disk.td")
        c.kill("materialized")
