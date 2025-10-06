# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import DeploymentStatus, Materialized
from materialize.mzcompose.services.postgres import CockroachOrPostgresMetadata

BASE_VERSION = "v0.140.0"

SYSTEM_PARAMETER_DEFAULTS = get_default_system_parameters()

SERVICES = [
    CockroachOrPostgresMetadata(),
    Materialized(
        name="mz_old",
        sanity_restart=False,
        image=f"materialize/materialized:{BASE_VERSION}",
        deploy_generation=0,
        system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
        external_metadata_store=True,
        default_replication_factor=2,
    ),
    Materialized(
        name="mz_new",
        sanity_restart=False,
        deploy_generation=1,
        system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
        restart="on-failure",
        external_metadata_store=True,
        default_replication_factor=2,
    ),
]


def workflow_default(c: Composition):
    c.down(destroy_volumes=True)

    c.up("mz_old")
    c.sql("SELECT * FROM mz_tables LIMIT 1", service="mz_old")

    c.up("mz_new")
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
    c.promote_mz("mz_new")
    c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")

    c.sql("SELECT * FROM mz_tables LIMIT 1", service="mz_new")
