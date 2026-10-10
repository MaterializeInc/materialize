# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Addresses of the system under test and its dependencies."""

import os
from dataclasses import dataclass
from pathlib import Path

SQL_PORT = 6875
INTERNAL_SQL_PORT = 6877
INTERNAL_HTTP_PORT = 6878


@dataclass(frozen=True)
class Endpoints:
    namespace: str
    """Namespace of the Materialize CR and everything orchestratord creates."""
    operator_namespace: str
    environment: str
    """Name of the Materialize CR."""
    kafka_broker: str
    schema_registry_url: str
    upstream_postgres_url: str
    metadata_postgres_url: str
    state_dir: Path
    """Shared across test command invocations within one timeline."""
    environmentd_image: str | None
    """The environmentd image under test."""
    initial_environmentd_image: str | None
    """The environmentd image the environment starts on. When it differs from
    `environmentd_image`, the timeline upgrades across releases."""

    @property
    def upgrade_pending_image(self) -> str | None:
        """The image to upgrade to, if this timeline starts on an older release."""
        if (
            self.environmentd_image is not None
            and self.initial_environmentd_image is not None
            and self.initial_environmentd_image != self.environmentd_image
        ):
            return self.environmentd_image
        return None

    @classmethod
    def from_env(cls) -> "Endpoints":
        return cls(
            namespace=os.environ.get(
                "MZ_ANTITHESIS_NAMESPACE", "materialize-environment"
            ),
            operator_namespace=os.environ.get(
                "MZ_ANTITHESIS_OPERATOR_NAMESPACE", "materialize"
            ),
            environment=os.environ["MZ_ANTITHESIS_ENVIRONMENT"],
            kafka_broker=os.environ["MZ_ANTITHESIS_KAFKA_BROKER"],
            schema_registry_url=os.environ["MZ_ANTITHESIS_SCHEMA_REGISTRY_URL"],
            upstream_postgres_url=os.environ["MZ_ANTITHESIS_UPSTREAM_POSTGRES_URL"],
            metadata_postgres_url=os.environ["MZ_ANTITHESIS_METADATA_POSTGRES_URL"],
            state_dir=Path(
                os.environ.get(
                    "MZ_ANTITHESIS_STATE_DIR", "/var/lib/antithesis-workload"
                )
            ),
            environmentd_image=os.environ.get("MZ_ANTITHESIS_ENVIRONMENTD_IMAGE"),
            initial_environmentd_image=os.environ.get(
                "MZ_ANTITHESIS_INITIAL_ENVIRONMENTD_IMAGE"
            ),
        )

    def environmentd_service(self, resource_id: str) -> str:
        """DNS name of the Service that routes to the active generation."""
        return f"mz{resource_id}-environmentd.{self.namespace}.svc.cluster.local"

    def generation_service(self, resource_id: str, generation: int) -> str:
        """DNS name of the Service for one environmentd generation."""
        return f"mz{resource_id}-environmentd-{generation}.{self.namespace}.svc.cluster.local"
