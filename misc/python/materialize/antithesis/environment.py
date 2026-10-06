# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The Materialize environment under test, as seen from the workload pod."""

from __future__ import annotations

from functools import cached_property

from materialize import orchestratord
from materialize.antithesis.endpoints import Endpoints


class Environment:
    def __init__(self, endpoints: Endpoints | None = None) -> None:
        self.endpoints = endpoints or Endpoints.from_env()
        orchestratord.load_config()

    @cached_property
    def materialize(self) -> orchestratord.Materialize:
        return orchestratord.Materialize(
            self.endpoints.namespace, self.endpoints.environment
        )

    def sql_host(self) -> str:
        """Host of the Service that routes to the active generation.

        The resource id is fixed for the life of the CR, so it is cached in the
        state directory to spare every command a Kubernetes API round trip.
        """
        cache = self.endpoints.state_dir / "resource-id"
        if cache.exists():
            resource_id = cache.read_text().strip()
        else:
            resource_id = self.materialize.resource_id()
            if resource_id is None:
                raise RuntimeError("Materialize CR has no resource id yet")
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(resource_id)
        return self.endpoints.environmentd_service(resource_id)
