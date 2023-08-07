# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import uuid

import pytest
from frontegg_api import FronteggClient, FronteggTenant

from materialize.cloudtest.config.environment_config import EnvironmentConfig


def create_frontegg_client(pytestconfig: pytest.Config):
    if (
        pytestconfig.option.frontegg_vendor_client_id is not None
        and pytestconfig.option.frontegg_vendor_secret_key is not None
    ):
        client_id = pytestconfig.option.frontegg_vendor_client_id
        secret_key = pytestconfig.option.frontegg_vendor_secret_key
    else:
        client_id = os.environ["FRONTEGG_CLIENT_ID"]
        secret_key = os.environ["FRONTEGG_SECRET_KEY"]

    return FronteggClient(client_id, secret_key)


def load_frontegg_tenant(
    config: EnvironmentConfig,
    frontegg_client: FronteggClient,
) -> FronteggTenant:
    tenants = frontegg_client.list_tenants(uuid.UUID(config.auth.organization_id))
    assert len(tenants) == 1
    return tenants[0]


def update_tenant(
    frontegg_client: FronteggClient,
    tenant: FronteggTenant,
):
    # Ensure the tenant is *unblocked* before it's yielded to a test. This
    # ensures any leftover state from a previous test isn't passed on.
    tenant.metadata.blocked = False
    frontegg_client.update_tenant(tenant.as_request())
