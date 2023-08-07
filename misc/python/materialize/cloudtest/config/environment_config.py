# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass, field
from typing import Callable, Optional

import pytest

from materialize.cloudtest.util.authentication import AuthConfig, get_auth
from materialize.cloudtest.util.controller import Endpoint


@dataclass
class EnvironmentConfig:
    region: str
    stack: str

    system_context: str
    environment_context: str

    sync_server_address: Endpoint
    internal_api_server_address: Endpoint
    region_api_server_address: Endpoint

    e2e_test_user_email: Optional[str]

    refresh_auth_fn: Callable[[], AuthConfig]
    auth: AuthConfig = field(init=False)

    def __post_init__(self):
        self.refresh_auth()

    def refresh_auth(self):
        self.auth = self.refresh_auth_fn()


@dataclass
class EgressIpConfig:
    testdb_dbname: str
    testdb_user: str
    testdb_password: str
    testdb_host: str
    testdb_port: str


def load_environment_config(pytestconfig: pytest.Config):
    args = pytestconfig.option

    config = EnvironmentConfig(
        system_context=args.system_context,
        environment_context=args.environment_context,
        sync_server_address=args.sync_server_address,
        internal_api_server_address=args.internal_api_server_address,
        region_api_server_address=args.region_api_server_address,
        e2e_test_user_email=args.e2e_test_user_email,
        refresh_auth_fn=lambda: get_auth(args),
        region=args.region,
        stack=args.stack,
    )

    return config


def load_egress_ip_config(pytestconfig: pytest.Config):
    args = pytestconfig.option

    testdb_dbname = args.testdb_dbname
    assert testdb_dbname is not None
    testdb_user = args.testdb_user
    assert testdb_user is not None
    testdb_password = args.testdb_password
    assert testdb_password is not None
    testdb_host = args.testdb_host
    assert testdb_host is not None
    testdb_port = args.testdb_port
    assert testdb_port is not None

    return EgressIpConfig(
        testdb_dbname=testdb_dbname,
        testdb_user=testdb_user,
        testdb_password=testdb_password,
        testdb_host=testdb_host,
        testdb_port=testdb_port,
    )
