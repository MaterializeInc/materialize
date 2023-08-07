# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from _pytest.config import Config

import pytest
from frontegg_api import FronteggClient

from materialize.cloudtest.app.cloudtest_application import CloudtestApplication
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.config.controller_config import (
    CONTROLLER_DEFINITIONS,
    CONTROLLER_NAMES,
)
from materialize.cloudtest.config.environment_config import (
    EnvironmentConfig,
    load_egress_ip_config,
    load_environment_config,
)
from materialize.cloudtest.config.frontegg_config import (
    create_frontegg_client,
    load_frontegg_tenant,
    update_tenant,
)
from materialize.cloudtest.util.controller import (
    Endpoint,
    cleanup_controllers,
    launch_controllers,
    wait_for_controllers,
)
from materialize.cloudtest.util.environment import (
    create_environment_assignment,
    delete_environment_assignment,
)
from materialize.cloudtest.util.kubectl import cleanup_crds


def pytest_configure(config: "Config") -> None:
    config.addinivalue_line(
        "markers",
        "long: A long running test. Select with -m=long, deselect with -m 'not long'",
    )


@pytest.fixture(scope="session")
def mz(pytestconfig: pytest.Config) -> MaterializeApplication:
    return CloudtestApplication()


@pytest.fixture(scope="session")
def log_filter(pytestconfig: pytest.Config) -> Any:
    return pytestconfig.getoption("log_filter")


@pytest.fixture(scope="session")
def dev(pytestconfig: pytest.Config) -> Any:
    return pytestconfig.getoption("dev")


@pytest.fixture(scope="session")
def aws_region(pytestconfig: pytest.Config) -> Any:
    return pytestconfig.getoption("aws_region")


@pytest.fixture
def config(pytestconfig: pytest.Config):
    args = pytestconfig.option

    config = load_environment_config(pytestconfig)

    delete_environment_assignment(config)
    yield config

    if not args.skip_cleanup:
        # Re-authenticate if using frontegg auth since our token could have expired
        # while the test ran
        if args.e2e_test_user_email is not None:
            config.refresh_auth()
        delete_environment_assignment(config)


@pytest.fixture
def egress_ip_config(pytestconfig: pytest.Config):
    return load_egress_ip_config(pytestconfig)


@pytest.fixture
def environment_assignment(config: EnvironmentConfig):
    return create_environment_assignment(config)


@pytest.fixture(scope="session")
def app(pytestconfig: pytest.Config):
    return CloudtestApplication()


#@pytest.fixture
def frontegg_client(pytestconfig: pytest.Config):
    return create_frontegg_client(pytestconfig)


#@pytest.fixture
def frontegg_tenant(
    config: EnvironmentConfig,
    frontegg_client: FronteggClient,
):
    tenant = load_frontegg_tenant(config, frontegg_client)
    update_tenant(frontegg_client, tenant)

    yield tenant

    update_tenant(frontegg_client, tenant)


def pytest_addoption(parser: pytest.Parser, pluginmanager: pytest.PytestPluginManager):
    parser.addoption("--dev", action="store_true")
    parser.addoption(
        "--aws-region",
        action="store",
        default=None,
        help="AWS region to pass to testdrive",
    )
    parser.addoption(
        "--log-filter",
        action="store",
        default=None,
        help="Log filter for Materialize binaries",
    )

    parser.addoption("--region", default="local")
    parser.addoption("--stack", default="kind")

    parser.addoption("--system-context", default="kind-mzcloud")
    parser.addoption("--environment-context", default="kind-mzcloud")

    for controller_definition in CONTROLLER_DEFINITIONS:
        if controller_definition.is_configurable:
            parser.addoption(
                f"--{controller_definition.name}-address",
                default=f"http://127.0.0.1:{controller_definition.default_port}",
                type=Endpoint.parse,
            )

    parser.addoption("--e2e-test-user-email")
    parser.addoption("--e2e-test-user-password")
    parser.addoption("--frontegg-host")
    parser.addoption("--frontegg-vendor-client-id")
    parser.addoption("--frontegg-vendor-secret-key")

    parser.addoption("--testdb-dbname")
    parser.addoption("--testdb-user")
    parser.addoption("--testdb-password")
    parser.addoption("--testdb-host")
    parser.addoption("--testdb-port")

    # TODO changed to True
    parser.addoption("--launch-controllers", action="store_true", default=True)

    parser.addoption("--skip-cleanup", action="store_true", default=False)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]):
    for item in items:
        if not any(item.iter_markers()):
            item.add_marker("unmarked")

    # if we're explicitly selecting the tests to run, don't filter them
    if config.option.keyword or config.option.markexpr:
        return

    for item in items:
        if "frontegg" in item.keywords:
            item.add_marker(
                pytest.mark.skip(reason="frontegg tests are disabled by default"),
            )
        if "egress_ips" in item.keywords:
            item.add_marker(
                pytest.mark.skip(reason="egress ip tests are disabled by default"),
            )


def pytest_sessionstart(session: pytest.Session):
    args = session.config.option

    if args.launch_controllers:
        cleanup_controllers()
    cleanup_crds(args.system_context, args.environment_context)
    if args.launch_controllers:
        launch_controllers(CONTROLLER_NAMES)
        wait_for_controllers(
            args.sync_server_address,
            args.internal_api_server_address,
            args.region_api_server_address,
        )


def pytest_sessionfinish(session: pytest.Session):
    args = session.config.option
    if args.launch_controllers and not args.skip_cleanup:
        cleanup_controllers()

    if not args.skip_cleanup:
        cleanup_crds(args.system_context, args.environment_context)
