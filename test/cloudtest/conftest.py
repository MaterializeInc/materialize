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

from materialize.cloudtest.app.materialize_application import MaterializeApplication


def pytest_configure(config: "Config") -> None:
    config.addinivalue_line(
        "markers",
        "long: A long running test. Select with -m=long, deselect with -m 'not long'",
    )
    config.addinivalue_line(
        "markers",
        "node_recovery: Tests that require a separate cluster definition",
    )


@pytest.fixture(scope="session")
def mz(pytestconfig: pytest.Config) -> MaterializeApplication:
    return MaterializeApplication(
        release_mode=(not pytestconfig.getoption("dev")),
        # NOTE(necaris): pyright doesn't like that the `getoption` default type is `Notset`
        aws_region=pytestconfig.getoption("aws_region", None),  # type: ignore
        log_filter=pytestconfig.getoption("log_filter", None),  # type: ignore
        apply_node_selectors=bool(
            pytestconfig.getoption("apply_node_selectors", None) or False  # type: ignore
        ),
    )


@pytest.fixture(scope="session")
def log_filter(pytestconfig: pytest.Config) -> Any:
    return pytestconfig.getoption("log_filter")


@pytest.fixture(scope="session")
def dev(pytestconfig: pytest.Config) -> Any:
    return pytestconfig.getoption("dev")


@pytest.fixture(scope="session")
def aws_region(pytestconfig: pytest.Config) -> Any:
    return pytestconfig.getoption("aws_region")


def pytest_addoption(parser: pytest.Parser) -> None:
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

    parser.addoption(
        "--apply-node-selectors",
        action="store_true",
        default=False,
    )
