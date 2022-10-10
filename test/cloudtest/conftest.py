# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _pytest.config import Config

import pytest

from materialize.cloudtest.application import MaterializeApplication


def pytest_configure(config: "Config") -> None:
    config.addinivalue_line(
        "markers",
        "long: A long running test. Select with -m=long, deselect with -m 'not long'",
    )


@pytest.fixture(scope="session")
def mz(pytestconfig: pytest.Config) -> MaterializeApplication:
    return MaterializeApplication(release_mode=(not pytestconfig.getoption("dev")))


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--dev", action="store_true")
