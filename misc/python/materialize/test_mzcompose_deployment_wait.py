# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import DeploymentStatus
from materialize.ui import UIError


def test_deployment_wait_does_not_swallow_interrupt():
    c = SimpleNamespace(
        exec=Mock(
            side_effect=[
                KeyboardInterrupt(),
                SimpleNamespace(stdout='{"status":"ReadyToPromote"}'),
            ]
        )
    )
    with pytest.raises(KeyboardInterrupt):
        Composition.await_mz_deployment_status(
            c, DeploymentStatus.READY_TO_PROMOTE, timeout=1, sleep_time=None
        )
    c.exec.assert_called_once()


def test_deployment_wait_still_retries_probe_failure():
    c = SimpleNamespace(
        exec=Mock(
            side_effect=[
                UIError("probe failed"),
                SimpleNamespace(stdout='{"status":"ReadyToPromote"}'),
            ]
        )
    )
    Composition.await_mz_deployment_status(
        c, DeploymentStatus.READY_TO_PROMOTE, timeout=1, sleep_time=None
    )
    assert c.exec.call_count == 2
