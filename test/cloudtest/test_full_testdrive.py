# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest

from materialize.cloudtest.application import MaterializeApplication


@pytest.mark.long
def test_full_testdrive(mz: MaterializeApplication) -> None:
    mz.testdrive.copy("test/testdrive", "/workdir")
    mz.testdrive.run("testdrive/*.td")
