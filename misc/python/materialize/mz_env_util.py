# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""mz env util"""

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.mz import Mz


def get_cloud_hostname(
    c: Composition,
    app_password: str,
    region: str = "aws/us-east-1",
    environment: str = "production",
    quiet: bool = False,
) -> str:
    with c.override(
        Mz(region=region, environment=environment, app_password=app_password)
    ):
        return c.cloud_hostname(quiet=quiet)
