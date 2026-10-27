# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Selection of the listener config schema that a Materialize version can parse."""

from materialize import MZ_ROOT
from materialize.mz_version import MzVersion


def resolve_listeners_config_path(
    image_version: MzVersion,
    config_name: str = "testdrive",
) -> str | None:
    """The `config_name` listeners config to mount for an environmentd version.

    Returns None for an environmentd that predates listener-config support,
    in which case no config is mounted.
    """
    if image_version >= "v26.32.0-dev":
        return f"{MZ_ROOT}/src/materialized/ci/listener_configs/v26_32_0/{config_name}.json"
    elif image_version >= "v0.147.0-dev":
        return f"{MZ_ROOT}/src/materialized/ci/listener_configs/v0_147_0/{config_name}.json"
    else:
        # environmentd before v0.147.0 has no MZ_LISTENERS_CONFIG_PATH support.
        return None
