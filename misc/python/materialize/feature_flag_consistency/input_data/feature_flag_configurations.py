# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.feature_flag_consistency.feature_flag.feature_flag import (
    FeatureFlagSystemConfigurationPair,
)

FEATURE_FLAG_CONFIGURATION_PAIRS = dict()


def append_config(config_pair: FeatureFlagSystemConfigurationPair) -> None:
    FEATURE_FLAG_CONFIGURATION_PAIRS[config_pair.name] = config_pair
