# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass

# These values must be in lowercase
FEATURE_FLAG_TRUE_VALUE = "true"
FEATURE_FLAG_FALSE_VALUE = "false"


@dataclass
class FeatureFlagValue:
    name: str
    value: str


@dataclass
class FeatureFlagSystemConfiguration:
    name: str
    shortcut: str
    flags: list[FeatureFlagValue]

    def to_system_params(self) -> dict[str, str]:
        return {flag.name: flag.value for flag in self.flags}


@dataclass
class FeatureFlagSystemConfigurationPair:
    name: str
    config1: FeatureFlagSystemConfiguration
    config2: FeatureFlagSystemConfiguration

    def __post_init__(self):
        assert (
            self.config1.name != self.config2.name
        ), "Feature flag configurations must have different names"
        assert (
            self.config1.shortcut != self.config2.shortcut
        ), "Feature flag configurations must have different shortcuts"

    def get_configs(self) -> list[FeatureFlagSystemConfiguration]:
        return [self.config1, self.config2]


def create_boolean_feature_flag_configuration_pair(
    flag_name: str, shortcut: str
) -> FeatureFlagSystemConfigurationPair:
    return FeatureFlagSystemConfigurationPair(
        name=flag_name,
        config1=FeatureFlagSystemConfiguration(
            name="default",
            shortcut="default",
            flags=[FeatureFlagValue(flag_name, FEATURE_FLAG_FALSE_VALUE)],
        ),
        config2=FeatureFlagSystemConfiguration(
            name=f"w/ {flag_name}",
            shortcut=shortcut,
            flags=[FeatureFlagValue(flag_name, FEATURE_FLAG_TRUE_VALUE)],
        ),
    )
