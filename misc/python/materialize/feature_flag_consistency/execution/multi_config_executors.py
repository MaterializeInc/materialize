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
from materialize.output_consistency.execution.multi_mz_executors import (
    MultiMzSqlExecutors,
)
from materialize.output_consistency.execution.sql_executor import SqlExecutor


class MultiConfigSqlExecutors(MultiMzSqlExecutors):

    def __init__(
        self,
        executor1: SqlExecutor,
        executor2: SqlExecutor,
        flag_configuration_pair: FeatureFlagSystemConfigurationPair,
    ):
        super().__init__(executor1, executor2)
        self.flag_configuration_pair = flag_configuration_pair

    def get_database_infos(self) -> str:
        config1_info = str(self.flag_configuration_pair.config1.to_system_params())
        config2_info = str(self.flag_configuration_pair.config2.to_system_params())
        return (
            f"Using {self.executor.name} in version '{self.executor.query_version()}' with config '{config1_info}'. "
            f"Using {self.executor2.name} in version '{self.executor2.query_version()}' with config '{config2_info}'. "
        )
