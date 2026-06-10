# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import buildkite
from materialize.buildkite import BuildkiteEnvVar
from materialize.test_analytics.data.base_data_storage import BaseDataStorage
from materialize.test_analytics.util.mz_sql_util import as_sanitized_literal


class OutputConsistencyStatsStorage(BaseDataStorage):

    def add_stats(
        self,
        count_executed_queries: int,
        count_successful_queries: int,
        count_ignored_error_queries: int,
        count_failures: int,
        count_available_data_types: int,
        count_available_op_variants: int,
        count_predefined_queries: int,
        count_generated_select_expressions: int,
        count_ignored_select_expressions: int,
        count_used_ops: int,
    ) -> None:
        if count_generated_select_expressions == 0:
            print("No upload to test analytics because the job was aborted")
            return

        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)
        step_key = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_KEY)

        sql_statement = f"""
            INSERT INTO output_consistency_stats
            (
                build_job_id,
                framework,
                count_generated_expressions,
                count_ignored_expressions,
                count_executed_queries,
                count_successful_queries,
                count_ignored_error_queries,
                count_failed_queries,
                count_predefined_queries,
                count_available_data_types,
                count_available_ops,
                count_used_ops
            )
            SELECT
                {as_sanitized_literal(job_id)},
                {as_sanitized_literal(step_key)},
                {count_generated_select_expressions},
                {count_ignored_select_expressions},
                {count_executed_queries},
                {count_successful_queries},
                {count_ignored_error_queries},
                {count_failures},
                {count_predefined_queries},
                {count_available_data_types},
                {count_available_op_variants},
                {count_used_ops}
            ;
            """

        self.database_connector.add_update_statements([sql_statement])
