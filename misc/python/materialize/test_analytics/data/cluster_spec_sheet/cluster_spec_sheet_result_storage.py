# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass

from materialize import buildkite
from materialize.buildkite import BuildkiteEnvVar
from materialize.test_analytics.data.base_data_storage import BaseDataStorage
from materialize.test_analytics.util.mz_sql_util import as_sanitized_literal


@dataclass
class ClusterSpecSheetResultEntry:
    scenario: str
    scenario_version: str
    scale: int
    mode: str
    category: str
    test_name: str
    cluster_size: str
    repetition: int
    size_bytes: int | None
    time_ms: int | None


@dataclass
class ClusterSpecSheetEnvironmentdResultEntry:
    scenario: str
    scenario_version: str
    scale: int
    mode: str
    category: str
    test_name: str
    envd_cpus: int
    repetition: int
    qps: float | None


class ClusterSpecSheetResultStorage(BaseDataStorage):

    def add_result(
        self,
        framework_version: str,
        results: list[ClusterSpecSheetResultEntry],
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        for result_entry in results:
            # TODO: remove NULL castings when database-issues#8100 is resolved
            sql_statements.append(f"""
                INSERT INTO cluster_spec_sheet_result
                (
                    build_job_id,
                    framework_version,
                    scenario,
                    scenario_version,
                    scale,
                    mode,
                    category,
                    test_name,
                    cluster_size,
                    repetition,
                    size_bytes,
                    time_ms
                )
                SELECT
                    {as_sanitized_literal(job_id)},
                    {as_sanitized_literal(framework_version)},
                    {as_sanitized_literal(result_entry.scenario)},
                    {as_sanitized_literal(result_entry.scenario_version)},
                    {result_entry.scale},
                    {as_sanitized_literal(result_entry.mode)},
                    {as_sanitized_literal(result_entry.category)},
                    {as_sanitized_literal(result_entry.test_name)},
                    {as_sanitized_literal(result_entry.cluster_size)},
                    {result_entry.repetition},
                    {result_entry.size_bytes or 'NULL::BIGINT'},
                    {result_entry.time_ms or 'NULL::BIGINT'}
                ;
                """)

        self.database_connector.add_update_statements(sql_statements)


class ClusterSpecSheetEnvironmentdResultStorage(BaseDataStorage):

    def add_result(
        self,
        framework_version: str,
        results: list[ClusterSpecSheetEnvironmentdResultEntry],
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        for result_entry in results:
            # TODO: remove NULL castings when database-issues#8100 is resolved
            sql_statements.append(f"""
                INSERT INTO cluster_spec_sheet_environmentd_result
                (
                    build_job_id,
                    framework_version,
                    scenario,
                    scenario_version,
                    scale,
                    mode,
                    category,
                    test_name,
                    envd_cpus,
                    repetition,
                    qps
                )
                SELECT
                    {as_sanitized_literal(job_id)},
                    {as_sanitized_literal(framework_version)},
                    {as_sanitized_literal(result_entry.scenario)},
                    {as_sanitized_literal(result_entry.scenario_version)},
                    {result_entry.scale},
                    {as_sanitized_literal(result_entry.mode)},
                    {as_sanitized_literal(result_entry.category)},
                    {as_sanitized_literal(result_entry.test_name)},
                    {result_entry.envd_cpus},
                    {result_entry.repetition},
                    {result_entry.qps or 'NULL::FLOAT'}
                ;
                """)

        self.database_connector.add_update_statements(sql_statements)


class ClusterSpecSheetTestExplanationStorage(BaseDataStorage):
    """The `test_name` to explanation mapping shown next to spec sheet charts.

    Unlike the result tables this holds no per-build rows: the `explanation`
    passed to each measurement in test/cluster-spec-sheet/mzcompose.py is the
    source of truth and a default branch run upserts what it recorded, at most
    one row per test name.
    """

    def add_or_update_explanations(self, explanations: dict[str, str]) -> None:
        """Upsert one row per entry, keyed by test name.

        Only for the default branch: the rows are global, and the build number
        that orders them tells nothing about a branch commit. The caller
        enforces that.

        A row already written by a newer build is left alone, so a job of an
        older commit finishing after a newer build cannot revert the text to
        what that older commit said. Jobs of the same build may overwrite each
        other, which is harmless because they carry the same text.

        Explanations are never deleted here: a renamed or removed test leaves
        its row behind until someone cleans it up.
        """
        build_number = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_NUMBER)

        sql_statements = []

        for test_name, explanation in explanations.items():
            name_literal = as_sanitized_literal(test_name)
            explanation_literal = as_sanitized_literal(explanation)

            sql_statements.append(f"""
                UPDATE cluster_spec_sheet_test_explanation
                SET explanation = {explanation_literal}, build_number = {build_number}
                WHERE test_name = {name_literal}
                AND build_number <= {build_number}
                ;
                """)
            sql_statements.append(f"""
                INSERT INTO cluster_spec_sheet_test_explanation (test_name, explanation, build_number)
                    SELECT {name_literal}, {explanation_literal}, {build_number}
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM cluster_spec_sheet_test_explanation
                        WHERE test_name = {name_literal}
                    )
                ;
                """)

        self.database_connector.add_update_statements(sql_statements)
