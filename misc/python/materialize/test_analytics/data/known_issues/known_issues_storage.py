# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.github import KnownGitHubIssue
from materialize.test_analytics.data.base_data_storage import BaseDataStorage
from materialize.test_analytics.util.mz_sql_util import as_sanitized_literal


class KnownIssuesStorage(BaseDataStorage):
    def add_or_update_issue(
        self,
        issue: KnownGitHubIssue,
    ) -> None:
        issue_str = (
            f"{issue.info['repository_url'].split('/')[-1]}/{issue.info['number']}"
        )
        sql_statements = [
            f"""
            UPDATE issue
            SET title = {as_sanitized_literal(issue.info["title"])},
                ci_regexp = {as_sanitized_literal(issue.regex.pattern.decode("utf-8"))},
                state = {as_sanitized_literal(issue.info["state"])}
            WHERE issue_id = {as_sanitized_literal(issue_str)}
            ;
            """,
            f"""
            INSERT INTO issue (issue_id, title, ci_regexp, state)
                SELECT {as_sanitized_literal(issue_str)},
                       {as_sanitized_literal(issue.info["title"])},
                       {as_sanitized_literal(issue.regex.pattern.decode("utf-8"))},
                       {as_sanitized_literal(issue.info["state"])}
                WHERE NOT EXISTS (
                    SELECT 1 FROM issue WHERE issue_id = {as_sanitized_literal(issue_str)}
                )
            ;
            """,
        ]

        self.database_connector.add_update_statements(sql_statements)
