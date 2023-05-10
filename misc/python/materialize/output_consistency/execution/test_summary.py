# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


class ConsistencyTestSummary:
    def __init__(
        self,
        count_executed_query_templates: int,
        count_successful_query_templates: int,
        dry_run: bool,
    ):
        self.count_executed_query_templates = count_executed_query_templates
        self.count_successful_query_templates = count_successful_query_templates
        self.mode = "DB" if not dry_run else "DRY_RUN"

    def all_passed(self) -> bool:
        return (
            self.count_successful_query_templates == self.count_executed_query_templates
        )

    def __str__(self) -> str:
        return f"{self.count_successful_query_templates}/{self.count_executed_query_templates} passed in mode '{self.mode}'."
