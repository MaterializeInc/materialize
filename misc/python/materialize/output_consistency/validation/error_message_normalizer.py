# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re

FUNCTION_ID_SUFFIX_PATTERN = re.compile(r" \(function \[s\d+ AS pg_catalog\.\w+\]\)")
# Example: column "table_func_8ab9ea9d-340c-45c2-967d-65118d6c979b" does not exist
TABLE_FUNC_PATTERN = re.compile(r"column \"table_func_[a-f0-9-]+\"")


class ErrorMessageNormalizer:
    def normalize(self, error_message: str) -> str:
        # replace source prefix in column
        normalized_message = error_message
        normalized_message = re.sub(
            'column "[^.]*\\.', 'column "<source>.', normalized_message
        )
        normalized_message = re.sub(
            TABLE_FUNC_PATTERN, 'column "table_func_x"', normalized_message
        )
        normalized_message = re.sub(FUNCTION_ID_SUFFIX_PATTERN, "", normalized_message)
        normalized_message = normalized_message.replace("Evaluation error: ", "")

        # This will replace ln, log, and log10 mentions with log
        # see https://github.com/MaterializeInc/database-issues/issues/5902
        normalized_message = re.sub(
            "(?<=function )(ln|log|log10)(?= is not defined for zero)",
            "log",
            normalized_message,
        )

        if (
            "not found in data type record" in normalized_message
            or (
                re.search(
                    r"function .*?record\(.*\) does not exist", normalized_message
                )
            )
            or (re.search(r"operator does not exist: .*?record", normalized_message))
            or "CAST does not support casting from record" in normalized_message
        ):
            # tracked with https://github.com/MaterializeInc/database-issues/issues/8243
            normalized_message = normalized_message.replace("?", "")

        # strip error message details (see materialize#29661, materialize#19822, database-issues#7061)
        normalized_message = re.sub(r" \(.*?\.\)", "", normalized_message)

        return normalized_message
