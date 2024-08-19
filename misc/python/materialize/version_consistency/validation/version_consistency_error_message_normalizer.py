# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re

from materialize.output_consistency.validation.error_message_normalizer import (
    ErrorMessageNormalizer,
)

FUNCTION_ID_SUFFIX_PATTERN = re.compile(r" \(function \[s\d+ AS pg_catalog\.\w+\]\)")
# Example: column "table_func_8ab9ea9d-340c-45c2-967d-65118d6c979b" does not exist
TABLE_FUNC_PATTERN = re.compile(r"column \"table_func_[a-f0-9-]+\"")


class VersionConsistencyErrorMessageNormalizer(ErrorMessageNormalizer):
    def normalize(self, error_message: str) -> str:
        error_message = super().normalize(error_message)
        error_message = re.sub(FUNCTION_ID_SUFFIX_PATTERN, "", error_message)
        error_message = re.sub(
            TABLE_FUNC_PATTERN, 'column "table_func_x"', error_message
        )

        return error_message
