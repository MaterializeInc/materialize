# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from functools import partial

from materialize.mz_version import MzVersion
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.execution.query_output_mode import QueryOutputMode
from materialize.output_consistency.expression.expression import (
    Expression,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.ignore_filter.expression_matchers import (
    involves_data_type_category,
    is_any_date_time_expression,
    is_known_to_involve_exact_data_types,
    is_operation_tagged,
    is_timezone_conversion_expression,
    matches_fun_by_any_name,
    matches_fun_by_name,
    matches_op_by_any_pattern,
    matches_op_by_pattern,
)
from materialize.output_consistency.ignore_filter.ignore_verdict import (
    YesIgnore,
)
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    GenericInconsistencyIgnoreFilter,
    PostExecutionInconsistencyIgnoreFilterBase,
    PreExecutionInconsistencyIgnoreFilterBase,
)
from materialize.output_consistency.ignore_filter.internal_output_inconsistency_ignore_filter import (
    IgnoreVerdict,
    uses_aggregation_shortcut_optimization,
)
from materialize.output_consistency.input_data.operations.string_operations_provider import (
    TAG_REGEX,
)
from materialize.output_consistency.input_data.types.number_types_provider import (
    NON_INTEGER_TYPE_IDENTIFIERS,
)
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.row_selection import DataRowSelection
from materialize.output_consistency.validation.validation_message import ValidationError
from materialize.postgres_consistency.ignore_filter.pg_inconsistency_ignore_filter import (
    MATH_FUNCTIONS_WITH_PROBLEMATIC_FLOATING_BEHAVIOR,
)

# Do not specify "-dev" versions. The suffix will be cropped; it is not necessary.
MZ_VERSION_0_77_0 = MzVersion.parse_mz("v0.77.0")
MZ_VERSION_0_78_0 = MzVersion.parse_mz("v0.78.0")
MZ_VERSION_0_81_0 = MzVersion.parse_mz("v0.81.0")
MZ_VERSION_0_88_0 = MzVersion.parse_mz("v0.88.0")
MZ_VERSION_0_93_0 = MzVersion.parse_mz("v0.93.0")
MZ_VERSION_0_95_0 = MzVersion.parse_mz("v0.95.0")
MZ_VERSION_0_99_0 = MzVersion.parse_mz("v0.99.0")
MZ_VERSION_0_107_0 = MzVersion.parse_mz("v0.107.0")
MZ_VERSION_0_109_0 = MzVersion.parse_mz("v0.109.0")
MZ_VERSION_0_117_0 = MzVersion.parse_mz("v0.117.0")
MZ_VERSION_0_118_0 = MzVersion.parse_mz("v0.118.0")
MZ_VERSION_0_128_0 = MzVersion.parse_mz("v0.128.0")


class VersionConsistencyIgnoreFilter(GenericInconsistencyIgnoreFilter):
    def __init__(
        self,
        mz1_version_without_dev_suffix: MzVersion,
        mz2_version_without_dev_suffix: MzVersion,
        uses_dfr: bool,
    ):
        assert (
            not mz1_version_without_dev_suffix.is_dev_version()
        ), "mz1_version is a dev version but that suffix should be dropped"
        assert (
            not mz2_version_without_dev_suffix.is_dev_version()
        ), "mz1_version is a dev version but that suffix should be dropped"

        lower_version, higher_version = (
            (mz1_version_without_dev_suffix, mz2_version_without_dev_suffix)
            if mz1_version_without_dev_suffix < mz2_version_without_dev_suffix
            else (mz2_version_without_dev_suffix, mz1_version_without_dev_suffix)
        )
        super().__init__(
            VersionPreExecutionInconsistencyIgnoreFilter(
                lower_version, higher_version, uses_dfr
            ),
            VersionPostExecutionInconsistencyIgnoreFilter(
                lower_version, higher_version, uses_dfr
            ),
        )


class VersionPreExecutionInconsistencyIgnoreFilter(
    PreExecutionInconsistencyIgnoreFilterBase
):
    def __init__(
        self, lower_version: MzVersion, higher_version: MzVersion, uses_dfr: bool
    ):
        self.lower_version = lower_version
        self.higher_version = higher_version
        self.uses_dfr = uses_dfr

    def shall_ignore_expression(
        self, expression: Expression, row_selection: DataRowSelection
    ) -> IgnoreVerdict:
        if (
            self.lower_version <= MZ_VERSION_0_77_0 <= self.higher_version
            and is_any_date_time_expression(expression)
        ):
            return YesIgnore("Fixed issue regarding time zone handling")

        if (
            self.lower_version <= MZ_VERSION_0_78_0 <= self.higher_version
            and expression.matches(
                partial(
                    matches_fun_by_any_name,
                    function_names_in_lower_case={"array_agg", "string_agg"},
                ),
                True,
            )
        ):
            return YesIgnore("Accepted: no order explicitly specified")

        if (
            self.lower_version <= MZ_VERSION_0_81_0 <= self.higher_version
            and expression.matches(
                partial(
                    matches_fun_by_any_name,
                    function_names_in_lower_case={"min", "max"},
                ),
                True,
            )
        ):
            return YesIgnore(
                "Implemented min/max for interval and time types in PR 24007"
            )

        if (
            self.lower_version <= MZ_VERSION_0_88_0 <= self.higher_version
            and expression.matches(
                partial(
                    matches_fun_by_name,
                    function_name_in_lower_case="date_trunc",
                ),
                True,
            )
        ):
            return YesIgnore("date_trunc fixed in PR 25202")

        if (
            self.lower_version <= MZ_VERSION_0_93_0 <= self.higher_version
            and expression.matches(
                partial(
                    matches_op_by_any_pattern,
                    patterns={
                        "$ ILIKE $",
                        "$ NOT ILIKE $",
                    },
                ),
                True,
            )
        ):
            return YesIgnore("ILIKE fixed in PR 26183")

        if (
            self.lower_version <= MZ_VERSION_0_93_0 <= self.higher_version
            and expression.matches(
                partial(is_operation_tagged, tag=TAG_REGEX),
                True,
            )
        ):
            return YesIgnore("Newline handling in regex fixed in PR 26191")

        if (
            self.lower_version <= MZ_VERSION_0_95_0 <= self.higher_version
            and expression.matches(
                partial(
                    matches_fun_by_any_name,
                    function_names_in_lower_case={"min", "max"},
                ),
                True,
            )
            and is_any_date_time_expression(expression)
        ):
            return YesIgnore("Type of min(time) / max(time) fixed in PR 26335")

        if (
            self.lower_version <= MZ_VERSION_0_99_0 <= self.higher_version
            and is_any_date_time_expression(expression)
        ):
            return YesIgnore(
                "Casting intervals to mz_timestamps introduced in PR 26970"
            )

        if (
            self.lower_version <= MZ_VERSION_0_107_0 <= self.higher_version
            and expression.matches(
                partial(
                    matches_op_by_pattern,
                    pattern="$ @> $",
                ),
                True,
            )
        ):
            return YesIgnore("Contains on list and array introduced in PR 27959")

        if self.lower_version <= MZ_VERSION_0_118_0 <= self.higher_version:
            if expression.matches(
                partial(
                    involves_data_type_category,
                    data_type_category=DataTypeCategory.RANGE,
                ),
                True,
            ):
                return YesIgnore("Changes to array range presentation in PR 29532")

            if expression.matches(
                partial(
                    involves_data_type_category,
                    data_type_category=DataTypeCategory.BYTEA,
                ),
                True,
            ):
                return YesIgnore("Changes to byte array presentation in PR 29591")

            if is_any_date_time_expression(expression):
                return YesIgnore(
                    "Implicit cast from interval to mz_timestamp removed in PR 29579"
                )

            if expression.matches(
                partial(
                    matches_fun_by_any_name,
                    function_names_in_lower_case=MATH_FUNCTIONS_WITH_PROBLEMATIC_FLOATING_BEHAVIOR,
                ),
                True,
            ) or expression.matches(
                partial(
                    is_known_to_involve_exact_data_types,
                    internal_data_type_identifiers=NON_INTEGER_TYPE_IDENTIFIERS,
                ),
                True,
            ):
                return YesIgnore("Introduced variable-length encoding in PR 29454")

        return super().shall_ignore_expression(expression, row_selection)


class VersionPostExecutionInconsistencyIgnoreFilter(
    PostExecutionInconsistencyIgnoreFilterBase
):
    def __init__(
        self, lower_version: MzVersion, higher_version: MzVersion, uses_dfr: bool
    ):
        self.lower_version = lower_version
        self.higher_version = higher_version
        self.uses_dfr = uses_dfr

    def _shall_ignore_success_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        if (
            self.lower_version <= MZ_VERSION_0_109_0 <= self.higher_version
            and self._aggregration_shortcut_changed(
                query_template, contains_aggregation
            )
        ):
            return YesIgnore("Evaluation order changed with PR 28144")

        if (
            self.lower_version <= MZ_VERSION_0_117_0 <= self.higher_version
        ) and query_template.matches_any_expression(
            is_any_date_time_expression,
            True,
        ):
            # the older version may fail where the newer no longer does
            return YesIgnore(
                "Added support for implicit cast from date to mz_timestamp with PR#29494"
            )

        return super()._shall_ignore_success_mismatch(
            error, query_template, contains_aggregation
        )

    def _shall_ignore_content_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
        col_index: int,
        all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        if (
            self.lower_version <= MZ_VERSION_0_117_0 <= self.higher_version
            and query_template.matches_any_expression(
                partial(
                    matches_fun_by_name,
                    function_name_in_lower_case="unnest",
                ),
                True,
            )
        ):
            # the involvement of unnest affects all columns, therefore use matches_any_expression
            return YesIgnore(
                "Involvement of unnest changes order with PR#29422 for both DFR and CTF"
            )

        return super()._shall_ignore_content_mismatch(
            error,
            query_template,
            contains_aggregation,
            col_index,
            all_involved_characteristics,
        )

    def _shall_ignore_error_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        if (
            self.lower_version <= MZ_VERSION_0_117_0 <= self.higher_version
        ) and query_template.matches_any_expression(
            is_any_date_time_expression,
            True,
        ):
            # this causes different errors
            return YesIgnore(
                "Added support for implicit cast from date to mz_timestamp with PR#29494"
            )

        if (
            self.lower_version <= MZ_VERSION_0_128_0 <= self.higher_version
        ) and query_template.matches_any_expression(
            is_timezone_conversion_expression,
            True,
        ):
            return YesIgnore("Changed error message for private preview features")

        return super()._shall_ignore_error_mismatch(
            error, query_template, contains_aggregation
        )

    def _shall_ignore_explain_plan_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
        query_output_mode: QueryOutputMode,
    ) -> IgnoreVerdict:
        if (
            self.lower_version <= MZ_VERSION_0_109_0 <= self.higher_version
            and self._aggregration_shortcut_changed(
                query_template, contains_aggregation
            )
        ):
            return YesIgnore("Evaluation order changed with PR 28144")

        return super()._shall_ignore_explain_plan_mismatch(
            error, query_template, contains_aggregation, query_output_mode
        )

    def _aggregration_shortcut_changed(
        self, query_template: QueryTemplate, contains_aggregation: bool
    ) -> bool:
        return query_template.matches_any_expression(
            partial(
                uses_aggregation_shortcut_optimization,
                contains_aggregation=contains_aggregation,
            ),
            True,
        )
