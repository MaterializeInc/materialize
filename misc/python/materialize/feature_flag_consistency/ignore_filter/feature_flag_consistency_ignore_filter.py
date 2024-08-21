# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.feature_flag_consistency.feature_flag.feature_flag import (
    FeatureFlagSystemConfiguration,
    FeatureFlagSystemConfigurationPair,
)
from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
    is_other_db_evaluation_strategy,
)
from materialize.output_consistency.ignore_filter.ignore_verdict import (
    IgnoreVerdict,
    YesIgnore,
)
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    GenericInconsistencyIgnoreFilter,
    PostExecutionInconsistencyIgnoreFilterBase,
    PreExecutionInconsistencyIgnoreFilterBase,
)
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.validation.validation_message import ValidationError


class FeatureFlagConsistencyIgnoreFilter(GenericInconsistencyIgnoreFilter):
    def __init__(self, configuration_pair: FeatureFlagSystemConfigurationPair):
        super().__init__(
            FeatureFlagPreExecutionInconsistencyIgnoreFilter(configuration_pair),
            FeatureFlagPostExecutionInconsistencyIgnoreFilter(configuration_pair),
        )


class FeatureFlagPreExecutionInconsistencyIgnoreFilter(
    PreExecutionInconsistencyIgnoreFilterBase
):
    def __init__(self, configuration_pair: FeatureFlagSystemConfigurationPair):
        self.configuration_pair = configuration_pair


class FeatureFlagPostExecutionInconsistencyIgnoreFilter(
    PostExecutionInconsistencyIgnoreFilterBase
):
    def __init__(self, configuration_pair: FeatureFlagSystemConfigurationPair):
        self.configuration_pair = configuration_pair

    def _shall_ignore_success_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        if query_template.uses_join() and (
            query_template.has_where_condition() or query_template.has_row_selection()
        ):
            return YesIgnore("#17189: evaluation order")

        return super()._shall_ignore_success_mismatch(
            error, query_template, contains_aggregation
        )

    def _shall_ignore_error_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        if query_template.uses_join() and (
            query_template.has_where_condition() or query_template.has_row_selection()
        ):
            return YesIgnore("#17189: evaluation order")

        return super()._shall_ignore_error_mismatch(
            error, query_template, contains_aggregation
        )


def get_flag_configuration(
    configuration_pair: FeatureFlagSystemConfigurationPair, strategy: EvaluationStrategy
) -> FeatureFlagSystemConfiguration:
    if is_other_db_evaluation_strategy(strategy.identifier):
        return configuration_pair.config2
    else:
        return configuration_pair.config1
