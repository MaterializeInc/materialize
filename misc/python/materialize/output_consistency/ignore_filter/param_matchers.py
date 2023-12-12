# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections.abc import Callable

from materialize.output_consistency.operation.operation_param import OperationParam


def index_of_param_by_type(
    params: list[OperationParam], type_to_match: type[OperationParam]
) -> int | None:
    return index_of_param(params, lambda p: isinstance(p, type_to_match))


def index_of_param_by_equality(
    params: list[OperationParam], param_to_match: OperationParam
) -> int | None:
    return index_of_param(params, lambda p: p == param_to_match)


def index_of_param(
    params: list[OperationParam], match_fn: Callable[[OperationParam], bool]
) -> int | None:
    for i, param in enumerate(params):
        if match_fn(param):
            return i

    return None
