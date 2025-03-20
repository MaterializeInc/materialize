# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.expression.expression import Expression


class ArgContext:
    def __init__(self) -> None:
        self.args: list[Expression] = []
        self.contains_aggregation = False

    def append(self, arg: Expression) -> None:
        self.args.append(arg)

        if arg.is_aggregate:
            self.contains_aggregation = True

    def has_no_args(self) -> bool:
        return len(self.args) == 0

    def requires_aggregation(self) -> bool:
        return self.contains_aggregation
