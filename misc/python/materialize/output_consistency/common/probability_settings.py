# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


class ProbabilitySettings:
    def __init__(
        self,
    ) -> None:
        self.horizontal_layout_when_not_aggregated = 0.9
        self.horizontal_layout_when_aggregated = 0.1
        self.create_complex_expression = 0.2
        self.restrict_vertical_layout_to_2_or_3_rows = 0.8
