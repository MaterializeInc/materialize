# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


class Watermarks:
    """Holds the minimum and the maximum value expected to be present in a topic, source, table or view"""

    def __init__(self, min_watermark: int = 0, max_watermark: int = 0) -> None:
        self.min = min_watermark
        self.max = max_watermark

    def shift(self, delta: int) -> None:
        self.min = self.min + delta
        self.max = self.max + delta
