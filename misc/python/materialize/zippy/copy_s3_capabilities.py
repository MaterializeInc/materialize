# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.zippy.framework import Capability


class S3ObjectExists(Capability):
    """An object has been written to S3 and can be read back."""

    @classmethod
    def format_str(cls) -> str:
        return "zippy/{}"

    def __init__(self, s3_key: str, min_val: int, max_val: int) -> None:
        self.name = s3_key
        self.s3_key = s3_key
        self.min_val = min_val
        self.max_val = max_val
