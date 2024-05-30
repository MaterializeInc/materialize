# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


class SourceAliasSpec:
    def __init__(self, source_object_index: int):
        self.source_object_index = source_object_index


def source_alias_spec_of_from_object() -> SourceAliasSpec:
    return SourceAliasSpec(0)


def source_alias_spec_of_join_object(join_object_index: int) -> SourceAliasSpec:
    return SourceAliasSpec(1 + join_object_index)
