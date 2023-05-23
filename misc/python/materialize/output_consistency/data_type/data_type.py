# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory


class DataType:
    """Defines a SQL data type"""

    def __init__(self, identifier: str, type_name: str, category: DataTypeCategory):
        self.identifier = identifier
        self.column_name = f"{identifier.lower()}_val"
        """Column name if used with VERTICAL storage layout"""
        self.type_name = type_name
        self.category = category
