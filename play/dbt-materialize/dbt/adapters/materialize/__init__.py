# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dbt.adapters.materialize.connections import MaterializeCredentials
from dbt.adapters.materialize.impl import MaterializeAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import materialize

Plugin = AdapterPlugin(
    adapter=MaterializeAdapter,
    credentials=MaterializeCredentials,
    include_path=materialize.PACKAGE_PATH,
)
