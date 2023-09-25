# Copyright 2020 Josh Wills. All rights reserved.
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dbt.adapters.base import AdapterPlugin
from dbt.adapters.materialize.connections import (
    MaterializeCredentials,
)
from dbt.adapters.materialize.impl import MaterializeAdapter
from dbt.include import materialize

Plugin = AdapterPlugin(
    adapter=MaterializeAdapter,
    credentials=MaterializeCredentials,
    include_path=materialize.PACKAGE_PATH,
    dependencies=["postgres"],
)
