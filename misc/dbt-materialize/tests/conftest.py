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

import os

import pytest

pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "materialize",
        "threads": 1,
        "host": "{{ env_var('DBT_HOST', 'localhost') }}",
        "user": "materialize",
        "pass": "password",
        "database": "materialize",
        "port": "{{ env_var('DBT_PORT', 6875) }}",
        "sslmode": "{{ env_var('DBT_SSLMODE', '') }}",
        "sslcert": "{{ env_var('DBT_SSLCERT', '') }}",
        "sslkey": "{{ env_var('DBT_SSLKEY', '') }}",
        "sslrootcert": "{{ env_var('DBT_SSLROOTCERT', '') }}",
    }
