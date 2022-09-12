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


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="materialize_cloud", type=str)


# Using @pytest.mark.skip_profile('materialize_binary') uses the 'skip_by_profile_type'
# autouse fixture below
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )


@pytest.fixture(scope="session")
def profile(request):
    return request.config.getoption("--profile")


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile = request.config.getoption("--profile")
    if profile == "materialize_binary":
        target = materialize_binary_target()
    elif profile == "materialize_cloud":
        target = materialize_cloud_target()
    else:
        raise ValueError(f"Invalid profile type '{profile}'")
    return target


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
def materialize_binary_target():
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


def materialize_cloud_target():
    return {
        "type": "materialize",
        "threads": 1,
        "host": "{{ env_var('DBT_HOST', 'localhost') }}",
        "user": "materialize",
        "pass": "password",
        "database": "materialize",
        "cluster": "default",
        "port": "{{ env_var('DBT_PORT', 6875) }}",
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip("skipped on '{profile_type}' profile")
