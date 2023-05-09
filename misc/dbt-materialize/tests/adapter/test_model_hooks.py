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

import pytest
from dbt.tests.adapter.hooks import test_model_hooks as core_base
from fixtures import model_hook, test_hooks


class TestPrePostModelHooksMaterialize(core_base.TestPrePostModelHooks):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(f"drop table if exists { project.test_schema }.on_model_hook")
        project.run_sql(model_hook.format(schema=project.test_schema))

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return test_hooks

    def check_hooks(self, state, project, host, count=1):
        self.get_ctx_vars(state, count=count, project=project)


class TestPrePostModelHooksUnderscoresMaterialize(
    core_base.TestPrePostModelHooksUnderscores
):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(f"drop table if exists { project.test_schema }.on_model_hook")
        project.run_sql(model_hook.format(schema=project.test_schema))

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return test_hooks

    def check_hooks(self, state, project, host, count=1):
        self.get_ctx_vars(state, count=count, project=project)


class TestHookRefsMaterialize(core_base.TestHookRefs):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(f"drop table if exists { project.test_schema }.on_model_hook")
        project.run_sql(model_hook.format(schema=project.test_schema))

    def check_hooks(self, state, project, host, count=1):
        self.get_ctx_vars(state, count=count, project=project)
