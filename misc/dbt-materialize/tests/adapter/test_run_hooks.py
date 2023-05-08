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
from dbt.tests.adapter.hooks import test_run_hooks as core_base
from fixtures import test_run_operation


class TestPrePostRunHooksMaterialize(core_base.TestPrePostRunHooks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return test_run_operation

    def check_hooks(self, state, project, host):
        ctx = self.get_ctx_vars(state, project)

        assert ctx["test_state"] == state
        assert ctx["target_dbname"] == "materialize"
        assert ctx["target_host"] == "materialized"
        assert ctx["target_name"] == "default"
        assert ctx["target_schema"] == project.test_schema
        assert ctx["target_threads"] == 1
        assert ctx["target_type"] == project.adapter_type

        assert (
            ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
        ), "run_started_at was not set"
        assert (
            ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
        ), "invocation_id was not set"


class TestAfterRunHooksMaterialize(core_base.TestAfterRunHooks):
    pass
