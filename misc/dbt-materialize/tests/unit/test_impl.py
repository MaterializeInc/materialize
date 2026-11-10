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


import subprocess

import pytest
from dbt.adapters.materialize.impl import MaterializeAdapter


@pytest.mark.parametrize(
    "error",
    [
        FileNotFoundError(2, "No such file or directory", "git"),
        subprocess.CalledProcessError(128, ["git", "rev-parse", "HEAD"]),
    ],
)
def test_git_commit_sha_is_none_when_git_fails(monkeypatch, error):
    def run(*args, **kwargs):
        raise error

    monkeypatch.setattr(subprocess, "run", run)
    assert MaterializeAdapter.get_git_commit_sha() is None
