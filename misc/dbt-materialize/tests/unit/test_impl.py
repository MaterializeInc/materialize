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
from types import SimpleNamespace

import psycopg2
import pytest
from dbt.adapters.materialize.impl import MaterializeAdapter

SWAP_SQL = "BEGIN; ALTER SCHEMA a SWAP WITH b; COMMIT;"


# psycopg2 sets `pgcode` from the server response and it is read-only on
# instances, so tests set it on a subclass instead.
class DdlConflictError(psycopg2.Error):
    pgcode = "40001"


class PermissionDeniedError(psycopg2.Error):
    pgcode = "42501"


class FakeCursor:
    def __init__(self, handle):
        self.handle = handle

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self.handle.executed.append(sql)
        error = self.handle.errors.get(sql)
        if error is not None:
            raise error


class FakeHandle:
    def __init__(self, errors=None):
        self.executed = []
        self.errors = errors or {}

    def cursor(self):
        return FakeCursor(self)


def swap_adapter(handle):
    adapter = MaterializeAdapter.__new__(MaterializeAdapter)
    connection = SimpleNamespace(handle=handle)
    adapter.connections = SimpleNamespace(get_thread_connection=lambda: connection)
    return adapter


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


def test_atomic_swap_returns_true_on_success():
    handle = FakeHandle()
    assert swap_adapter(handle).try_atomic_swap(SWAP_SQL) is True
    assert handle.executed == [SWAP_SQL]


def test_atomic_swap_rolls_back_and_returns_false_on_ddl_conflict():
    handle = FakeHandle({SWAP_SQL: DdlConflictError("concurrent DDL")})
    assert swap_adapter(handle).try_atomic_swap(SWAP_SQL) is False
    assert handle.executed == [SWAP_SQL, "ROLLBACK"]


def test_atomic_swap_rolls_back_and_raises_other_errors():
    error = PermissionDeniedError("permission denied")
    handle = FakeHandle({SWAP_SQL: error})
    with pytest.raises(PermissionDeniedError) as raised:
        swap_adapter(handle).try_atomic_swap(SWAP_SQL)
    assert raised.value is error
    assert handle.executed == [SWAP_SQL, "ROLLBACK"]


def test_atomic_swap_returns_false_on_ddl_conflict_when_rollback_fails():
    handle = FakeHandle(
        {SWAP_SQL: DdlConflictError("concurrent DDL"), "ROLLBACK": psycopg2.Error()}
    )
    assert swap_adapter(handle).try_atomic_swap(SWAP_SQL) is False


def test_atomic_swap_raises_the_swap_error_when_rollback_fails():
    handle = FakeHandle(
        {
            SWAP_SQL: PermissionDeniedError("permission denied"),
            "ROLLBACK": psycopg2.Error(),
        }
    )
    with pytest.raises(PermissionDeniedError):
        swap_adapter(handle).try_atomic_swap(SWAP_SQL)
