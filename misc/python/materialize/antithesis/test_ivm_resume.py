# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from contextlib import nullcontext
from dataclasses import replace
from typing import cast

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import ivm_model as model
from materialize.antithesis.drivers import ivm


@pytest.fixture
def database():
    with sqlite3.connect(":memory:") as db:
        db.execute(
            "CREATE TABLE resume (slot INTEGER PRIMARY KEY, t INTEGER, gen INTEGER, state TEXT)"
        )
        yield db


def test_legacy_resume_is_not_compared_with_an_expanded_union(database):
    old = {"t": Counter({model.pad((1, 0, 5, 1)): 3})}
    database.execute(
        "INSERT INTO resume VALUES (0, 10, 2, ?)",
        (json.dumps(model.encode_state(old)),),
    )
    assert ivm._load_resume(database) is None


@pytest.mark.parametrize("change", ["kind", "definition", "columns", "table"])
def test_resume_is_fenced_by_its_query_and_relation_definitions(
    database, monkeypatch, change
):
    state = {"t": Counter({model.pad((1, 0, 5, 1)): 3})}
    ivm._save_resume(database, 10, 2, state)
    if change == "kind":
        monkeypatch.setattr(model, "VIEWS", model.VIEWS[:-1])
    elif change == "definition":
        first = replace(model.VIEWS[0], sql=model.VIEWS[0].sql + " WHERE false")
        monkeypatch.setattr(model, "VIEWS", (first, *model.VIEWS[1:]))
    elif change == "columns":
        first = replace(model.VIEWS[0], columns=("changed",))
        monkeypatch.setattr(model, "VIEWS", (first, *model.VIEWS[1:]))
    else:
        first = replace(model.TABLES[0], ddl=model.TABLES[0].ddl + ", extra int")
        monkeypatch.setattr(model, "TABLES", (first, *model.TABLES[1:]))
    assert ivm._load_resume(database) is None


def test_current_resume_preserves_missing_output_for_the_oracle(database):
    state = {
        "t": Counter({model.pad((1, 0, 5, 1)): 3}),
        "d": Counter({model.pad((0, 2)): 2}),
        "intersect_all:index": Counter({model.pad((0,)): 2}),
    }
    ivm._save_resume(database, 10, 2, state)
    resumed = ivm._load_resume(database)
    assert resumed is not None
    assert resumed == ivm.Resume(10, 2, state)
    current = {**state, "intersect_all:mv": Counter({model.pad((0,)): 2})}
    assert model.state_diff(resumed.state, current)


def test_empty_current_resume_remains_a_valid_checkpoint(database):
    ivm._save_resume(database, 10, None, {})
    assert ivm._load_resume(database) == ivm.Resume(10, None, {})


def test_warm_fixture_upgrade_adds_new_paths_without_dropping_old_objects(
    monkeypatch, database
):
    new_names = {
        f"{kind}_{suffix}"
        for kind in model.SET_OP_KINDS
        for suffix in ("v", "v_idx", "mv")
    }
    old_names = set(ivm._object_names()) - new_names

    class Connection:
        def __init__(self):
            self.names = set(old_names)
            self.created = set()
            self.row = None

        def execute(self, statement, params=()):
            if isinstance(statement, bytes):
                statement = statement.decode()
            assert "DROP " not in statement
            if statement.startswith("SELECT (SELECT count(*)"):
                self.row = (1, len(self.names & set(params[2])))
            elif statement.startswith("SELECT 1 FROM mz_clusters"):
                self.row = (1,)
            else:
                created = re.search(
                    r"CREATE (?:MATERIALIZED VIEW|INDEX|TABLE|VIEW) IF NOT EXISTS (?:ivm\.)?(\w+)",
                    statement,
                )
                if created:
                    name = created.group(1)
                    if name not in self.names:
                        self.created.add(name)
                    self.names.add(name)
            return self

        def fetchone(self):
            return self.row

    conn = Connection()
    monkeypatch.setattr(ivm, "ensure_retain_history", lambda host: True)
    monkeypatch.setattr(ivm, "timeline_choice", lambda *args: "2m")
    monkeypatch.setattr(ivm, "_connect", lambda *args, **kwargs: nullcontext(conn))
    fake_connection = cast(psycopg.Connection, conn)
    assert not ivm._objects_present(fake_connection)
    assert ivm.ensure_objects("unused", database)
    assert ivm._objects_present(fake_connection)
    assert conn.created == new_names
    assert old_names <= conn.names
