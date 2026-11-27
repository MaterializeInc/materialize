# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import runpy
from unittest.mock import Mock

import psycopg
import pytest

from materialize import MZ_ROOT
from materialize import cluster_spec_sheet_ddl as ddl
from materialize.cluster_spec_sheet_ddl import DdlMeasurement, ddl_statements


class Connection:
    def __init__(self, fail=False):
        self.statements = []
        self.fail = fail
        self.closed = False

    def execute(self, statement):
        text = statement if isinstance(statement, str) else statement.as_string()
        self.statements.append(text)
        if self.fail and text.startswith("CREATE"):
            raise psycopg.OperationalError("lost response")

    def close(self):
        self.closed = True


def test_batches_use_persistent_connections_and_record_every_statement():
    connections = []

    def connect():
        connection = Connection()
        connections.append(connection)
        return connection

    events = []
    driver = DdlMeasurement(Mock(side_effect=connect), 4, "probe", "c", events.append)
    try:
        created = driver.run("table", "create", slots=3)
        dropped = driver.run("table", "drop", slots=3)
    finally:
        driver.close()
    assert len(connections) == 4
    for result in (created, dropped):
        assert result["successful_statements"] == 12
        assert result["errors"] == 0
        assert len(result["samples"]) == 12
        assert result["elapsed_seconds"] >= max(
            sample["start_offset_seconds"] + sample["latency_seconds"]
            for sample in result["samples"]
        )
    assert all(c.closed and len(c.statements) == 7 for c in connections)
    creates = [q for c in connections for q in c.statements if q.startswith("CREATE")]
    assert len(set(creates)) == 12


def test_failed_ddl_is_reported_without_retry():
    connection = Connection(fail=True)
    events = []
    driver = DdlMeasurement(
        Mock(return_value=connection), 1, "probe", "c", events.append
    )
    try:
        with pytest.raises(RuntimeError, match="1 errors"):
            driver.run("view", "cycle", duration=1)
    finally:
        driver.close()
    assert len(connection.statements) == 2  # Session setup and one failed CREATE.
    assert events[-1]["errors"] == 1
    assert events[-1]["successful_statements"] == 0
    assert events[-1]["samples"][0]["error"] == "OperationalError"


def test_index_name_is_unqualified_and_identifiers_are_quoted():
    create, drop = ddl_statements("index", 's"x', "cluster", 'i"x')
    assert create.as_string() == (
        'CREATE INDEX "i""x" IN CLUSTER "cluster" ON "s""x"."base" (a)'
    )
    assert drop.as_string() == 'DROP INDEX "s""x"."i""x"'


def test_index_clients_use_independent_tables_and_single_slots():
    connections = [Connection(), Connection()]
    available = iter(connections)
    driver = DdlMeasurement(
        Mock(side_effect=available), 2, "probe", "c", lambda _: None
    )
    try:
        with pytest.raises(ValueError, match="one slot"):
            driver.run("index", "create", slots=2)
        driver.run("index", "create")
        driver.run("index", "drop")
    finally:
        driver.close()
    for worker, connection in enumerate(connections):
        assert connection.statements[1] == (
            f'CREATE INDEX "probe_{worker}_0" IN CLUSTER "c" '
            f'ON "probe"."index_base_{worker}" (a)'
        )
        assert connection.statements[2] == f'DROP INDEX "probe"."probe_{worker}_0"'


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_fixed_rate_accounts_for_busy_slots_and_drain():
    clock = Clock()
    events = []
    ddl.fixed_rate_probe(
        lambda: clock.sleep(0.75),
        events.append,
        start=100,
        duration=1,
        hz=4,
        clock=clock,
        sleep=clock.sleep,
    )
    assert [e["status"] for e in events] == ["ok", "missed", "missed", "ok"]
    assert [e["scheduled_offset_seconds"] for e in events] == [0, 0.25, 0.5, 0.75]
    assert events[-1]["completion_offset_seconds"] == 1.5
    assert events[-1]["service_latency_seconds"] == 0.75
    assert events[-1]["scheduled_latency_seconds"] == 0.75


def test_fixed_rate_records_dispatch_delay_and_missed_wakeups():
    clock = Clock()
    events = []

    def sleep(seconds):
        clock.sleep(seconds + 0.125)

    ddl.fixed_rate_probe(
        lambda: clock.sleep(0.03125),
        events.append,
        start=100,
        duration=0.5,
        hz=4,
        clock=clock,
        sleep=sleep,
    )
    assert events[0]["service_latency_seconds"] == 0.03125
    assert events[0]["scheduled_latency_seconds"] == 0.15625
    clock = Clock()
    events = []
    ddl.fixed_rate_probe(
        Mock(side_effect=AssertionError("must not dispatch")),
        events.append,
        start=99,
        duration=1,
        hz=4,
        clock=clock,
        sleep=clock.sleep,
    )
    assert len(events) == 4
    assert all(e["status"] == "missed" for e in events)


def test_probe_errors_are_recorded_before_propagating():
    clock = Clock()
    events = []
    with pytest.raises(RuntimeError, match="2 errors"):
        ddl.fixed_rate_probe(
            Mock(side_effect=psycopg.OperationalError("lost response")),
            events.append,
            start=100,
            duration=0.5,
            hz=4,
            clock=clock,
            sleep=clock.sleep,
        )
    assert all(e["error"] == "OperationalError" for e in events)
    assert all("completion_offset_seconds" in e for e in events)


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "-1", "bad"])
def test_responsiveness_duration_rejects_invalid_values(value):
    with pytest.raises(ValueError):
        ddl.responsiveness_duration(value)


def test_responsiveness_conditions_do_not_expand_ddl_concurrencies():
    assert ddl.responsiveness_conditions([1, 8, 32]) == [
        ("probes_only", 0),
        ("view_load", 1),
        ("view_load_replan", 1),
        ("view_load", 8),
        ("view_load_replan", 8),
    ]
    assert ddl.responsiveness_conditions([32]) == [("probes_only", 0)]


class ProbeConnection(Connection):
    def execute(self, statement):
        super().execute(statement)
        text = self.statements[-1]
        if text.startswith("SHOW CREATE"):
            rows = [("read_table", "CREATE TABLE read_table (a int)")]
        elif text.startswith("SELECT"):
            rows = [(1024, 524800)]
        elif text.startswith("EXPLAIN PHYSICAL"):
            rows = [("ReadIndex read_primary_idx",)]
        else:
            rows = [("plan",)]
        return Mock(fetchall=Mock(return_value=rows))


@pytest.mark.parametrize("fail", [False, True])
def test_responsiveness_events_and_connections_are_isolated(monkeypatch, fail):
    connections = []

    def connect():
        conn = ProbeConnection()
        connections.append(conn)
        return conn

    class Driver:
        def __init__(self, connect, concurrency, schema, cluster, emit):
            self.emit = emit
            self.conn = connect()

        def run(self, *args, **kwargs):
            self.emit({"event": "phase_start", "kind": "view"})
            self.emit({"event": "phase_end", "kind": "view", "errors": int(fail)})
            if fail:
                raise RuntimeError("DDL failure")

        def close(self):
            self.conn.close()

    def probe(execute, emit, **kwargs):
        execute()
        emit({"status": "ok"})

    monkeypatch.setattr(ddl, "DdlMeasurement", Driver)
    monkeypatch.setattr(ddl, "fixed_rate_probe", probe)
    events = []

    def run():
        ddl.run_responsiveness(
            connect, "probe", "c", 20, "view_load_replan", 8, events.append
        )

    if fail:
        with pytest.raises(RuntimeError, match="DDL failure"):
            run()
    else:
        run()
    assert len(connections) == 4
    assert all(c.closed for c in connections)
    assert all(c.autocommit and c.prepare_threshold is None for c in connections[:3])
    assert all(e["event"].startswith("responsiveness_") for e in events)
    assert all(
        e["condition"] == "view_load_replan" and e["concurrency"] == 8 for e in events
    )
    assert events[-1]["failed"] == fail
    assert {
        e.get("probe") for e in events if e["event"] == "responsiveness_sample"
    } == {
        "show_create",
        "indexed_select",
        "replan",
    }


def test_probe_setup_failure_closes_connections():
    conn = ProbeConnection()
    conn.execute = Mock(side_effect=psycopg.OperationalError("setup failed"))
    with pytest.raises(psycopg.OperationalError, match="setup failed"):
        ddl.run_responsiveness(lambda: conn, "probe", "c", 20, "probes_only", 0, Mock())
    assert conn.closed


@pytest.fixture(scope="module")
def harness(tmp_path_factory):
    from materialize.mzcompose import loader

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            loader, "composition_path", tmp_path_factory.mktemp("composition")
        )
        return runpy.run_path(str(MZ_ROOT / "test/cluster-spec-sheet/mzcompose.py"))


def test_default_off_keeps_fixture_unchanged_and_opt_in_counts_fixture(harness):
    for duration in (0, 20):
        scenario = harness["DdlCatalogScalingScenario"](
            "views", [1, 8], 20, 3, responsiveness_seconds=duration
        )
        runner = Mock()
        runner.run_query.return_value = [(12,)]
        scenario.init(runner)
        queries = [c.args[0] for c in runner.run_query.call_args_list]
        assert sum("read_table" in q for q in queries) == (4 if duration else 0)
        assert queries[-1].startswith("SELECT count(*) FROM mz_objects")
        assert scenario.base_objects == 12
        assert "CREATE TABLE ddl_probe.base (a int, b text)" in queries
        assert not any("INSERT INTO ddl_probe.base" in q for q in queries)


@pytest.mark.parametrize(
    "env,cli,expected",
    [(None, [], 0), ("20", [], 20), ("20", ["--ddl-responsiveness-duration", "0"], 0)],
)
def test_responsiveness_cli_defaults_and_override(
    harness, monkeypatch, env, cli, expected
):
    if env is None:
        monkeypatch.delenv("CI_DDL_RESPONSIVENESS_DURATION", raising=False)
    else:
        monkeypatch.setenv("CI_DDL_RESPONSIVENESS_DURATION", env)

    class Parsed(Exception):
        pass

    class Parser(argparse.ArgumentParser):
        def parse_args(self):
            args = super().parse_args(["--target", "docker", *cli])
            assert args.ddl_responsiveness_duration == expected
            raise Parsed()

    with pytest.raises(Parsed):
        harness["workflow_default"](Mock(), Parser())


@pytest.mark.parametrize(
    "env,cli", [("nan", []), (None, ["--ddl-responsiveness-duration", "-1"])]
)
def test_responsiveness_cli_rejects_invalid_duration(harness, monkeypatch, env, cli):
    if env is not None:
        monkeypatch.setenv("CI_DDL_RESPONSIVENESS_DURATION", env)

    class Parser(argparse.ArgumentParser):
        def parse_args(self):
            return super().parse_args(["--target", "docker", *cli])

    with pytest.raises(SystemExit) as error:
        harness["workflow_default"](Mock(), Parser())
    assert error.value.code == 2


@pytest.mark.parametrize("duration,size", [(0, 5000), (20, 5000), (20, 1000)])
def test_responsiveness_runs_only_after_normal_matrix(
    harness, monkeypatch, tmp_path, duration, size
):
    scenario = harness["DdlCatalogScalingScenario"](
        "views",
        [1, 8],
        20,
        3,
        tail_size=size,
        tail_duration=30,
        responsiveness_seconds=duration,
    )
    scenario.record_configuration = Mock()
    runner = Mock(scale=size)
    runner.run_query.side_effect = [[("view", size)], [(0,)]]
    calls = []

    class Driver:
        def __init__(self, *args):
            pass

        def run(self, kind, phase, **kwargs):
            calls.append((kind, phase, kwargs))
            return {
                "samples": [
                    {"operation": op, "latency_seconds": 1} for op in ("create", "drop")
                ]
            }

        def close(self):
            pass

    namespace = scenario.run.__func__.__globals__
    monkeypatch.setitem(namespace, "DdlMeasurement", Driver)
    monkeypatch.setitem(
        namespace,
        "run_responsiveness",
        lambda *args: calls.append(("responsiveness", args[4:6])),
    )
    monkeypatch.setattr(namespace["time"], "sleep", lambda _: None)
    monkeypatch.setattr(namespace["buildkite"], "is_in_buildkite", lambda: False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "test/cluster-spec-sheet").mkdir(parents=True)
    scenario.run(runner)
    # Six kind/concurrency pairs, each with warmup, three batches/cycles and tail.
    assert len(calls[:66]) == 66
    assert all(call[0] != "responsiveness" for call in calls[:66])
    assert len(calls) == (81 if duration and size == 5000 else 66)
    assert runner.add_result.call_count == 36
    assert scenario.record_configuration.call_args.args[-1] == "after-measurement"
