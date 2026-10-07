# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import importlib.util
import json
import sys
from types import SimpleNamespace

import pytest

from materialize import MZ_ROOT
from materialize.cluster_spec_sheet_qps import (
    QPS_CONCURRENCIES,
    QpsSweep,
    validate_qps_result,
)


def test_sweep_defaults():
    sweep = QpsSweep(QPS_CONCURRENCIES, ["prepared"])
    assert sum(sweep.duration + sweep.warmup for _ in sweep.concurrencies) * 6 == 1500
    assert sweep.clusters == 32


@pytest.mark.parametrize(
    "kwargs",
    [
        {"clusters": 0},
        {"duration": 0},
        {"warmup": -1},
        {"query_timeout": 0},
        {"duration": float("nan")},
        {"warmup": float("inf")},
    ],
)
def test_invalid_sweep(kwargs):
    with pytest.raises(ValueError):
        QpsSweep([1], ["prepared"], **kwargs)


def test_connection_routing_and_escaping():
    flags = [
        "-driver",
        "postgres",
        "-host",
        "host",
        "-port",
        "6875",
        "-username",
        "user",
        "-password",
        "quote'and\\slash",
        "-database",
        "materialize",
        "-params",
        "sslmode=require&cluster=c",
    ]
    config = QpsSweep([1, 8], ["prepared"], clusters=3).config(flags, 8, "prepared")
    assert len(config["libpq_envs"]) == 3
    for i, env in enumerate(config["libpq_envs"]):
        cluster = "c" if i == 0 else f"qps_{i}"
        assert env["PGOPTIONS"] == f"-c cluster={cluster} -c statement_timeout=30000"
        assert config["cluster_names"][i] == cluster
        assert env["PGSSLMODE"] == "require"
        assert env["PGPASSWORD"] == "quote'and\\slash"
        assert env["PGHOST"] == "host"
        assert env["PGDATABASE"] == "materialize"
    assert config["query"] == "SELECT * FROM qps_gen_view WHERE x = 5"
    assert config["expected_rows"] == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("errors", 1),
        ("queries", 0),
        ("qps", 0),
        ("protocol", "simple"),
        ("concurrency", 8),
        ("max_latency_ms", 59),
        ("max_latency_ms", float("nan")),
        ("p99_latency_ms", float("inf")),
    ],
)
def test_invalid_result(field, value):
    result = dict(
        concurrency=1,
        protocol="prepared",
        errors=0,
        queries=100,
        qps=20,
        mean_latency_ms=50,
        p50_latency_ms=45,
        p95_latency_ms=55,
        p99_latency_ms=60,
        max_latency_ms=70,
    )
    validate_qps_result(result, 1, "prepared")
    result[field] = value
    with pytest.raises(ValueError):
        validate_qps_result(result, 1, "prepared")


def composition_module():
    name = "cluster_spec_sheet_mzcompose_test"
    spec = importlib.util.spec_from_file_location(
        name, MZ_ROOT / "test/cluster-spec-sheet/mzcompose.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_each_cpu_runs_full_concurrency_sweep(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    from materialize.mzcompose import loader

    monkeypatch.setattr(loader, "composition_path", tmp_path)
    module = composition_module()
    options = QpsSweep([1, 4, 16], ["prepared", "simple"], clusters=2)
    services = {service.name: service.config for service in module.SERVICES}
    assert services["qps-pgbench"]["mzbuild"] == "postgres"
    assert "qpsbench" not in services
    assert "dbbench" not in services
    workload = module.QpsEnvdStrongScalingScenario(options)
    sweep = module.EnvdCpuSweep(workload.name(), workload)
    target = module.DockerTarget(None)
    measured = []
    workload.measure = lambda runner, size, concurrency, protocol: measured.append(
        (runner.envd_cpus, concurrency, protocol, size)
    )

    class Runner:
        envd_cpus = None

        def __init__(self):
            self.target = target
            self.sql = []

        def run_query(self, query, **kwargs):
            self.sql.append(query)

    runner = Runner()
    for point in sweep.scale_points(target, 32):
        runner.envd_cpus = point.envd_cpus
        sweep.measure(runner, point)
    assert len(measured) == 6 * 3 * 2
    assert measured[0] == (1, 1, "prepared", "scale=1,workers=1")
    assert measured[-1] == (32, 16, "simple", "scale=1,workers=1")
    assert sum("CREATE CLUSTER qps_" in q for q in runner.sql) == 6
    assert [q for q in runner.sql if q.startswith("CREATE CLUSTER qps_")] == [
        f"CREATE CLUSTER qps_{i} SIZE 'scale=1,workers=1', REPLICATION FACTOR 1"
        for _ in range(6)
        for i in range(1, 2)
    ]
    assert sum("DROP CLUSTER IF EXISTS qps_" in q for q in runner.sql) == 6


def test_ten_query_clusters_fit_ten_cluster_account(monkeypatch, tmp_path):
    from materialize.mzcompose import loader

    monkeypatch.setattr(loader, "composition_path", tmp_path)
    module = composition_module()
    workload = module.QpsEnvdStrongScalingScenario(
        QpsSweep([1, 512], ["prepared"], clusters=10)
    )

    class Runner:
        target = object.__new__(module.CloudTarget)
        clusters = {"c", "quickstart"}
        resized = False

        def run_query(self, query, **kwargs):
            if query.startswith("CREATE CLUSTER"):
                assert len(self.clusters) < 10
                self.clusters.add(query.split()[2])
            elif query.startswith("DROP CLUSTER IF EXISTS"):
                self.clusters.discard(query.split()[4])
            elif query.startswith("ALTER CLUSTER c SET"):
                assert (
                    query == "ALTER CLUSTER c SET (SIZE '50cc', REPLICATION FACTOR 1)"
                )
                self.resized = True

    runner = Runner()
    measured = []
    workload.measure = lambda *args: measured.append(
        (runner.resized, len(runner.clusters))
    )
    workload.run(runner)
    assert measured == [(True, 10), (True, 10)]
    assert runner.clusters == {"c"}


def test_pgbench_measurement_records_max_latency(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    from materialize.mzcompose import loader

    monkeypatch.setattr(loader, "composition_path", tmp_path)
    module = composition_module()
    measured = dict(
        concurrency=1,
        protocol="prepared",
        clusters=1,
        errors=0,
        queries=200,
        qps=10,
        mean_latency_ms=5,
        p50_latency_ms=4,
        p95_latency_ms=6,
        p99_latency_ms=8,
        max_latency_ms=30,
        started_at="2026-01-01T00:00:00+00:00",
        finished_at="2026-01-01T00:00:20+00:00",
        driver=dict(
            warnings=[],
            cpu_percent=1,
            throttled_period_percent=0,
            peak_thread_cpu_percent=2,
        ),
    )
    calls, results = [], []

    def run(config, composition):
        calls.append((config, composition))
        return measured

    monkeypatch.setattr(module.pgbench, "run", run)

    flags = [
        "-host",
        "host",
        "-port",
        "6875",
        "-username",
        "user",
        "-database",
        "materialize",
        "-params",
        "sslmode=require",
    ]
    runner = SimpleNamespace(
        envd_cpus=2,
        target=SimpleNamespace(
            composition=SimpleNamespace(), qps_connection_flags=lambda: flags
        ),
        run_query=lambda *args, **kwargs: [(None, 0)],
        add_result=lambda *args, **kwargs: results.append(kwargs),
    )
    module.QpsEnvdStrongScalingScenario(
        QpsSweep([1], ["prepared"], clusters=1)
    ).measure(runner, "50cc", 1, "prepared")
    config, composition = calls[0]
    assert composition is runner.target.composition
    assert config["libpq_envs"][0]["PGOPTIONS"].startswith("-c cluster=c ")
    assert results[0]["qps_details"]["max_latency_ms"] == 30
    assert results[0]["qps_details"]["driver_peak_thread_cpu_percent"] == 2
    assert "max_latency_ms" in module.ENVD_FIELDNAMES
    artifact = next((tmp_path / "test/cluster-spec-sheet/qps-logs").glob("*.json"))
    assert json.loads(artifact.read_text())["max_latency_ms"] == 30
