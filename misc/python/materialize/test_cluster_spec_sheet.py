# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import csv
import importlib.util
import sys

import pytest

from materialize import MZ_ROOT
from materialize.cluster_spec_sheet import (
    QPS_CONCURRENCIES,
    QpsSweep,
    validate_qps_result,
)


def pgbench_module():
    spec = importlib.util.spec_from_file_location(
        "pgbench_spike", MZ_ROOT / "test/pgbench/run.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_pgbench_allocation():
    module = pgbench_module()
    assert module.allocation(1, 32) == [1]
    assert module.allocation(8, 3) == [3, 3, 2]
    assert module.allocation(512, 32) == [16] * 32


def test_pgbench_common_window_and_pooled_percentiles(tmp_path):
    module = pgbench_module()
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_text("0 1 100 0 1 0\n0 2 100 0 3 0\n0 3 100 0 5 0\n0 4 100 0 7 0\n")
    b.write_text("0 1 10000 0 2 0\n0 2 10000 0 4 0\n0 3 10000 0 6 0\n0 4 10000 0 8 0\n")
    result = module.merge_logs([a, b], 2, 1, 3)
    assert result["queries"] == 3
    assert result["qps"] == 1
    assert result["mean_latency_ms"] == pytest.approx(3.4)
    assert result["p99_latency_ms"] == 10
    with pytest.raises(ValueError, match="common full"):
        module.merge_logs([a, b], 2, 1, 20)
    with pytest.raises(ValueError, match="every client"):
        module.merge_logs([a, b], 3, 1, 3)


def test_pgbench_failures_are_not_successful_samples(tmp_path):
    module = pgbench_module()
    path = tmp_path / "failed"
    path.write_text("0 1 failed 0 1 0\n")
    with pytest.raises(ValueError, match="failed"):
        list(module.records(path))
    with pytest.raises(ValueError, match="zero failed"):
        module.summary("number of failed transactions: 1 (1%)")


def test_pgbench_libpq_options():
    flags = [
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
        "sslmode=require",
    ]
    config = QpsSweep([1], ["prepared"], clusters=2).config(flags, 1, "prepared")
    envs = config["libpq_envs"]
    assert envs[0]["PGPASSWORD"] == "quote'and\\slash"
    assert envs[0]["PGDATABASE"] == "materialize"
    assert envs[0]["PGHOST"] == "host"
    first_options, second_options = envs[0]["PGOPTIONS"], envs[1]["PGOPTIONS"]
    assert "-c cluster=c " in first_options
    assert "-c cluster=qps_1 " in second_options
    assert "statement_logging_sample_rate=0" in first_options


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
    assert len(config["dsns"]) == 3
    for i, dsn in enumerate(config["dsns"]):
        cluster = "c" if i == 0 else f"qps_{i}"
        assert f"cluster='{cluster}'" in dsn
        assert "sslmode='require'" in dsn
        assert "statement_logging_sample_rate='0'" in dsn
        assert "password='quote\\'and\\\\slash'" in dsn
    assert config["query"] == "SELECT * FROM qps_gen_view WHERE x = 5"
    assert config["expected_rows"] == 1
    assert config["require_statement_logging_disabled"] is True


@pytest.mark.parametrize(
    "field,value",
    [
        ("errors", 1),
        ("queries", 0),
        ("qps", 0),
        ("protocol", "simple"),
        ("concurrency", 8),
        ("statement_logging_sample_rate", 0.99),
        ("statement_logging_sample_rate", None),
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
        p99_latency_ms=60,
        statement_logging_sample_rate=0,
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


@pytest.mark.parametrize("cpu_scales", [None, [32]])
def test_each_cpu_runs_full_concurrency_sweep(monkeypatch, tmp_path, cpu_scales):
    monkeypatch.chdir(tmp_path)
    from materialize.mzcompose import loader

    monkeypatch.setattr(loader, "composition_path", tmp_path)
    module = composition_module()
    options = QpsSweep([1, 4, 16], ["prepared", "simple"], clusters=2)
    workload = module.QpsEnvdStrongScalingScenario(options)
    sweep = module.EnvdCpuSweep(workload.name(), workload, cpu_scales)
    expected_cpus = [1, 2, 4, 8, 16, 32] if cpu_scales is None else cpu_scales
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
    assert len(measured) == len(expected_cpus) * 3 * 2
    assert measured[0] == (expected_cpus[0], 1, "prepared", "scale=1,workers=1")
    assert measured[-1] == (32, 16, "simple", "scale=1,workers=1")
    assert sum("CREATE CLUSTER qps_" in q for q in runner.sql) == len(expected_cpus)
    assert [q for q in runner.sql if q.startswith("CREATE CLUSTER qps_")] == [
        f"CREATE CLUSTER qps_{i} SIZE 'scale=1,workers=1', REPLICATION FACTOR 1"
        for _ in expected_cpus
        for i in range(1, 2)
    ]
    assert sum("DROP CLUSTER IF EXISTS qps_" in q for q in runner.sql) == len(
        expected_cpus
    )


@pytest.mark.parametrize("cpu_scales", [[], [0], [-1], [32, 16], [16, 16]])
def test_invalid_cpu_selection(monkeypatch, tmp_path, cpu_scales):
    from materialize.mzcompose import loader

    monkeypatch.setattr(loader, "composition_path", tmp_path)
    module = composition_module()
    workload = module.QpsEnvdStrongScalingScenario(QpsSweep([128, 512], ["prepared"]))
    with pytest.raises(ValueError):
        module.EnvdCpuSweep(workload.name(), workload, cpu_scales)


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


def test_concurrency_plots(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    from materialize.mzcompose import loader

    monkeypatch.setattr(loader, "composition_path", tmp_path)
    module = composition_module()
    uploaded = []
    monkeypatch.setattr(
        module, "upload_file", lambda paths, title: uploaded.extend(paths)
    )
    path = tmp_path / "sweep.envd.csv"
    with path.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=module.ENVD_FIELDNAMES)
        writer.writeheader()
        for cpus in [1, 2]:
            for concurrency in [1, 8]:
                writer.writerow(
                    dict(
                        scenario="qps_envd_strong_scaling",
                        category="peek_qps",
                        mode="strong",
                        envd_cpus=cpus,
                        concurrency=concurrency,
                        protocol="prepared",
                        qps=100 * concurrency,
                        mean_latency_ms=10,
                        p99_latency_ms=12,
                    )
                )
    module.analyze_envd_results_file(str(path))
    plots = list((tmp_path / "test/cluster-spec-sheet/plots/sweep").glob("*.png"))
    assert len(plots) == 3
    assert all(p.stat().st_size > 0 for p in plots)
    assert set(uploaded) == {str(p.relative_to(tmp_path)) for p in plots}
