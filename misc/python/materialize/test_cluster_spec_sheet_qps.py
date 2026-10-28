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
import json
import sys
from types import SimpleNamespace
from urllib.parse import urlencode

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


@pytest.mark.parametrize(
    "options",
    [
        [],
        ["-c statement_logging_sample_rate=0"],
        [r"-c application_name=quote'and\\slash\ with\ spaces&plus+percent%"],
        [
            "-c cluster=other -c statement_timeout=1",
            "-c statement_logging_sample_rate=0.5",
        ],
    ],
)
def test_connection_routing_and_escaping(options):
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
        urlencode(
            {"sslmode": "require", "cluster": "c", "options": options}, doseq=True
        ),
    ]
    config = QpsSweep([1, 8], ["prepared"], clusters=3).config(flags, 8, "prepared")
    assert len(config["libpq_envs"]) == 3
    for i, env in enumerate(config["libpq_envs"]):
        cluster = "c" if i == 0 else f"qps_{i}"
        expected = " ".join(
            [*options, f"-c cluster={cluster} -c statement_timeout=30000"]
        )
        assert env["PGOPTIONS"] == expected
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
    workload.measure = lambda runner, size, concurrency, protocol, disable_statement_logging: measured.append(
        (runner.envd_cpus, concurrency, protocol, size, disable_statement_logging)
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
    assert measured == [
        (cpus, concurrency, protocol, "scale=1,workers=1", disabled)
        for cpus in (1, 2, 4, 8, 16, 32)
        for disabled in (False, True)
        for protocol in ("prepared", "simple")
        for concurrency in (1, 4, 16)
    ]
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
    assert measured == [(True, 10)] * 4
    assert runner.clusters == {"c"}


@pytest.mark.parametrize("disable_statement_logging", [False, True])
def test_pgbench_measurement_records_max_latency(
    monkeypatch, tmp_path, disable_statement_logging
):
    monkeypatch.chdir(tmp_path)
    from materialize.mzcompose import loader

    monkeypatch.setattr(loader, "composition_path", tmp_path)
    module = composition_module()
    monkeypatch.setenv("BUILDKITE", "true")
    monkeypatch.setattr(module.buildkite, "is_in_buildkite", lambda: False)
    ci_calls = []
    monkeypatch.setattr(
        module.buildkite,
        "upload_artifact",
        lambda *args, **kwargs: ci_calls.append("upload"),
    )
    monkeypatch.setattr(
        module.buildkite,
        "add_annotation",
        lambda *args, **kwargs: ci_calls.append("annotate"),
    )
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
        statement_logging_sample_rates=[0 if disable_statement_logging else 0.99],
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
        urlencode({"sslmode": "require", "options": "-c application_name=benchmark"}),
    ]
    runner = SimpleNamespace(
        envd_cpus=2,
        target=SimpleNamespace(
            composition=SimpleNamespace(), qps_connection_flags=lambda: flags
        ),
        run_query=lambda *args, **kwargs: [(None, 0)],
        add_result=lambda *args, **kwargs: results.append((args, kwargs)),
    )
    module.QpsEnvdStrongScalingScenario(
        QpsSweep([1], ["prepared"], clusters=1)
    ).measure(runner, "50cc", 1, "prepared", disable_statement_logging)
    config, composition = calls[0]
    assert composition is runner.target.composition
    expected_options = "-c application_name=benchmark"
    if disable_statement_logging:
        expected_options += " -c statement_logging_sample_rate=0"
    assert config["libpq_envs"][0]["PGOPTIONS"] == (
        expected_options + " -c cluster=c -c statement_timeout=30000"
    )
    assert flags[-1] == urlencode(
        {"sslmode": "require", "options": "-c application_name=benchmark"}
    )
    suffix = "_logging_disabled" if disable_statement_logging else ""
    assert results[0][0][:2] == (
        f"peek_qps{suffix}",
        f"qps_prepared_1_conns_1_clusters{suffix}",
    )
    assert results[0][1]["qps_details"]["max_latency_ms"] == 30
    assert results[0][1]["qps_details"]["driver_peak_thread_cpu_percent"] == 2
    assert "max_latency_ms" in module.ENVD_FIELDNAMES
    artifact = next((tmp_path / "test/cluster-spec-sheet/qps-logs").glob("*.json"))
    assert json.loads(artifact.read_text())["max_latency_ms"] == 30
    assert artifact.name == f"2_qps_prepared_1_conns_1_clusters{suffix}.json"
    if disable_statement_logging:
        measured["statement_logging_sample_rates"] = [0.99]
        with pytest.raises(ValueError, match="logging.*disabled"):
            module.QpsEnvdStrongScalingScenario(
                QpsSweep([1], ["prepared"], clusters=1)
            ).measure(runner, "50cc", 1, "prepared", True)
        assert len(results) == 1
    assert ci_calls == []


@pytest.mark.parametrize("protocol", ["prepared", "simple"])
def test_concurrency_plots(monkeypatch, tmp_path, protocol):
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
        for category in ("peek_qps", "peek_qps_logging_disabled"):
            for cpus in [1, 2]:
                for concurrency in [1, 8]:
                    writer.writerow(
                        dict(
                            scenario="qps_envd_strong_scaling",
                            category=category,
                            mode="strong",
                            envd_cpus=cpus,
                            concurrency=concurrency,
                            protocol=protocol,
                            qps=100 * concurrency,
                            mean_latency_ms=10,
                            p99_latency_ms=12,
                            max_latency_ms=18,
                        )
                    )
    module.analyze_envd_results_file(str(path))
    plots = list((tmp_path / "test/cluster-spec-sheet/plots/sweep").glob("*.png"))
    assert len(plots) == 8
    assert sum("peek_qps_logging_disabled" in p.name for p in plots) == 4
    assert any(p.name.endswith("_max_latency_ms.png") for p in plots)
    assert all(p.stat().st_size > 0 for p in plots)
    assert all(f"_{protocol}_" in p.name for p in plots)
    assert set(uploaded) == {str(p.relative_to(tmp_path)) for p in plots}


def test_historical_results_keep_cpu_axis_plots(monkeypatch, tmp_path):
    from materialize.mzcompose import loader

    monkeypatch.setattr(loader, "composition_path", tmp_path)
    module = composition_module()
    plotted = []
    monkeypatch.setattr(module, "plot", lambda *args, **kwargs: plotted.append(kwargs))
    path = tmp_path / "historical.envd.csv"
    with path.open("w") as f:
        writer = csv.DictWriter(
            f, fieldnames=["scenario", "category", "mode", "envd_cpus", "qps"]
        )
        writer.writeheader()
        writer.writerow(
            dict(
                scenario="qps_envd_strong_scaling",
                category="peek_qps",
                mode="strong",
                envd_cpus=1,
                qps=100,
            )
        )
    module.analyze_envd_results_file(str(path))
    assert plotted == [{"x": "envd_cpus"}]


def test_logging_variants_are_uploaded_separately(monkeypatch, tmp_path):
    from materialize.mzcompose import loader

    monkeypatch.setattr(loader, "composition_path", tmp_path)
    module = composition_module()
    monkeypatch.setattr(module.buildkite, "is_in_buildkite", lambda: True)
    monkeypatch.setattr(module, "create_test_analytics_config", lambda _: None)
    uploaded = []
    database = SimpleNamespace(
        builds=SimpleNamespace(add_build_job=lambda **kwargs: None),
        cluster_spec_sheet_environmentd_results=SimpleNamespace(
            add_result=lambda **kwargs: uploaded.extend(kwargs["results"])
        ),
        submit_updates=lambda: None,
    )
    monkeypatch.setattr(module, "TestAnalyticsDb", lambda _: database)
    path = tmp_path / "sweep.envd.csv"
    with path.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=module.ENVD_FIELDNAMES)
        writer.writeheader()
        for suffix, qps in (("", 100), ("_logging_disabled", 200)):
            writer.writerow(
                dict(
                    scenario="qps_envd_strong_scaling",
                    scenario_version="3.0.0",
                    scale=1,
                    mode="strong",
                    category=f"peek_qps{suffix}",
                    test_name=f"qps_prepared_512_conns_32_clusters{suffix}",
                    envd_cpus=32,
                    repetition=0,
                    qps=qps,
                )
            )
    module.upload_environmentd_results_to_test_analytics(None, str(path), True)
    assert [(r.category, r.test_name, r.envd_cpus, r.qps) for r in uploaded] == [
        ("peek_qps", "qps_prepared_512_conns_32_clusters", 32, 100),
        (
            "peek_qps_logging_disabled",
            "qps_prepared_512_conns_32_clusters_logging_disabled",
            32,
            200,
        ),
    ]


def test_cluster_plots_match_recorded_results(monkeypatch, tmp_path):
    # The fixture is a slice of a real `.cluster.csv` run. It mixes `cc` and
    # `M.1` sizes, has repetitions, and leaves `size_bytes` empty for
    # materialized view scenarios.
    monkeypatch.chdir(tmp_path)
    from materialize.mzcompose import loader

    monkeypatch.setattr(loader, "composition_path", tmp_path)
    module = composition_module()
    monkeypatch.setattr(module, "MZ_ROOT", tmp_path)
    monkeypatch.setattr(module, "upload_file", lambda paths, title: None)
    saved = {}
    save_plot = module.save_plot

    def record(plot_dir, data_frame, title, slug):
        saved[slug] = data_frame.copy()
        save_plot(plot_dir, data_frame, title, slug)

    monkeypatch.setattr(module, "save_plot", record)
    path = tmp_path / "recorded.cluster.csv"
    path.write_bytes(
        (
            MZ_ROOT / "misc/python/materialize/testdata/cluster_spec_sheet.cluster.csv"
        ).read_bytes()
    )
    module.analyze_cluster_results_file(str(path))

    # Recompute the expected minimum per plot cell without pandas.
    expected: dict[str, dict[tuple[float, str], float]] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            slug = f"{row['scenario']}_{row['category']}_{row['mode']}"
            credits_per_h = module.extract_cluster_size(row["cluster_size"])
            time_ms = float(row["time_ms"])
            credit_time = credits_per_h / 3600 * 100 * time_ms / 1000
            for suffix, value in (("time_ms", time_ms), ("credits", credit_time)):
                cells = expected.setdefault(f"{slug}_{suffix}", {})
                key = (credits_per_h, row["test_name"])
                cells[key] = min(cells.get(key, value), value)

    assert set(saved) == set(expected) | {f"{s}_normalized" for s in expected}
    for slug, cells in expected.items():
        frame = saved[slug]
        assert list(frame.index) == sorted({c for c, _ in cells})
        assert list(frame.columns) == sorted({t for _, t in cells})
        actual = {
            (credits_per_h, test_name): value
            for credits_per_h, row in frame.iterrows()
            for test_name, value in row.items()
        }
        assert actual == pytest.approx(cells)
        normalized = saved[f"{slug}_normalized"]
        assert list(normalized.iloc[0]) == pytest.approx([1.0] * len(frame.columns))
        assert normalized.to_numpy() == pytest.approx(
            (frame / frame.iloc[0]).to_numpy()
        )

    plot_dir = tmp_path / "test/cluster-spec-sheet/plots/recorded"
    for slug in saved:
        for ext in ("svg", "html"):
            assert (plot_dir / f"{slug}.{ext}").stat().st_size > 0
