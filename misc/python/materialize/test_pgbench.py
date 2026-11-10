# Copyright Materialize, Inc. and contributors. All rights reserved.
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from materialize import pgbench as runner


def test_client_allocation():
    assert runner.allocation(1, 32) == [1]
    assert runner.allocation(8, 3) == [3, 3, 2]
    assert runner.allocation(512, 32) == [16] * 32
    with pytest.raises(ValueError):
        runner.allocation(0, 32)


def test_common_window_and_pooled_percentiles(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_text("0 1 90000 0 1 0\n0 2 100 0 3 0\n0 3 100 0 5 0\n0 4 80000 0 7 0\n")
    b.write_text("0 1 70000 0 2 0\n0 2 10000 0 4 0\n0 3 60000 0 6 0\n0 4 50000 0 8 0\n")
    result = runner.merge_logs([a, b], 2, 1, 3)
    assert result["queries"] == 3
    assert result["qps"] == 1
    assert result["mean_latency_ms"] == pytest.approx(3.4)
    assert result["p50_latency_ms"] == 0.1
    assert result["p95_latency_ms"] == 10
    assert result["p99_latency_ms"] == 10
    assert result["max_latency_ms"] == 10
    with pytest.raises(ValueError, match="common full"):
        runner.merge_logs([a, b], 2, 1, 20)
    with pytest.raises(ValueError, match="every client"):
        runner.merge_logs([a, b], 3, 1, 3)


def test_maximum_includes_outlier_beyond_p99(tmp_path):
    path = tmp_path / "transactions"
    path.write_text(
        "0 0 900000 0 1 0\n"
        + "".join(
            f"0 {i} {50000 if i == 200 else 1000} 0 2 {i}\n" for i in range(1, 201)
        )
        + "0 201 800000 0 4 0\n"
    )
    result = runner.merge_logs([path], 1, 1, 1)
    assert result["queries"] == 200
    assert result["p99_latency_ms"] == 1
    assert result["max_latency_ms"] == 50


@pytest.mark.parametrize(
    "line", ["0 1 failed 0 1 0", "0 1 skipped 0 1 0", "0 1 100 1 1 0", "bad log"]
)
def test_invalid_transaction_logs(tmp_path, line):
    path = tmp_path / "failed"
    path.write_text(line + "\n")
    with pytest.raises(ValueError):
        list(runner.records(path))


def test_native_summary():
    text = "number of failed transactions: 0 (0%)\nnumber of transactions actually processed: 10\nlatency average = 2.000 ms\ntps = 500.000 (without initial connection time)\n"
    assert runner.summary(text) == {"queries": 10, "qps": 500, "mean_latency_ms": 2}
    with pytest.raises(ValueError, match="zero failed"):
        runner.summary(text.replace("0 (0%)", "1 (1%)"))
    with pytest.raises(ValueError, match="missing"):
        runner.summary("number of failed transactions: 0 (0%)")


def test_wait_for_every_client_despite_staggered_startup(tmp_path):
    first = {}
    a, b = tmp_path / "transactions-0.123", tmp_path / "transactions-1.456"
    a.write_text("0 1 1000 0 1 0\n1 1 1000 0 2 0\n")
    assert runner.observe_startup(tmp_path, [2, 1], first) is None
    b.write_text("0 1 1000 0 40")
    assert runner.observe_startup(tmp_path, [2, 1], first) is None
    b.write_text("0 1 1000 0 40 500000\n")
    assert runner.observe_startup(tmp_path, [2, 1], first) == 40.5
    assert first == {(0, 0): 1, (0, 1): 2, (1, 0): 40.5}


def test_startup_rejects_failed_transactions(tmp_path):
    (tmp_path / "transactions-0.123").write_text("0 1 failed 0 1 0\n")
    with pytest.raises(ValueError, match="failed"):
        runner.observe_startup(tmp_path, [1], {})


@pytest.mark.parametrize("fail", [False, True])
def test_existing_postgres_image_and_container_cleanup(monkeypatch, tmp_path, fail):
    monkeypatch.setattr(runner, "MZ_ROOT", tmp_path)
    services, removed = [], []

    def override(service):
        services.append(service)
        return nullcontext()

    def measure(config, root, container):
        assert config == {"concurrency": 1}
        assert root.is_dir()
        assert container == "container-id"
        if fail:
            raise ValueError("measurement failed")
        return {"qps": 123}

    monkeypatch.setattr(runner, "measure", measure)
    composition = Mock(
        override=override,
        up=lambda name: None,
        invoke=lambda *args, **kwargs: SimpleNamespace(stdout="container-id\n"),
        rm=lambda name: removed.append(name),
    )
    if fail:
        with pytest.raises(ValueError, match="measurement failed"):
            runner.run({"concurrency": 1}, composition)
    else:
        assert runner.run({"concurrency": 1}, composition) == {"qps": 123}
    assert services[0].config["mzbuild"] == "postgres"
    assert services[0].config["entrypoint"] == ["sleep", "infinity"]
    assert removed == ["qps-pgbench"]
    assert not list((tmp_path / "temp").iterdir())


def test_container_thread_cpu_ticks(monkeypatch):
    fields = ["S"] + ["0"] * 10 + ["100", "50"]
    monkeypatch.setattr(
        runner,
        "container_output",
        lambda *args: "123 (pgbench worker) " + " ".join(fields) + "\n",
    )
    assert runner.thread_ticks("container-id") == {"123": 150}
