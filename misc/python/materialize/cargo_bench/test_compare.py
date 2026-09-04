# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
from pathlib import Path

from materialize.cargo_bench.compare import (
    Verdict,
    compare,
    format_duration,
    render_markdown,
)


def _estimate(point: float, lower: float, upper: float) -> dict:
    return {
        "confidence_interval": {
            "confidence_level": 0.95,
            "lower_bound": lower,
            "upper_bound": upper,
        },
        "point_estimate": point,
        "standard_error": 0.0,
    }


def _estimates(mean_ns: float) -> dict:
    e = _estimate(mean_ns, mean_ns * 0.99, mean_ns * 1.01)
    return {
        "mean": e,
        "median": e,
        "median_abs_dev": e,
        "slope": None,
        "std_dev": e,
    }


def _write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def _bench(
    root: Path,
    directory: str,
    full_id: str,
    current_ns: float,
    ancestor_ns: float | None = None,
    change: tuple[float, float, float] | None = None,
) -> None:
    d = root / directory
    _write(d / "new" / "benchmark.json", {"full_id": full_id})
    _write(d / "new" / "estimates.json", _estimates(current_ns))
    if ancestor_ns is not None:
        _write(d / "ancestor" / "estimates.json", _estimates(ancestor_ns))
    if change is not None:
        point, lower, upper = change
        e = _estimate(point, lower, upper)
        _write(d / "change" / "estimates.json", {"mean": e, "median": e})


def test_verdicts(tmp_path: Path) -> None:
    _bench(tmp_path, "g/regressed", "g/regressed", 120.0, 100.0, (0.20, 0.15, 0.25))
    _bench(tmp_path, "g/noisy", "g/noisy", 115.0, 100.0, (0.15, 0.05, 0.25))
    _bench(tmp_path, "g/faster", "g/faster", 80.0, 100.0, (-0.20, -0.25, -0.15))
    _bench(tmp_path, "g/same", "g/same", 101.0, 100.0, (0.01, -0.01, 0.03))
    _bench(tmp_path, "g/brand_new", "g/brand_new", 50.0)

    report = compare(tmp_path, threshold=0.10)

    verdicts = {r.id: r.verdict for r in report.results}
    assert verdicts == {
        "g/regressed": Verdict.REGRESSION,
        "g/noisy": Verdict.UNCHANGED,
        "g/faster": Verdict.IMPROVEMENT,
        "g/same": Verdict.UNCHANGED,
        "g/brand_new": Verdict.NEW,
    }
    assert report.has_regressions
    assert report.warnings == []


def test_ordering_regressions_first(tmp_path: Path) -> None:
    _bench(tmp_path, "a", "a", 50.0)
    _bench(tmp_path, "b", "b", 101.0, 100.0, (0.01, -0.01, 0.03))
    _bench(tmp_path, "c", "c", 80.0, 100.0, (-0.20, -0.25, -0.15))
    _bench(tmp_path, "d", "d", 120.0, 100.0, (0.20, 0.15, 0.25))
    _bench(tmp_path, "e", "e", 150.0, 100.0, (0.50, 0.45, 0.55))

    report = compare(tmp_path, threshold=0.10)

    assert [r.id for r in report.results] == ["e", "d", "c", "b", "a"]


def test_new_benchmark_has_no_ancestor_fields(tmp_path: Path) -> None:
    _bench(tmp_path, "n", "n", 50.0)

    [result] = compare(tmp_path, threshold=0.10).results

    assert result.verdict == Verdict.NEW
    assert result.current_mean_ns == 50.0
    assert result.ancestor_mean_ns is None
    assert result.change_mean is None


def test_missing_change_with_ancestor_warns_and_is_new(tmp_path: Path) -> None:
    _bench(tmp_path, "m", "m", 50.0, ancestor_ns=40.0)

    report = compare(tmp_path, threshold=0.10)

    [result] = report.results
    assert result.verdict == Verdict.NEW
    assert result.ancestor_mean_ns == 40.0
    assert len(report.warnings) == 1
    assert "m" in report.warnings[0]


def test_stale_change_without_ancestor_is_ignored(tmp_path: Path) -> None:
    _bench(tmp_path, "s", "s", 50.0, change=(0.5, 0.4, 0.6))

    [result] = compare(tmp_path, threshold=0.10).results

    assert result.verdict == Verdict.NEW


def test_no_regressions(tmp_path: Path) -> None:
    _bench(tmp_path, "b", "b", 101.0, 100.0, (0.01, -0.01, 0.03))

    assert not compare(tmp_path, threshold=0.10).has_regressions


def test_format_duration() -> None:
    assert format_duration(999.4) == "999.40 ns"
    assert format_duration(1_500.0) == "1.50 µs"
    assert format_duration(2_500_000.0) == "2.50 ms"
    assert format_duration(3_000_000_000.0) == "3.00 s"


def test_render_markdown(tmp_path: Path) -> None:
    _bench(tmp_path, "g/regressed", "g/regressed", 120.0, 100.0, (0.20, 0.15, 0.25))
    _bench(tmp_path, "g/brand_new", "g/brand_new", 50.0)
    report = compare(tmp_path, threshold=0.10)

    markdown = render_markdown(report)

    lines = markdown.splitlines()
    assert lines[0] == "| Benchmark | Ancestor | Current | Change | 95% CI | Verdict |"
    assert lines[1] == "| --- | --- | --- | --- | --- | --- |"
    assert lines[2] == (
        "| g/regressed | 100.00 ns | 120.00 ns | +20.0% | [+15.0%, +25.0%] | **regression** |"
    )
    assert lines[3] == "| g/brand_new |  | 50.00 ns |  |  | new |"
