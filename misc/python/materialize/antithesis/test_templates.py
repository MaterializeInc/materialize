# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import runpy
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest

from materialize.antithesis import templates

SOURCE = Path(__file__).resolve().parents[4] / "test/antithesis/workload/test/main"


def test_mixed_template_keeps_every_authoritative_command(tmp_path: Path) -> None:
    templates.generate(SOURCE, tmp_path)
    discovered = {p.name for p in templates.commands(SOURCE)}
    assert {p.name for p in (tmp_path / "main").iterdir()} == discovered
    for name in templates.PROFILES:
        paths = list((tmp_path / name).iterdir())
        assert sum(p.name.startswith("first_") for p in paths) == 1
        assert any(p.name.startswith("parallel_driver_") for p in paths)
        assert all(p.stat().st_mode & 0o111 for p in paths)


def test_focused_templates_omit_unrelated_workloads(tmp_path: Path) -> None:
    templates.generate(SOURCE, tmp_path)
    transactions = {p.name for p in (tmp_path / "transactions").iterdir()}
    cdc = {p.name for p in (tmp_path / "cdc").iterdir()}
    rollouts = {p.name for p in (tmp_path / "rollouts").iterdir()}
    assert "parallel_driver_client_history.py" in transactions
    assert "parallel_driver_read_then_write.py" in transactions
    assert "parallel_driver_session_mix.py" in transactions
    assert "parallel_driver_pg_upstream.py" not in transactions
    assert "parallel_driver_ivm_writes.py" not in transactions
    assert "parallel_driver_pg_upstream.py" in cdc
    assert "parallel_driver_kafka_produce.py" in cdc
    assert "parallel_driver_kafka_sink_writes.py" in cdc
    assert "parallel_driver_ivm_writes.py" not in cdc
    assert "parallel_driver_rollouts.py" in rollouts
    assert "parallel_driver_spec_phase.py" in rollouts
    assert "parallel_driver_pg_upstream.py" in rollouts
    assert "parallel_driver_ivm_writes.py" in rollouts
    assert "parallel_driver_kafka_sink_writes.py" not in rollouts


def test_profile_membership_follows_new_commands_for_existing_drivers(
    tmp_path: Path,
) -> None:
    source = tmp_path / "commands"
    source.mkdir()
    for name, module in (
        ("first_configure", "configure"),
        ("parallel_driver_new_counter_action", "counters"),
        ("anytime_new_pg_oracle", "postgres_sources"),
    ):
        path = source / f"{name}.py"
        path.write_text(f"from materialize.antithesis.drivers.{module} import main\n")
        path.chmod(0o755)
    output = tmp_path / "templates"
    templates.generate(source, output)
    assert (output / "transactions/parallel_driver_new_counter_action.py").exists()
    assert not (output / "cdc/parallel_driver_new_counter_action.py").exists()
    assert (output / "cdc/anytime_new_pg_oracle.py").exists()
    assert (output / "rollouts/anytime_new_pg_oracle.py").exists()
    (output / "transactions/parallel_driver_obsolete.py").write_text("obsolete")
    templates.generate(source, output)
    assert not (output / "transactions/parallel_driver_obsolete.py").exists()


def test_profile_membership_accepts_module_imports_and_shared_diagnostics(
    tmp_path: Path,
) -> None:
    counter = tmp_path / "parallel_driver_counter.py"
    counter.write_text("import materialize.antithesis.drivers.counters as counters\n")
    assert templates.included(counter, templates.PROFILES["transactions"])
    assert not templates.included(counter, templates.PROFILES["cdc"])
    diagnostics = tmp_path / "anytime_workload_efficiency.py"
    diagnostics.write_text("from materialize.antithesis import telemetry\n")
    assert all(templates.included(diagnostics, p) for p in templates.PROFILES.values())


@pytest.mark.parametrize("name", templates.PROFILES)
def test_generated_command_measures_the_original_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    templates.generate(SOURCE, tmp_path)
    telemetry = ModuleType("materialize.antithesis.telemetry")
    calls = []

    def run_command(profile: str, source: str) -> int:
        calls.append((profile, source))
        return 12

    telemetry.run_command = run_command  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, telemetry.__name__, telemetry)
    with pytest.raises(SystemExit) as exited:
        runpy.run_path(str(tmp_path / name / "first_configure.py"), run_name="__main__")
    assert exited.value.code == 12
    assert calls == [(name, str(SOURCE / "first_configure.py"))]


@pytest.mark.parametrize("name", templates.PROFILES)
def test_initialization_seeds_only_the_selected_workloads(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    modules = {
        name: MagicMock()
        for name in ("configure", "rollouts", *templates.PROFILES["main"].setup)
    }
    configure = modules["configure"]
    configure.main.return_value = 7
    monkeypatch.setenv(templates.PROFILE_ENV, name)
    imported = []

    def import_module(module: str):
        driver = module.rsplit(".", 1)[1]
        imported.append(driver)
        return modules[driver]

    monkeypatch.setattr(templates.importlib, "import_module", import_module)
    assert templates.initialize() == 7
    assert imported == ["configure", "rollouts", *templates.PROFILES[name].setup]
    modules["rollouts"].seed_serving_version.assert_called_once()
    for driver in templates.PROFILES["main"].setup:
        assert modules[driver].setup_main.called is (
            driver in templates.PROFILES[name].setup
        )
    configure.open_state.return_value.execute.assert_called_once_with(
        "INSERT OR REPLACE INTO profile VALUES ('workload_template', ?)", (name,)
    )
    configure.open_state.return_value.close.assert_called_once()


def test_generation_rejects_overwriting_command_sources(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must not overlap"):
        templates.generate(tmp_path, tmp_path / "templates")


def test_generation_rejects_unclassified_or_nonexecutable_commands(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first_configure.py"
    first.write_text("from materialize.antithesis.templates import initialize\n")
    with pytest.raises(ValueError, match="must be executable"):
        templates.commands(tmp_path)
    first.chmod(0o755)
    unknown = tmp_path / "anytime_unknown.py"
    unknown.write_text("pass\n")
    unknown.chmod(0o755)
    with pytest.raises(ValueError, match="identify one driver"):
        templates.generate(tmp_path, tmp_path.parent / f"{tmp_path.name}-output")
