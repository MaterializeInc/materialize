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
import yaml

from materialize import MZ_ROOT, mzbuild
from materialize.antithesis import templates

SOURCE = Path(__file__).resolve().parents[4] / "test/antithesis/workload/test/main"


@pytest.mark.parametrize("role_name", ["workload", "workload-operator"])
def test_workload_can_read_termination_logs_without_mutating_them(
    role_name: str,
) -> None:
    resources = yaml.safe_load_all(
        (MZ_ROOT / "test/antithesis/manifests/30-workload.yaml").read_text()
    )
    role = next(
        resource
        for resource in resources
        if resource["kind"] == "Role" and resource["metadata"]["name"] == role_name
    )
    log_rules = [rule for rule in role["rules"] if "pods/log" in rule["resources"]]
    assert len(log_rules) == 1
    assert log_rules[0]["apiGroups"] == [""]
    assert log_rules[0]["resources"] == ["pods/log"]
    assert log_rules[0]["verbs"] == ["get"]


def test_workload_context_carries_authoritative_halt_policy(tmp_path: Path) -> None:
    definition = yaml.safe_load(
        (MZ_ROOT / "test/antithesis/workload/mzbuild.yml").read_text()
    )
    actions = [
        action
        for action in definition["pre-image"]
        if action.get("source") == "src/ore/src"
    ]
    assert len(actions) == 1
    config = dict(actions[0])
    assert config.pop("type") == "copy"
    action = mzbuild.Copy(MagicMock(root=MZ_ROOT), tmp_path, config)
    assert "src/ore/src/antithesis.rs" in action.inputs()
    action.run(None)
    policy = Path("src/ore/src/antithesis.rs")
    assert (tmp_path / policy).read_bytes() == (MZ_ROOT / policy).read_bytes()


def test_mixed_template_keeps_every_authoritative_command(tmp_path: Path) -> None:
    templates.generate(SOURCE, tmp_path)
    discovered = {p.name for p in templates.commands(SOURCE)}
    assert {p.name for p in (tmp_path / "main").iterdir()} == discovered
    for name in templates.PROFILES:
        paths = list((tmp_path / name).iterdir())
        assert sum(p.name.startswith("first_") for p in paths) == 1
        assert any(p.name.startswith("parallel_driver_") for p in paths)
        assert all(p.stat().st_mode & 0o111 for p in paths)


def test_all_profiles_audit_exits_without_a_separate_scheduled_command(
    tmp_path: Path,
) -> None:
    templates.generate(SOURCE, tmp_path)
    for name in templates.PROFILES:
        assert not (tmp_path / name / "anytime_pod_exits.py").exists()
        for command in (tmp_path / name).iterdir():
            assert "run_command" in command.read_text()
    entrypoint = (MZ_ROOT / "test/antithesis/workload/entrypoint.sh").read_text()
    assert (
        "exec python3 -m materialize.antithesis.drivers.recovery --observe"
        in entrypoint
    )
    assert entrypoint.index("materialize.antithesis.setup") < entrypoint.index(
        "--observe"
    )


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
    assert "parallel_driver_read_paths_load.py" in rollouts
    assert "anytime_read_paths_agree.py" in rollouts
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
        for name in (
            "bootstrap",
            "configure",
            "rollouts",
            *templates.PROFILES["main"].setup,
        )
    }
    configure = modules["configure"]

    def configure_main() -> int:
        if not templates.enabled_driver("rollouts", name):
            modules["bootstrap"].ensure_target_image.assert_called_once()
        return 7

    configure.main.side_effect = configure_main
    monkeypatch.setenv(templates.PROFILE_ENV, name)
    imported = []

    def import_module(module: str):
        driver = module.rsplit(".", 1)[1]
        imported.append(driver)
        return modules[driver]

    monkeypatch.setattr(templates.importlib, "import_module", import_module)
    assert templates.initialize() == 7
    assert imported == [
        *(["bootstrap"] if not templates.enabled_driver("rollouts", name) else []),
        "configure",
        "rollouts",
        *templates.PROFILES[name].setup,
    ]
    assert modules["bootstrap"].ensure_target_image.called is (
        not templates.enabled_driver("rollouts", name)
    )
    modules["rollouts"].seed_serving_version.assert_called_once()
    for driver in templates.PROFILES["main"].setup:
        assert modules[driver].setup_main.called is (
            driver in templates.PROFILES[name].setup
        )
    configure.open_state.return_value.execute.assert_called_once_with(
        "INSERT OR REPLACE INTO profile VALUES ('workload_template', ?)", (name,)
    )
    configure.open_state.return_value.close.assert_called_once()


@pytest.mark.parametrize("name", ["transactions", "cdc"])
def test_failed_target_bootstrap_stops_before_configuration_or_data_setup(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    modules = {
        driver: MagicMock()
        for driver in (
            "bootstrap",
            "configure",
            "rollouts",
            *templates.PROFILES["main"].setup,
        )
    }
    modules["bootstrap"].ensure_target_image.side_effect = TimeoutError(
        "target did not become ready"
    )
    monkeypatch.setattr(
        templates.importlib,
        "import_module",
        lambda module: modules[module.rsplit(".", 1)[1]],
    )
    with pytest.raises(TimeoutError, match="target did not become ready"):
        templates.initialize(name)
    modules["configure"].main.assert_not_called()
    modules["rollouts"].seed_serving_version.assert_not_called()
    for driver in templates.PROFILES["main"].setup:
        modules[driver].setup_main.assert_not_called()


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
