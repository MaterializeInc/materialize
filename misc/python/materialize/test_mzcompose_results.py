# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import importlib.util
import threading
import xml.etree.ElementTree as ET
from contextlib import nullcontext
from types import SimpleNamespace
from typing import cast
from unittest import SkipTest
from unittest.mock import MagicMock, Mock

import pytest
import yaml

from materialize import MZ_ROOT, mzbuild
from materialize.cli.mzcompose import RunCommand
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.test_result import FailedTestExecutionError
from materialize.ui import UIError


@pytest.fixture
def composition(tmp_path, monkeypatch):
    (tmp_path / "mzcompose.py").write_text("SERVICES = []\n")
    repo = cast(
        mzbuild.Repository,
        SimpleNamespace(
            compositions={"results": tmp_path}, rd=SimpleNamespace(coverage=False)
        ),
    )
    c = Composition(repo, "results", resolve_image_specs=False, silent=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BUILDKITE_JOB_ID", raising=False)
    monkeypatch.setattr("materialize.buildkite.is_in_buildkite", lambda: False)
    return c


def run_workflow(c, monkeypatch, body, name="default", composition_name="results"):
    c.workflows[name] = body
    monkeypatch.setattr(c, "workflow", lambda *args: body())
    RunCommand().handle_composition(
        argparse.Namespace(
            workflow=name,
            unknown_args=[],
            unknown_subargs=[name],
            find=composition_name,
        ),
        c,
    )


@pytest.mark.parametrize(
    "detailed", [None, "testdrive", "sqllogictest", "testdrive-composition"]
)
def test_skips_are_visible_and_supported_cases_still_run(
    composition, monkeypatch, tmp_path, detailed
):
    c = composition
    c.has_testdrive_junit = detailed == "testdrive"
    c.has_sqllogictest_junit = detailed == "sqllogictest"
    supported = []
    reason = "Unsupported: pre-native to native upgrade"

    def body():
        with c.test_case("pre-native"):
            raise SkipTest(reason)
        with c.test_case("supported"):
            supported.append(True)

    run_workflow(
        c,
        monkeypatch,
        body,
        composition_name=(
            "testdrive" if detailed == "testdrive-composition" else "results"
        ),
    )
    assert supported == [True]
    path = tmp_path / (
        "junit_mzcompose-skipped.xml" if detailed else "junit_mzcompose.xml"
    )
    report = ET.parse(path)
    cases = {case.attrib["name"]: case for case in report.findall(".//testcase")}
    assert cases["pre-native"].find("skipped").attrib["message"] == reason
    assert not report.findall(".//error")
    assert not c.test_results["pre-native"].is_successful()
    assert c.test_results["supported"].is_successful()
    if detailed:
        assert list(cases) == ["pre-native"]
        assert not (tmp_path / "junit_mzcompose.xml").exists()
    else:
        assert cases["supported"].find("skipped") is None


def test_skip_does_not_hide_failure(composition, monkeypatch, tmp_path):
    c = composition

    def body():
        with c.test_case("unsupported"):
            raise SkipTest("Unsupported upgrade")
        with c.test_case("broken"):
            raise UIError("real failure")

    with pytest.raises(UIError, match="at least one test case failed"):
        run_workflow(c, monkeypatch, body)
    report = ET.parse(tmp_path / "junit_mzcompose.xml")
    assert len(report.findall(".//skipped")) == 1
    assert len(report.findall(".//error")) == 1
    assert "real failure" in report.find(".//error").attrib["message"]


def test_skip_reporting_is_redacted_and_aggregated(
    composition, monkeypatch, tmp_path, capsys
):
    c = composition
    monkeypatch.setattr("materialize.buildkite.is_in_buildkite", lambda: True)
    annotate = Mock()
    monkeypatch.setattr("materialize.buildkite.add_annotation", annotate)

    def body():
        for name in ("first", "second"):
            with c.test_case(name):
                raise SkipTest("Unsupported mzp_test-secret")

    run_workflow(c, monkeypatch, body)
    annotate.assert_called_once()
    content = annotate.call_args.args[2]
    assert "first" in content and "second" in content
    assert "mzp_test-secret" not in content
    assert "mzp_test-secret" not in (tmp_path / "junit_mzcompose.xml").read_text()
    captured = capsys.readouterr()
    assert "mzp_test-secret" not in captured.out + captured.err


def test_cleanup_does_not_overwrite_reports(composition, monkeypatch, tmp_path):
    c = composition
    for filename in ("junit_mzcompose.xml", "junit_mzcompose-skipped.xml"):
        (tmp_path / filename).write_text("existing report")
    annotate = Mock()
    monkeypatch.setattr("materialize.buildkite.is_in_buildkite", lambda: True)
    monkeypatch.setattr("materialize.buildkite.add_annotation", annotate)

    def body():
        with c.test_case("unsupported"):
            raise SkipTest("Unsupported upgrade")

    run_workflow(c, monkeypatch, body, name="ci-cleanup")
    assert all(
        path.read_text() == "existing report" for path in tmp_path.glob("junit_*.xml")
    )
    annotate.assert_not_called()


@pytest.fixture
def legacy_upgrade():
    path = MZ_ROOT / "test/legacy-upgrade/mzcompose.py"
    spec = importlib.util.spec_from_file_location("legacy_upgrade_workflow", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_pre_native_upgrade_stops_before_reset_or_start(legacy_upgrade):
    c = Mock(spec=Composition)
    c.down.side_effect = AssertionError("unsupported upgrade attempted reset")
    with pytest.raises(SkipTest, match="pre-native"):
        legacy_upgrade.test_upgrade_from_version(c, "v26.45.0", "*")
    assert c.method_calls == []


def test_legacy_default_preserves_current_source_case(
    legacy_upgrade, composition, monkeypatch
):
    c = composition
    versions = [MzVersion.parse_mz("v26.44.0"), MzVersion.parse_mz("v26.45.0")]
    monkeypatch.setattr(
        legacy_upgrade,
        "get_released_upgrade_versions",
        lambda **kwargs: versions,
    )
    monkeypatch.setattr(legacy_upgrade.buildkite, "get_parallelism_count", lambda: 1)
    monkeypatch.setattr(legacy_upgrade.buildkite, "get_parallelism_index", lambda: 0)
    reset = Mock(side_effect=UIError("native case reached its reset boundary"))
    monkeypatch.setattr(c, "down", reset)
    legacy_upgrade.workflow_default(c, WorkflowArgumentParser("default", "", []))
    reset.assert_called_once_with(destroy_volumes=True)
    assert set(c.test_results) == {"v26.44.0", "v26.45.0", "current_source"}
    assert all(c.test_results[str(v)].skipped_reason is not None for v in versions)
    assert c.test_results["current_source"].errors
    assert c.test_results["current_source"].skipped_reason is None


def test_current_source_preserves_creation_and_validation_scripts(legacy_upgrade):
    c = MagicMock(spec=Composition)
    legacy_upgrade.test_upgrade_from_version(c, "current_source", "selected")
    create, check = c.run_testdrive_files.call_args_list
    assert create.kwargs["mz_service"] == "materialized"
    assert check.kwargs["mz_service"] == "materialized2"
    assert "create-in-{any_version,*,current_source}-selected.td" in create.args
    assert "check-from-{any_version,*,current_source}-selected.td" in check.args
    for invocation in (create, check):
        assert "--no-reset" in invocation.args
        assert "--var=upgrade-from-version=current_source" in invocation.args
    assert [
        arg for arg in create.args if arg.startswith(("--seed=", "--temp-dir="))
    ] == [arg for arg in check.args if arg.startswith(("--seed=", "--temp-dir="))]


@pytest.fixture
def platform_checks(composition, monkeypatch):
    path = MZ_ROOT / "test/platform-checks/mzcompose.py"
    spec = importlib.util.spec_from_file_location("platform_checks_workflow", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(
        MzVersion, "parse_cargo", lambda: MzVersion.parse_mz("v26.46.0-dev")
    )
    monkeypatch.setattr(module.buildkite, "get_parallelism_count", lambda: 1)
    monkeypatch.setattr(module.buildkite, "get_parallelism_index", lambda: 0)
    monkeypatch.setattr(module.buildkite, "shard_list", lambda values, key: values)
    monkeypatch.setattr(module, "setup", Mock())
    monkeypatch.setattr(module, "create_mzs", Mock(return_value=[]))
    monkeypatch.setattr(composition, "override", Mock(return_value=nullcontext()))
    monkeypatch.setattr(composition, "down", Mock())
    return module


@pytest.mark.parametrize("mode", ["sequential", "parallel", "oneatatime"])
@pytest.mark.parametrize(
    "tags",
    [
        ("v26.44.0", "v26.45.0"),
        (None, None),
        ("v26.45.0", None),
        (None, "v26.45.0"),
        ("v26.45.0", None, "v26.45.0"),
    ],
)
def test_platform_boundary_precedes_provisioning_and_reuses_plan(
    platform_checks, composition, monkeypatch, mode, tags
):
    from materialize.checks import mzcompose_actions
    from materialize.checks.checks import Check
    from materialize.checks.scenarios import Scenario

    class First(Check):
        pass

    class Second(Check):
        pass

    plans = []
    executed = []

    class Selected(Scenario):
        def actions(self):
            assert not getattr(self, "planned", False), "plan was generated twice"
            self.planned = True
            plan = [
                mzcompose_actions.StartMz(
                    self, tag=MzVersion.parse_mz(tag) if tag else None
                )
                for tag in tags
            ]
            plans.append(plan)
            return plan

    monkeypatch.setattr(platform_checks, "Selected", Selected, raising=False)
    monkeypatch.setattr(
        platform_checks,
        "all_subclasses",
        lambda cls: {First, Second} if cls is Check else {Selected},
    )
    for cls in (
        mzcompose_actions.StartMz,
        mzcompose_actions.ConfigureMz,
        mzcompose_actions.SetupSqlServerTesting,
        mzcompose_actions.SetupIcebergTesting,
    ):
        monkeypatch.setattr(
            cls, "execute", lambda action, executor: executed.append(action)
        )
        monkeypatch.setattr(cls, "join", lambda action, executor: None)
    platform_checks.workflow_default(
        composition,
        WorkflowArgumentParser(
            "default",
            "",
            ["--scenario=Selected", f"--execution-mode={mode}", "--seed=chosen-plan"],
        ),
    )
    runs = 2 if mode == "oneatatime" else 1
    unsupported = None in tags and any(tag is not None for tag in tags)
    assert len(plans) == runs
    assert len(composition.test_results) == runs
    if unsupported:
        assert all(
            "pre-native" in result.skipped_reason
            for result in composition.test_results.values()
        )
        assert executed == []
        platform_checks.create_mzs.assert_not_called()
        composition.override.assert_not_called()
        platform_checks.setup.assert_not_called()
        composition.down.assert_not_called()
    else:
        assert all(
            result.is_successful() for result in composition.test_results.values()
        )
        assert [
            action
            for action in executed
            if isinstance(action, mzcompose_actions.StartMz)
        ] == [
            action
            for plan in plans
            for action in plan
            if isinstance(action, mzcompose_actions.StartMz)
        ]
        assert platform_checks.setup.call_count == runs
        assert composition.down.call_count == runs - 1


@pytest.mark.parametrize(
    "name,unsupported",
    [
        ("UpgradeClusterdComputeFirst", True),
        ("UpgradeClusterdComputeLast", True),
        ("PreflightCheckRollback", True),
        ("ZeroDowntimeBumpedVersion", False),
        ("ZeroDowntimeRestartEntireMzForcedMigrations", False),
    ],
)
def test_platform_real_plans_classify_artifacts_not_parameter_versions(
    platform_checks, composition, monkeypatch, name, unsupported
):
    from materialize.checks import scenarios_upgrade
    from materialize.checks.executors import MzcomposeExecutor
    from materialize.checks.features import Features

    monkeypatch.setattr(
        scenarios_upgrade, "get_last_version", lambda: MzVersion.parse_mz("v26.45.0")
    )
    scenario = getattr(platform_checks, name)(
        [], MzcomposeExecutor(composition), Features([])
    )
    plan = scenario.actions()
    if unsupported:
        with pytest.raises(SkipTest, match="pre-native"):
            platform_checks.check_native_upgrade_boundary(plan)
    else:
        platform_checks.check_native_upgrade_boundary(plan)
    platform_checks.create_mzs.assert_not_called()
    composition.override.assert_not_called()


def test_platform_compute_only_boundary(platform_checks):
    from materialize.checks.mzcompose_actions import StartClusterdCompute, StartMz

    with pytest.raises(SkipTest, match="pre-native"):
        platform_checks.check_native_upgrade_boundary(
            [
                StartMz(Mock(), tag=MzVersion.parse_mz("v26.45.0")),
                StartClusterdCompute(),
            ]
        )


def test_platform_default_continues_after_skip_and_keeps_native_failures(
    platform_checks, composition, monkeypatch, tmp_path
):
    from materialize.checks import mzcompose_actions
    from materialize.checks.scenarios import Scenario

    class AUnsupported(Scenario):
        def actions(self):
            return [
                mzcompose_actions.StartMz(self, tag=MzVersion.parse_mz("v26.45.0")),
                mzcompose_actions.StartMz(self),
            ]

    class BNative(Scenario):
        def actions(self):
            return [mzcompose_actions.StartMz(self)]

    class CNativeForced(Scenario):
        def actions(self):
            return [mzcompose_actions.StartMz(self, force_migrations="replacement")]

    monkeypatch.setattr(
        platform_checks,
        "all_subclasses",
        lambda cls: (
            {AUnsupported, BNative, CNativeForced} if cls is Scenario else set()
        ),
    )

    sql_report = tmp_path / "sql-failure.xml"
    sql_report.write_text(
        '<testsuite><testcase name="check.td"><failure message="check.td:23:1: native forced migration rejected"/></testcase></testsuite>'
    )

    def execute(action, executor):
        if isinstance(action, mzcompose_actions.StartMz) and action.force_migrations:
            raise FailedTestExecutionError(
                composition._read_testdrive_junit_errors(sql_report)
            )

    for cls in (
        mzcompose_actions.StartMz,
        mzcompose_actions.ConfigureMz,
        mzcompose_actions.SetupSqlServerTesting,
        mzcompose_actions.SetupIcebergTesting,
    ):
        monkeypatch.setattr(cls, "execute", execute)
        monkeypatch.setattr(cls, "join", lambda action, executor: None)
    with pytest.raises(UIError, match="CNativeForced failed"):
        platform_checks.workflow_default(
            composition, WorkflowArgumentParser("default", "", [])
        )
    report_path = RunCommand().write_junit_report_to_file(
        RunCommand().generate_junit_suite(composition)
    )
    report = ET.parse(report_path)
    failures = [
        case for case in report.findall(".//testcase") if case.find("error") is not None
    ]
    assert [case.attrib["name"] for case in failures] == ["CNativeForced/check.td:23"]
    assert "native forced migration rejected" in failures[0].find("error").text
    assert composition.test_results["AUnsupported"].skipped_reason
    assert composition.test_results["BNative"].is_successful()
    assert composition.test_results["CNativeForced"].errors
    assert composition.test_results["CNativeForced"].skipped_reason is None
    assert platform_checks.setup.call_count == 2
    composition.down.assert_called_once_with(
        destroy_volumes=True, sanity_restart_mz=False
    )


def test_platform_parallel_failure_never_resets_under_a_running_check(
    platform_checks, composition, monkeypatch
):
    from materialize.checks import mzcompose_actions
    from materialize.checks.actions import Initialize, Testdrive
    from materialize.checks.checks import Check
    from materialize.checks.scenarios import Scenario

    started = threading.Event()
    release = threading.Event()
    workers = []

    class AFail(Check):
        def initialize(self):
            return Testdrive("fail")

    class BBlocked(Check):
        def initialize(self):
            return Testdrive("block")

    class AParallelFailure(Scenario):
        def actions(self):
            return [mzcompose_actions.StartMz(self), Initialize(self)]

    class ZMustNotRun(Scenario):
        def actions(self):
            raise AssertionError("another scenario planned after failure")

    monkeypatch.setattr(
        platform_checks,
        "all_subclasses",
        lambda cls: (
            {AParallelFailure, ZMustNotRun} if cls is Scenario else {AFail, BBlocked}
        ),
    )
    for cls in (
        mzcompose_actions.StartMz,
        mzcompose_actions.ConfigureMz,
        mzcompose_actions.SetupSqlServerTesting,
        mzcompose_actions.SetupIcebergTesting,
    ):
        monkeypatch.setattr(
            cls,
            "execute",
            lambda action, executor: setattr(
                executor, "current_mz_version", MzVersion.parse_cargo()
            ),
        )
        monkeypatch.setattr(cls, "join", lambda action, executor: None)

    def testdrive(input, **kwargs):
        if input == "block":
            workers.append(threading.current_thread())
            started.set()
            assert release.wait(10), "test did not release blocked worker"
        else:
            assert started.wait(10), "blocked worker did not start"
            raise UIError("first parallel check failed")

    monkeypatch.setattr(composition, "testdrive", testdrive)
    try:
        with pytest.raises(UIError, match="AParallelFailure failed"):
            platform_checks.workflow_default(
                composition,
                WorkflowArgumentParser("default", "", ["--execution-mode=parallel"]),
            )
        assert workers and workers[0].is_alive()
        assert set(composition.test_results) == {"AParallelFailure"}
        composition.down.assert_not_called()
        assert platform_checks.setup.call_count == 1
    finally:
        release.set()
        for worker in workers:
            worker.join(timeout=10)
            assert not worker.is_alive()


@pytest.mark.parametrize("workflow", ["default", "dangling", "stale-rows"])
def test_incident_1007_reports_unsupported_before_services(
    composition, monkeypatch, tmp_path, workflow
):
    path = MZ_ROOT / "test/catalog-migration/incident-1007/mzcompose.py"
    spec = importlib.util.spec_from_file_location("incident_1007_workflow", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    composition.workflows = {
        name.removeprefix("workflow_").replace("_", "-"): getattr(module, name)
        for name in dir(module)
        if name.startswith("workflow_")
    }
    for operation in ("up", "down", "override"):
        monkeypatch.setattr(
            composition,
            operation,
            Mock(side_effect=AssertionError(f"unsupported case attempted {operation}")),
        )
    RunCommand().handle_composition(
        argparse.Namespace(
            workflow=workflow,
            unknown_args=[],
            unknown_subargs=[workflow],
            find="results",
        ),
        composition,
    )
    report = ET.parse(tmp_path / "junit_mzcompose.xml")
    skipped = [
        case
        for case in report.findall(".//testcase")
        if case.find("skipped") is not None
    ]
    assert {case.attrib["name"] for case in skipped} == (
        {"dangling", "stale-rows"}
        if workflow == "default"
        else {f"workflow-{workflow}"}
    )
    assert all(
        "pre-native" in case.find("skipped").attrib["message"] for case in skipped
    )
    assert not report.findall(".//error")
    for operation in ("up", "down", "override"):
        getattr(composition, operation).assert_not_called()


def test_pre_native_preflight_triggers_are_explicitly_skipped():
    pipeline = yaml.safe_load(
        (MZ_ROOT / "ci/release-qualification/pipeline.template.yml").read_text()
    )
    steps = {
        step.get("id"): step for step in pipeline["steps"] if isinstance(step, dict)
    }
    for name in ("test-preflight-check-rollback", "nightly-preflight-check-rollback"):
        assert "pre-native" in steps[name]["skip"]
