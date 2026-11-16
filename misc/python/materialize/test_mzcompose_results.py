# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import copy
import importlib.util
import json
import threading
import xml.etree.ElementTree as ET
from contextlib import nullcontext
from types import SimpleNamespace
from typing import cast
from unittest import SkipTest
from unittest.mock import MagicMock, Mock, call

import pytest
import yaml

from materialize import MZ_ROOT, mzbuild
from materialize.cli.mzcompose import RunCommand
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.test_result import FailedTestExecutionError
from materialize.ui import CommandFailureCausedUIError, UIError


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


def test_native_promotion_does_not_retire_or_kill(composition, monkeypatch):
    execute = Mock(return_value=SimpleNamespace(stdout='{"result":"Success"}'))
    cleanup = Mock()
    monkeypatch.setattr(composition, "exec", execute)
    monkeypatch.setattr(composition, "kill_fenced_mz", cleanup)

    composition.promote_mz("candidate", retire_mz_service=None)

    assert execute.call_count == 1
    assert "localhost:6878/api/leader/promote" in execute.call_args.args
    cleanup.assert_not_called()


@pytest.mark.parametrize("image", [None, "known-compatible-native-image"])
def test_native_deployment_overrides_keep_the_external_relay(image):
    path = MZ_ROOT / "test/0dt/mzcompose.py"
    spec = importlib.util.spec_from_file_location("native_deployment_workflow", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    service = module.native_mz(
        name="candidate", image=image, options=["--unsafe-mode"], depends_on=["other"]
    )
    assert "--persist-pubsub-external" in service.config["command"]
    assert "--persist-pubsub-url=http://pubsub:6879" in service.config["command"]
    assert "--unsafe-mode" in service.config["command"]
    assert {"pubsub", "other"} <= service.config["depends_on"].keys()
    relay = module.Pubsub(image=image).config
    assert ("mzbuild" in relay) == (image is None)
    assert ("image" in relay) == (image is not None)
    assert relay["entrypoint"] == ["tini", "--", "pubsubd"]
    assert "ports" not in relay
    assert "volumes" not in relay


@pytest.mark.parametrize("failure", ["lifecycle", "interrupt", "setup"])
def test_upsert_failure_joins_workers_before_returning(monkeypatch, failure):
    spec = importlib.util.spec_from_file_location(
        "native_upsert_workflow", MZ_ROOT / "test/0dt/mzcompose.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    started = threading.Event()
    release = threading.Event()
    expired = threading.Event()
    retried_after_failure = threading.Event()
    workers = []
    in_flight = threading.Condition()
    entered = []
    now = module.datetime.now(module.UTC)

    class Clock:
        @staticmethod
        def now(tz):
            return now + module.timedelta(seconds=201 if expired.is_set() else 0)

    class Worker(threading.Thread):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            workers.append(self)

        def join(self, timeout=None):
            # Finish the outstanding fake I/O when its owner waits for it.
            # Without shutdown ownership, the blocked worker remains live at
            # workflow return, and the assertion below fails deterministically.
            release.set()
            super().join(timeout)

    c = MagicMock()
    c.override.side_effect = lambda *args: nullcontext()

    def testdrive(sql):
        if "CREATE SOURCE kafka_source" in sql:
            if failure == "setup" and threading.current_thread().name == "worker_0":
                expired.set()
                started.set()
                raise UIError("source setup failed")
        elif "kafka-ingest" in sql:
            if release.is_set():
                # Fail promptly rather than spin forever under the frozen
                # clock if cancellation stops preventing new batches.
                retried_after_failure.set()
                expired.set()
            else:
                with in_flight:
                    entered.append(threading.current_thread())
                    in_flight.notify_all()
                started.set()
                assert release.wait(10), "owner did not join its in-flight worker"

    def up(*services):
        if services == ("mz_new",):
            assert started.wait(10), "worker did not start"
            if failure != "setup":
                with in_flight:
                    assert in_flight.wait_for(
                        lambda: len(entered) == len(workers), timeout=10
                    ), "workers did not reach their in-flight batches"
            if failure == "interrupt":
                raise KeyboardInterrupt("candidate startup interrupted")
            if failure == "lifecycle":
                raise UIError("candidate startup failed")

    c.testdrive.side_effect = testdrive
    c.up.side_effect = up
    monkeypatch.setattr(module, "Thread", Worker)
    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(module, "setup", lambda c: None)
    try:
        message = (
            "candidate startup failed"
            if failure == "lifecycle"
            else "source setup failed"
        )
        error = UIError
        if failure == "interrupt":
            error = KeyboardInterrupt
            message = "candidate startup interrupted"
        with pytest.raises(error, match=message):
            module.workflow_upsert_sources(c)
        assert workers and not any(worker.is_alive() for worker in workers)
        assert not retried_after_failure.is_set()
    finally:
        # Also reap workers when testing the unfixed implementation.
        expired.set()
        release.set()
        for worker in workers:
            if worker.ident is not None:
                worker.join(timeout=10)
                assert not worker.is_alive()


@pytest.mark.parametrize("expected_host", [None, "new"])
def test_cdc_reader_wait_requires_stable_convergence(monkeypatch, expected_host):
    spec = importlib.util.spec_from_file_location(
        "native_reader_workflow", MZ_ROOT / "test/0dt/mzcompose.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    now = 0.0

    def sleep(seconds):
        nonlocal now
        now += seconds

    def observe():
        if now < 1:
            return []
        if now < 2:
            return [None]
        if now < 3:
            return ["old", "new"]
        if now < 4:
            return ["old"]
        if now < 9:
            return ["intermediate"]
        return ["new"]

    monkeypatch.setattr(
        module, "time", SimpleNamespace(monotonic=lambda: now, sleep=sleep)
    )
    assert (
        module._wait_for_cdc_reader(
            observe, expected_host=expected_host, excluded_host="old"
        )
        == "new"
    )
    # Neither overlap nor a changing eligible host satisfies the stable window.
    assert now == 19


def test_cdc_reader_wait_rejects_sustained_old_ownership(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "native_reader_workflow", MZ_ROOT / "test/0dt/mzcompose.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    now = 0.0

    def sleep(seconds):
        nonlocal now
        now += seconds

    monkeypatch.setattr(
        module, "time", SimpleNamespace(monotonic=lambda: now, sleep=sleep)
    )
    with pytest.raises(AssertionError, match="old"):
        module._wait_for_cdc_reader(lambda: ["old"], excluded_host="old")
    assert now == 300


@pytest.mark.parametrize(
    "receipt",
    [
        {"retired": 7, "environment_id": "expected-environment"},
        {"retired": 8, "environment_id": "expected-environment"},
        {"retired": 7, "environment_id": "other-environment"},
        {},
        None,
    ],
)
def test_native_retirement_checks_identity_without_cleanup(
    composition, monkeypatch, receipt
):
    execute = Mock(return_value=SimpleNamespace(stdout=json.dumps(receipt)))
    if receipt is None:
        execute.side_effect = UIError("HTTP 409")
    cleanup = Mock()
    monkeypatch.setattr(composition, "exec", execute)
    monkeypatch.setattr(composition, "kill_fenced_mz", cleanup)

    def retire():
        composition.retire_mz_deployment(
            "survivor", 7, environment_id="expected-environment"
        )

    if receipt is None:
        with pytest.raises(UIError, match="HTTP 409"):
            retire()
    elif receipt == {"retired": 7, "environment_id": "expected-environment"}:
        retire()
    else:
        with pytest.raises(AssertionError, match="Unexpected retirement receipt"):
            retire()

    args = execute.call_args.args
    assert args[0] == "survivor"
    assert args[-1] == "http://localhost:6882/api/deployments/7/retire"
    assert "--fail-with-body" in args
    assert json.loads(args[args.index("--data") + 1]) == {
        "environment_id": "expected-environment"
    }
    cleanup.assert_not_called()


@pytest.mark.parametrize("outcome", ["retired", "still_writable", "retire_error"])
def test_native_basic_checks_retired_writer_before_cleanup(monkeypatch, outcome):
    import psycopg

    spec = importlib.util.spec_from_file_location(
        "native_basic_workflow", MZ_ROOT / "test/0dt/mzcompose.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "setup", Mock())
    monkeypatch.setattr(module, "setup_sql_server_testing", Mock())
    c = MagicMock(spec=Composition)
    old_session = MagicMock()
    c.sql_connection.return_value.__enter__.return_value = old_session
    c.attach_mock(old_session, "old_session")

    def execute(sql):
        if sql == "INSERT INTO t VALUES (99, 100)" and outcome == "retired":
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        rows = [(42,)] if sql == "SELECT * FROM basic_peer" else [(1,)]
        return SimpleNamespace(fetchall=lambda: rows)

    old_session.execute.side_effect = execute
    if outcome == "retire_error":
        c.retire_mz_deployment.side_effect = UIError("retirement failed")
        with pytest.raises(UIError, match="retirement failed"):
            module.workflow_basic(c)
    elif outcome == "still_writable":
        with pytest.raises(AssertionError, match="retired deployment accepted"):
            module.workflow_basic(c)
    else:
        module.workflow_basic(c)
        c.kill_fenced_mz.assert_called_once_with("mz_old")

    c.sql_connection.assert_called_once_with(service="mz_old")
    c.promote_mz.assert_called_once_with("mz_new", retire_mz_service=None)
    c.retire_mz_deployment.assert_called_once_with(
        "mz_new", 0, environment_id=module.DEFAULT_MZ_ENVIRONMENT_ID
    )
    events = c.method_calls
    promotion = events.index(call.promote_mz("mz_new", retire_mz_service=None))
    promoted = events.index(
        call.await_mz_deployment_status(module.DeploymentStatus.IS_LEADER, "mz_new")
    )
    recovered = events.index(
        call.sql("SELECT 1", service="mz_new", reuse_connection=False)
    )
    peer_write = events.index(call.old_session.execute("INSERT INTO t VALUES (13, 14)"))
    retirement = next(
        i for i, event in enumerate(events) if event[0] == "retire_mz_deployment"
    )
    assert promotion < promoted < recovered < peer_write < retirement
    if outcome == "retired":
        rejected_write = events.index(
            call.old_session.execute("INSERT INTO t VALUES (99, 100)")
        )
        assert retirement < rejected_write < events.index(call.kill_fenced_mz("mz_old"))
    else:
        c.kill_fenced_mz.assert_not_called()
    survivor_checks = [
        i
        for i, event in enumerate(events)
        if event[0] == "testdrive" and "CREATE TABLE basic_survivor" in event.args[0]
    ]
    if outcome == "retired":
        assert len(survivor_checks) == 1
        assert events.index(call.kill_fenced_mz("mz_old")) < survivor_checks[0]
    else:
        assert not survivor_checks


@pytest.mark.parametrize(
    "outcome", ["retry_then_success", "exhausted", "wrong_identity"]
)
def test_native_retirement_retries_only_explicit_contention(
    composition, monkeypatch, outcome
):
    message = (
        "environment identity does not match this controller"
        if outcome == "wrong_identity"
        else "catalog changed; retry the request"
    )
    failure = CommandFailureCausedUIError(
        "HTTP 409", cmd=["curl"], stdout=json.dumps({"error": message})
    )
    replies: list[CommandFailureCausedUIError | SimpleNamespace] = [failure] * (
        3 if outcome == "exhausted" else 1
    )
    if outcome == "retry_then_success":
        replies.append(
            SimpleNamespace(stdout='{"retired":7,"environment_id":"expected"}')
        )
    execute = Mock(side_effect=replies)
    cleanup = Mock()
    monkeypatch.setattr(composition, "exec", execute)
    monkeypatch.setattr(composition, "kill_fenced_mz", cleanup)
    monkeypatch.setattr("materialize.mzcompose.composition.time.sleep", lambda _: None)

    if outcome == "retry_then_success":
        composition.retire_mz_deployment("survivor", 7, environment_id="expected")
    else:
        with pytest.raises(CommandFailureCausedUIError, match="HTTP 409"):
            composition.retire_mz_deployment("survivor", 7, environment_id="expected")

    assert execute.call_count == len(replies)
    assert all(call == execute.call_args_list[0] for call in execute.call_args_list)
    cleanup.assert_not_called()


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
    skipped = cases["pre-native"].find("skipped")
    assert skipped is not None
    assert skipped.attrib["message"] == reason
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
    error = report.find(".//error")
    assert error is not None
    assert "real failure" in error.attrib["message"]


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
    assert spec is not None and spec.loader is not None
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


@pytest.mark.parametrize("retirement_fails", [False, True])
def test_current_source_native_lifecycle(legacy_upgrade, retirement_fails):
    c = MagicMock(spec=Composition)
    if retirement_fails:
        c.retire_mz_deployment.side_effect = UIError("retirement failed")
        with pytest.raises(UIError, match="retirement failed"):
            legacy_upgrade.test_upgrade_from_version(c, "current_source", "selected")
    else:
        legacy_upgrade.test_upgrade_from_version(c, "current_source", "selected")

    deployments = [
        invocation.args[0]
        for invocation in c.override.call_args_list
        if isinstance(invocation.args[0], legacy_upgrade.Materialized)
    ]
    assert [deployment.name for deployment in deployments] == [
        "materialized",
        "materialized2",
    ]
    for generation, deployment in enumerate(deployments):
        assert "pubsub" in deployment.config["depends_on"]
        assert "--persist-pubsub-external" in deployment.config["command"]
        assert "--persist-pubsub-url=http://pubsub:6879" in deployment.config["command"]
        assert (
            f"--environment-id={legacy_upgrade.DEFAULT_MZ_ENVIRONMENT_ID}"
            in deployment.config["command"]
        )
        assert f"--deploy-generation={generation}" in deployment.config["command"]

    lifecycle = [
        invocation
        for invocation in c.method_calls
        if invocation[0]
        in {
            "await_mz_deployment_status",
            "promote_mz",
            "retire_mz_deployment",
            "kill_fenced_mz",
        }
    ]
    expected = [
        call.await_mz_deployment_status(
            legacy_upgrade.DeploymentStatus.READY_TO_PROMOTE, "materialized2"
        ),
        call.promote_mz("materialized2", retire_mz_service=None),
        call.await_mz_deployment_status(
            legacy_upgrade.DeploymentStatus.IS_LEADER, "materialized2"
        ),
        call.retire_mz_deployment(
            "materialized2", 0, environment_id=legacy_upgrade.DEFAULT_MZ_ENVIRONMENT_ID
        ),
    ]
    if not retirement_fails:
        expected.append(call.kill_fenced_mz("materialized"))
    assert lifecycle == expected
    assert c.run_testdrive_files.call_count == (1 if retirement_fails else 2)


def test_data_ingest_retirement_tracks_generations_across_service_reuse():
    from materialize.data_ingest import transaction_def
    from materialize.data_ingest.workload import Workload

    c = MagicMock(spec=Composition)
    workload = Workload(azurite=False)
    deploy = transaction_def.ZeroDowntimeDeploy(c, 1, workload, azurite=False)
    expected = []
    for generation in range(3):
        previous = workload.mz_service
        assert list(deploy.generate([])) == [None]
        target = workload.mz_service
        expected.extend(
            [
                call.promote_mz(target, retire_mz_service=None),
                call.retire_mz_deployment(
                    target,
                    generation,
                    environment_id=transaction_def.DEFAULT_MZ_ENVIRONMENT_ID,
                ),
                call.kill_fenced_mz(previous),
            ]
        )
        service = c.override.call_args.args[0]
        assert service.name == target
        assert f"--deploy-generation={generation + 1}" in service.config["command"]

    assert [
        invocation
        for invocation in c.method_calls
        if invocation[0] in {"promote_mz", "retire_mz_deployment", "kill_fenced_mz"}
    ] == expected

    restart = transaction_def.RestartMz(c, 1, workload, azurite=False)
    assert list(restart.generate([])) == [None]
    assert c.retire_mz_deployment.call_count == 3
    assert "--deploy-generation=3" in c.override.call_args.args[0].config["command"]
    for invocation in c.override.call_args_list:
        service = invocation.args[0]
        assert "pubsub" in service.config["depends_on"]
        assert "--persist-pubsub-external" in service.config["command"]
        assert "--persist-pubsub-url=http://pubsub:6879" in service.config["command"]


@pytest.mark.parametrize("external", [False, True])
def test_zippy_storage_topology_keeps_source_actions_eligible(external):
    from datetime import timedelta

    from materialize.zippy.framework import Action, Test
    from materialize.zippy.mz_actions import Mz0dtDeploy
    from materialize.zippy.scenarios import Scenario
    from materialize.zippy.storaged_actions import StoragedStart
    from materialize.zippy.storaged_capabilities import StoragedRunning

    class NeedsStorage(Action):
        @classmethod
        def requires(cls):
            return {StoragedRunning}

        def run(self, c, state):
            c.sql("storage action remains eligible")

    class StorageScenario(Scenario):
        def bootstrap(self):
            return [StoragedStart]

        def actions_with_weight(self):
            actions: dict[type[Action], int] = {NeedsStorage: 1}
            if not external:
                actions[Mz0dtDeploy] = 0
            return actions

        def finalization(self):
            return []

    c = MagicMock(spec=Composition)
    Test(StorageScenario(), actions=2, max_execution_time=timedelta(seconds=10)).run(c)
    c.sql.assert_called_once_with("storage action remains eligible")
    assert c.up.call_args_list == ([call("storaged")] if external else [])


@pytest.mark.parametrize("retirement_fails", [False, True])
def test_zippy_retirement_precedes_cleanup_and_routing(monkeypatch, retirement_fails):
    from materialize.zippy import mz_actions
    from materialize.zippy.framework import Capabilities, State

    c = MagicMock(spec=Composition)
    c.blob_store.return_value = "minio"
    c.is_running.return_value = True
    route = Mock()
    c.attach_mock(route, "route")
    monkeypatch.setattr(mz_actions, "restart_balancerd", route)
    state = State(external_storage=False)
    state.deploy_generation = 2
    if retirement_fails:
        c.retire_mz_deployment.side_effect = UIError("retirement failed")
        with pytest.raises(UIError, match="retirement failed"):
            mz_actions.Mz0dtDeploy(Capabilities()).run(c, state)
        c.kill_fenced_mz.assert_not_called()
        route.assert_not_called()
    else:
        mz_actions.Mz0dtDeploy(Capabilities()).run(c, state)
        c.kill_fenced_mz.assert_called_once_with("materialized")
        route.assert_called_once_with(c, state)
        events = [invocation[0] for invocation in c.method_calls]
        assert events.index("retire_mz_deployment") < events.index("kill_fenced_mz")
        assert events.index("kill_fenced_mz") < events.index("route")
    c.retire_mz_deployment.assert_called_once_with(
        "materialized2", 2, environment_id=mz_actions.DEFAULT_MZ_ENVIRONMENT_ID
    )
    c.promote_mz.assert_called_once_with("materialized2", retire_mz_service=None)
    service = c.override.call_args.args[0]
    assert "--deploy-generation=3" in service.config["command"]
    assert "--persist-pubsub-external" in service.config["command"]


@pytest.mark.parametrize("action_name", ["MzStart", "MzRestart"])
def test_zippy_restart_factories_keep_shared_relay_and_generation(action_name):
    from materialize.zippy import mz_actions
    from materialize.zippy.framework import Capabilities, State

    c = MagicMock(spec=Composition)
    c.blob_store.return_value = "minio"
    state = State(external_storage=False)
    state.mz_service = "materialized2"
    state.deploy_generation = 3
    getattr(mz_actions, action_name)(Capabilities()).run(c, state)
    service = c.override.call_args.args[0]
    assert service.name == "materialized2"
    assert "--deploy-generation=3" in service.config["command"]
    assert "pubsub" in service.config["depends_on"]
    assert "--persist-pubsub-external" in service.config["command"]
    assert "--persist-pubsub-url=http://pubsub:6879" in service.config["command"]
    c.up.assert_called_once_with("materialized2")
    assert call("pubsub") not in c.kill.call_args_list


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize("restore_fails", [False, True])
def test_zippy_restore_quiesces_relay_and_only_external_storage(
    external, restore_fails
):
    from materialize.zippy.backup_and_restore_actions import BackupAndRestore
    from materialize.zippy.framework import Capabilities, State

    c = MagicMock(spec=Composition)
    c.blob_store.return_value = "minio"
    c.metadata_store.return_value = "cockroach"
    c.restore.side_effect = lambda *args: Composition.restore(c, *args)

    def execute(*args):
        if restore_fails and args[0] == "persistcli":
            raise UIError("blob restoration failed")

    c.exec.side_effect = execute
    state = State(external_storage=external)
    state.mz_service = "materialized2"
    state.deploy_generation = 3
    action = BackupAndRestore(Capabilities())
    if restore_fails:
        with pytest.raises(UIError, match="blob restoration failed"):
            action.run(c, state)
    else:
        action.run(c, state)

    # Exercise the real Composition/Cockroach restore helpers, mocking only I/O.
    events = []
    for invocation in c.method_calls:
        name, args, _ = invocation
        if name == "kill":
            events.append(("stop", args[0]))
        elif name == "backup":
            events.append(("backup",))
        elif name == "exec" and args[0] == "cockroach":
            assert "RESTORE DATABASE" in args[-1]
            events.append(("restore metadata",))
        elif name == "exec" and args[0] == "persistcli":
            assert "restore-blob" in args
            events.append(("restore blobs",))
        elif name == "up" and args[0] in ("materialized2", "storaged"):
            events.append(("start", args[0]))
    expected = [("stop", "storaged")] if external else []
    expected += [
        ("backup",),
        ("stop", "pubsub"),
        ("stop", "materialized2"),
        ("restore metadata",),
        ("restore blobs",),
    ]
    if not restore_fails:
        expected.append(("start", "materialized2"))
        if external:
            expected.append(("start", "storaged"))
    assert events == expected
    service = c.override.call_args.args[0]
    assert service.name == "materialized2"
    assert "--deploy-generation=3" in service.config["command"]
    assert "pubsub" in service.config["depends_on"]
    assert "--persist-pubsub-url=http://pubsub:6879" in service.config["command"]


@pytest.mark.parametrize("scenario", ["UserTables", "PostgresCdc"])
def test_zippy_native_replica_override_spans_execution_and_restores(
    monkeypatch, scenario
):
    repo = mzbuild.Repository(MZ_ROOT)
    c = Composition(repo, "zippy", resolve_image_specs=False)
    scope = c.workflows["default"].__globals__
    before = copy.deepcopy(c.compose["services"])
    rm = Mock()
    monkeypatch.setattr(c, "rm", rm)
    for operation in ("up", "sql", "enable_minio_versioning", "pull_if_variable"):
        monkeypatch.setattr(c, operation, Mock())
    monkeypatch.setattr(c, "sql_query", Mock(return_value=[("u10", "u20")]))
    monkeypatch.setitem(scope, "setup_default_ssh_test_connection", Mock())
    shared_options = ["--catalog-config=test", "--catalog-deploy-generation=0"]
    options = Mock(return_value=shared_options)
    monkeypatch.setitem(scope, "native_catalog_options", options)

    def run(composition):
        rm.assert_called_once_with("materialized")
        command = composition.compose["services"]["storaged"]["command"]
        if scenario == "PostgresCdc":
            assert "--catalog-cluster-id=u10" in command
            assert "--catalog-replica-id=u20" in command
            assert all(option in command for option in shared_options)
            assert "--persist-pubsub-url=http://pubsub:6879" in command
        else:
            options.assert_not_called()
        for service in ("materialized", "materialized2"):
            config = composition.compose["services"][service]
            assert "pubsub" in config["depends_on"]
            assert "--persist-pubsub-external" in config["command"]

    execute = Mock(side_effect=run)
    monkeypatch.setitem(scope, "Test", lambda **kwargs: SimpleNamespace(run=execute))
    c.workflows["default"](
        c,
        WorkflowArgumentParser(
            "default", "", ["--scenario", scenario, "--actions", "0"]
        ),
    )
    execute.assert_called_once_with(c)
    for service in ("materialized", "materialized2", "storaged"):
        assert c.compose["services"][service] == before[service]


@pytest.mark.parametrize("retirement_fails", [False, True])
def test_parallel_deploy_retires_captured_generation_before_cleanup(
    monkeypatch, retirement_fails
):
    from random import Random

    from materialize.parallel_workload import action
    from materialize.parallel_workload.executor import Executor

    sleep = Mock()
    monkeypatch.setattr(action.time, "sleep", sleep)
    c = MagicMock(spec=Composition)
    deploy = action.ZeroDowntimeDeployAction(Random(0), c, False, False)
    # Deployment only updates this state. Composition I/O is mocked separately.
    executor = SimpleNamespace(db=SimpleNamespace(retiring_generations=frozenset()))
    for generation in range(3):
        c.reset_mock()
        target = "materialized2" if generation % 2 == 0 else "materialized"
        previous = "materialized" if generation % 2 == 0 else "materialized2"

        def retiring(*args, **kwargs):
            assert executor.db.retiring_generations == frozenset(range(generation + 1))
            if retirement_fails and generation == 2:
                raise UIError("retirement failed")

        c.retire_mz_deployment.side_effect = retiring
        c.promote_mz.side_effect = lambda *args, **kwargs: (
            executor.db.retiring_generations == frozenset(range(generation))
        ) or pytest.fail("promotion alone must not authorize retirement errors")
        if retirement_fails and generation == 2:
            with pytest.raises(UIError, match="retirement failed"):
                deploy.run(cast(Executor, executor))
            c.kill_fenced_mz.assert_not_called()
        else:
            assert deploy.run(cast(Executor, executor))
            c.kill_fenced_mz.assert_called_once_with(previous)
        expected = [
            call.await_mz_deployment_status(
                action.DeploymentStatus.READY_TO_PROMOTE, target, timeout=1800
            ),
            call.promote_mz(target, retire_mz_service=None),
            call.await_mz_deployment_status(action.DeploymentStatus.IS_LEADER, target),
            call.retire_mz_deployment(
                target, generation, environment_id=action.DEFAULT_MZ_ENVIRONMENT_ID
            ),
        ]
        assert [
            invocation
            for invocation in c.method_calls
            if invocation[0]
            in {"await_mz_deployment_status", "promote_mz", "retire_mz_deployment"}
        ] == expected
        events = [invocation[0] for invocation in c.method_calls]
        if "kill_fenced_mz" in events:
            assert events.index("retire_mz_deployment") < events.index("kill_fenced_mz")
        service = c.override.call_args.args[0]
        assert service.name == target
        assert f"--deploy-generation={generation + 1}" in service.config["command"]
        assert "pubsub" in service.config["depends_on"]
        assert "--persist-pubsub-external" in service.config["command"]
        assert "--persist-pubsub-url=http://pubsub:6879" in service.config["command"]
        c.kill.assert_not_called()
    assert sleep.call_count == (2 if retirement_fails else 3)
    assert all(60 <= invocation.args[0] <= 120 for invocation in sleep.call_args_list)


@pytest.mark.parametrize(
    "scenario", ["0dt-deploy", "kill", "backup-restore", "regression"]
)
def test_parallel_launch_keeps_shared_relay_only_for_coexistence(monkeypatch, scenario):
    c = Composition(
        mzbuild.Repository(MZ_ROOT), "parallel-workload", resolve_image_specs=False
    )
    scope = c.workflows["default"].__globals__
    for operation in ("up", "exec", "default_port", "port"):
        monkeypatch.setattr(c, operation, Mock(return_value=6975))
    monkeypatch.setitem(scope, "toxiproxy_start", Mock())
    monkeypatch.setitem(scope, "setup_sql_server_testing", Mock())
    monkeypatch.setitem(scope, "os", SimpleNamespace(_exit=Mock()))

    def run(*args):
        config = c.compose["services"]["materialized"]
        shared = scenario == "0dt-deploy"
        assert ("pubsub" in config["depends_on"]) == shared
        assert ("--persist-pubsub-external" in config["command"]) == shared
        assert (
            "--persist-pubsub-url=http://pubsub:6879" in config["command"]
        ) == shared
        assert ("materialized2" in args[1]) == shared
        assert args[3] == 7 and args[6] == 3

    execute = Mock(side_effect=run)
    monkeypatch.setitem(scope, "run", execute)
    c.workflows["default"](
        c,
        WorkflowArgumentParser(
            "default", "", ["--scenario", scenario, "--runtime", "7", "--threads", "3"]
        ),
    )
    execute.assert_called_once()


@pytest.fixture
def platform_checks(composition, monkeypatch):
    path = MZ_ROOT / "test/platform-checks/mzcompose.py"
    spec = importlib.util.spec_from_file_location("platform_checks_workflow", path)
    assert spec is not None and spec.loader is not None
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

    class DNativeFailure(Scenario):
        def actions(self):
            return [mzcompose_actions.StartMz(self)]

    monkeypatch.setattr(
        platform_checks,
        "all_subclasses",
        lambda cls: (
            {AUnsupported, BNative, CNativeForced, DNativeFailure}
            if cls is Scenario
            else set()
        ),
    )

    sql_report = tmp_path / "sql-failure.xml"
    sql_report.write_text(
        '<testsuite><testcase name="check.td"><failure message="check.td:23:1: native SQL assertion failed"/></testcase></testsuite>'
    )

    def execute(action, executor):
        if isinstance(action, mzcompose_actions.StartMz) and isinstance(
            action.scenario, DNativeFailure
        ):
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
    with pytest.raises(UIError, match="DNativeFailure failed"):
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
    assert [case.attrib["name"] for case in failures] == ["DNativeFailure/check.td:23"]
    error = failures[0].find("error")
    assert error is not None and error.text is not None
    assert "native SQL assertion failed" in error.text
    assert composition.test_results["AUnsupported"].skipped_reason
    assert composition.test_results["BNative"].is_successful()
    assert composition.test_results["CNativeForced"].skipped_reason
    assert not composition.test_results["CNativeForced"].errors
    assert composition.test_results["DNativeFailure"].errors
    assert composition.test_results["DNativeFailure"].skipped_reason is None
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
    assert spec is not None and spec.loader is not None
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
    for case in skipped:
        skip = case.find("skipped")
        assert skip is not None
        assert "pre-native" in skip.attrib["message"]
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


@pytest.mark.parametrize("mode", ["random", "earliest-to-current"])
def test_pg_cdc_mixed_upgrade_stops_before_certificate_setup(mode):
    path = MZ_ROOT / "test/pg-cdc-old-syntax/mzcompose.py"
    spec = importlib.util.spec_from_file_location("pg_cdc_old_syntax_workflow", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    c = Mock(spec=Composition)
    c.up.side_effect = AssertionError("unsupported upgrade started certificate setup")
    with pytest.raises(SkipTest, match="pre-native"):
        module.workflow_migration_multi_version_upgrade(
            c,
            WorkflowArgumentParser(
                "migration-multi-version-upgrade",
                "",
                [f"--mode={mode}", "--seed=fixed", "--pg-version=17", "basic.td"],
            ),
        )
    assert c.method_calls == []


@pytest.mark.parametrize(
    "scenario_name",
    [
        "RestartClusterdCompute",
        "RestartEnvironmentdClusterdStorage",
        "KillClusterdStorage",
    ],
)
def test_native_compute_plans_declare_inventory_before_launch(scenario_name):
    from materialize.checks import mzcompose_actions, scenarios
    from materialize.checks.executors import MzcomposeExecutor
    from materialize.checks.features import Features

    scenario = getattr(scenarios, scenario_name)(
        [], MzcomposeExecutor(Mock()), Features([])
    )
    plan = scenario.actions()
    declaration = next(
        i
        for i, a in enumerate(plan)
        if isinstance(a, mzcompose_actions.UseClusterdCompute)
    )
    starts = [
        (i, a)
        for i, a in enumerate(plan)
        if isinstance(a, mzcompose_actions.StartClusterdCompute)
    ]
    assert starts and all(declaration < i for i, _ in starts)
    assert all(action is starts[0][1] for _, action in starts)


@pytest.mark.parametrize("preserve_ports", [False, True])
def test_native_compute_start_retains_inventory_and_shared_context(
    composition, monkeypatch, preserve_ports
):
    from materialize.checks import mzcompose_actions

    c = composition
    c.preserve_ports = preserve_ports
    c.repo.images = {
        "materialized": Mock(docker_name=Mock(return_value="materialize/materialized"))
    }
    c.compose["services"]["clusterd_compute_1"] = {}
    lookup = Mock(return_value=[("u1", "u2")])
    monkeypatch.setattr(c, "sql_query", lookup)
    monkeypatch.setattr(c, "pull_if_variable", Mock())
    launches = []
    monkeypatch.setattr(
        c,
        "up",
        lambda *args: launches.append(
            copy.deepcopy(c.compose["services"]["clusterd_compute_1"])
        ),
    )
    context = Mock(
        return_value=["--catalog-deploy-generation=7", "--catalog-config={}"]
    )
    monkeypatch.setattr(mzcompose_actions, "native_catalog_options", context)
    executor = Mock()
    executor.mzcompose_composition.return_value = c
    start = mzcompose_actions.StartClusterdCompute()
    start.execute(executor)
    lookup.return_value = [("u1", "u3")]
    start.execute(executor)

    first, second = launches
    assert first == second
    assert {
        "--catalog-cluster-id=u1",
        "--catalog-replica-id=u2",
        *context.return_value,
    } <= set(first["command"])
    assert first["ports"] == [
        f"127.0.0.1:{port if preserve_ports else ''}:{port}"
        for port in (2100, 2101, 6878)
    ]
    lookup.assert_called_once()
    context.assert_called_once_with(c)


def test_native_compute_start_requires_declared_inventory(monkeypatch):
    from materialize.checks import mzcompose_actions

    c = Mock()
    c.sql_query.return_value = []
    executor = Mock()
    executor.mzcompose_composition.return_value = c
    with pytest.raises(ValueError):
        mzcompose_actions.StartClusterdCompute().execute(executor)
    c.override.assert_not_called()
    c.up.assert_not_called()


def test_historical_compute_start_keeps_its_original_interface(monkeypatch):
    from materialize.checks import mzcompose_actions

    c = Mock()
    c.override.side_effect = lambda *args: nullcontext()
    context = Mock(
        side_effect=AssertionError("historical launch requested native context")
    )
    monkeypatch.setattr(mzcompose_actions, "native_catalog_options", context)
    executor = Mock()
    executor.mzcompose_composition.return_value = c
    mzcompose_actions.StartClusterdCompute(tag=MzVersion.parse_mz("v26.30.0")).execute(
        executor
    )
    service = c.override.call_args.args[0]
    assert service.config["image"] == "materialize/clusterd:v26.30.0"
    assert not any(arg.startswith("--catalog-") for arg in service.config["command"])
    c.sql_query.assert_not_called()
    context.assert_not_called()
