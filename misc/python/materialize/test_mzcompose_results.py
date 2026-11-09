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
import xml.etree.ElementTree as ET
from types import SimpleNamespace
from typing import cast
from unittest import SkipTest
from unittest.mock import MagicMock, Mock

import pytest

from materialize import MZ_ROOT, mzbuild
from materialize.cli.mzcompose import RunCommand
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
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
