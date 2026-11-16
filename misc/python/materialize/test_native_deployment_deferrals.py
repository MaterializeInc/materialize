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
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import SkipTest
from unittest.mock import Mock

import pytest

from materialize import MZ_ROOT
from materialize.checks.mzcompose_actions import StartMz
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import WorkflowArgumentParser


@pytest.fixture(scope="module")
def launchers():
    modules = {}
    for name in ("0dt", "platform-checks", "orchestratord", "terraform"):
        spec = importlib.util.spec_from_file_location(
            name, MZ_ROOT / "test" / name / "mzcompose.py"
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        modules[name] = module
    return modules


@pytest.mark.parametrize("kind", ["replacement", "evolution"])
def test_positive_migrations_are_unsupported_before_resources(launchers, kind):
    c = Mock()
    with pytest.raises(SkipTest, match=f"migration \\({kind}\\)"):
        getattr(launchers["0dt"], f"workflow_builtin_schema_migrations_{kind}")(c)
    assert not c.mock_calls
    with pytest.raises(SkipTest, match="positive native builtin"):
        launchers["platform-checks"].check_native_upgrade_boundary(
            [StartMz(Mock(), force_migrations=kind)]
        )


def test_compatible_native_and_historical_plans_remain_supported(launchers):
    guard = launchers["platform-checks"].check_native_upgrade_boundary
    guard([StartMz(Mock(), deploy_generation=i) for i in range(4)])
    guard(
        [
            StartMz(Mock(), tag=MzVersion.parse_mz(tag), force_migrations="replacement")
            for tag in ("v26.0.0", "v26.1.0")
        ]
    )
    with pytest.raises(SkipTest, match="pre-native"):
        guard([StartMz(Mock(), tag=MzVersion.parse_mz("v26.1.0")), StartMz(Mock())])


@pytest.mark.parametrize("name", ["orchestratord", "terraform"])
@pytest.mark.parametrize("tag", [None, "branch-image", "v26.1.0"])
def test_deployment_tag_boundary(launchers, monkeypatch, name, tag):
    module = launchers[name]
    monkeypatch.setattr(module.ci_util, "dev_docker_tag", lambda: "branch-image")
    if tag == "v26.1.0":
        module.check_native_deployment(tag)
    else:
        with pytest.raises(SkipTest, match="Unsupported: current-native"):
            module.check_native_deployment(tag)


@pytest.mark.parametrize("action", ["noop", "upgrade", "upgrade-chain"])
def test_operator_default_preserves_historical_loops(launchers, action):
    module = launchers["orchestratord"]
    args = SimpleNamespace(tag=None, scenario=None, action=action)
    if action == "noop":
        with pytest.raises(SkipTest):
            module.check_default_deployment(args)
    else:
        module.check_default_deployment(args)


@pytest.mark.parametrize("native_step", [False, True])
def test_explicit_operator_scenario_checks_each_step(launchers, native_step):
    steps = [[{"modification": "EnvironmentdImageRef", "value": "v26.1.0"}]]
    steps.append([] if native_step else steps[0])
    args = SimpleNamespace(tag=None, scenario=json.dumps(steps), action="upgrade")
    guard = launchers["orchestratord"].check_default_deployment
    if native_step:
        with pytest.raises(SkipTest):
            guard(args)
    else:
        guard(args)


@pytest.mark.parametrize("copy_first", [True, False])
def test_operator_secondary_environment_retains_its_image(launchers, copy_first):
    mods = [
        {"modification": "NumMaterializeEnvironments", "value": 2},
        {"modification": "EnvironmentdImageRef", "value": "v26.1.0"},
    ]
    if not copy_first:
        mods.reverse()
    args = SimpleNamespace(tag=None, scenario=json.dumps([mods]), action="upgrade")
    guard = launchers["orchestratord"].check_default_deployment
    if copy_first:
        with pytest.raises(SkipTest, match="current-native"):
            guard(args)
    else:
        guard(args)


@pytest.mark.parametrize(
    "name,workflow",
    [
        ("terraform", name)
        for name in (
            "aws_temporary",
            "aws_upgrade",
            "aws_persistent_setup",
            "aws_persistent_test",
            "gcp_temporary",
            "azure_temporary",
        )
    ]
    + [
        ("orchestratord", name)
        for name in (
            "default",
            "upgrade_downtime",
            "v1_opt_in",
            "server_side_apply",
            "webhook_cert_rotation",
            "manually_promote",
            "orchestratord_upgrade",
            "revert_rollout",
            "rollout_timeout",
            "leader_failover",
            "oidc_auth",
            "clusterd_generation_scheduling",
        )
    ],
)
def test_native_workflows_stop_before_provisioning(
    launchers, monkeypatch, name, workflow
):
    module = launchers[name]
    c = Mock()
    setup = Mock(side_effect=AssertionError("provisioning must not run"))
    monkeypatch.setattr(module, "setup" if name == "orchestratord" else "AWS", setup)
    with pytest.raises(SkipTest, match="Unsupported: current-native"):
        getattr(module, f"workflow_{workflow}")(
            c, WorkflowArgumentParser(workflow, "", [])
        )
    assert not c.mock_calls
    setup.assert_not_called()


def test_documentation_skip_does_not_hide_historical_deployment(launchers, monkeypatch):
    module = launchers["orchestratord"]
    current = MzVersion.parse_mz("v26.2.0")
    old = MzVersion.parse_mz("v26.1.0")
    monkeypatch.setattr(module, "get_version", lambda tag: current)
    monkeypatch.setattr(module, "get_self_managed_versions", lambda: [old])
    monkeypatch.setattr(module.buildkite, "shard_list", lambda values, key: values)
    skipped = []

    @contextmanager
    def test_case(name):
        try:
            yield
        except SkipTest as error:
            skipped.append((name, str(error)))

    # Stop at the first historical resource acquisition, before any real I/O.
    c = Mock()
    c.test_case.side_effect = test_case
    c.up.side_effect = RuntimeError("historical deployment reached")
    with pytest.raises(RuntimeError, match="historical deployment reached"):
        module.workflow_documentation_defaults(
            c, WorkflowArgumentParser("documentation-defaults", "", [])
        )
    assert len(skipped) == 1
    assert skipped[0][0] == f"documentation-defaults/{current}"
    assert "Unsupported" in skipped[0][1]
    c.up.assert_called_once()


def test_version_bumped_native_upgrade_is_not_deferred(launchers):
    from materialize.checks.features import Features
    from materialize.checks.scenarios_zero_downtime import ZeroDowntimeBumpedVersion

    scenario = ZeroDowntimeBumpedVersion([], Mock(), Features([]))
    # Inspect the real plan without executing its builds or source mutations.
    launchers["platform-checks"].check_native_upgrade_boundary(scenario.actions())


@pytest.mark.parametrize("action", ["upgrade", "upgrade-chain"])
def test_historical_operator_workflow_reaches_setup(launchers, monkeypatch, action):
    module = launchers["orchestratord"]
    setup = Mock(side_effect=RuntimeError("historical setup reached"))
    monkeypatch.setattr(module, "setup", setup)
    with pytest.raises(RuntimeError, match="historical setup reached"):
        module.workflow_default(
            Mock(), WorkflowArgumentParser("default", "", ["--action", action])
        )
    setup.assert_called_once()


@pytest.mark.parametrize("workflow", ["balancer", "failure_events"])
def test_operator_only_workflows_are_not_deferred(launchers, monkeypatch, workflow):
    module = launchers["orchestratord"]
    setup = Mock(side_effect=RuntimeError("operator setup reached"))
    monkeypatch.setattr(module, "setup", setup)
    with pytest.raises(RuntimeError, match="operator setup reached"):
        getattr(module, f"workflow_{workflow}")(
            Mock(), WorkflowArgumentParser(workflow, "", [])
        )
    setup.assert_called_once()


def test_cloudtest_deferrals_precede_application_creation(monkeypatch, request):
    for filename in ("conftest.py", "test_upgrade.py"):
        spec = importlib.util.spec_from_file_location(
            f"cloudtest_{filename}", MZ_ROOT / "test/cloudtest" / filename
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        application = Mock(side_effect=AssertionError("must not provision Kubernetes"))
        monkeypatch.setattr(module, "MaterializeApplication", application)
        if filename == "conftest.py":
            module.pytest_configure(request.config)
            with pytest.raises(pytest.skip.Exception, match="Unsupported"):
                module.mz.__wrapped__(Mock())
        else:
            marks = [
                mark for mark in module.test_upgrade.pytestmark if mark.name == "skip"
            ]
            assert len(marks) == 1
            assert "Unsupported" in marks[0].kwargs["reason"]
        application.assert_not_called()
