# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
from contextlib import nullcontext
from subprocess import CompletedProcess
from unittest.mock import Mock

import pytest

from materialize.mzcompose.composition import Composition

OLD = "materialize/materialized:v0.140.0"
NATIVE = "materialize/materialized:mzbuild-known"
UNKNOWN = "custom/materialized:v0.140.0"


@pytest.fixture
def hook(monkeypatch):
    c = Composition.__new__(Composition)
    c.compose = {"services": {"materialized": {"image": OLD}}}
    c._preflight_native_images = {NATIVE}
    c.test_results = {}
    for method in (
        "kill",
        "up",
        "await_mz_deployment_status",
        "promote_mz",
        "sql",
        "capture_logs",
    ):
        setattr(c, method, Mock())
    c.validate_sources_sinks_clusters = Mock(return_value=None)
    c.override = Mock(side_effect=lambda *args: nullcontext())
    c.invoke = Mock(return_value=CompletedProcess([], 0, stdout="container\n"))
    monkeypatch.setenv("CI_FINAL_PREFLIGHT_CHECK_VERSION", "v0.140.0")
    monkeypatch.delenv("CI_FINAL_PREFLIGHT_CHECK_ROLLBACK", raising=False)
    monkeypatch.setattr(
        "materialize.mzcompose.composition.image_registry", lambda: "materialize"
    )
    running = {
        "Image": "sha256:running",
        "Config": {
            "Image": OLD,
            "Entrypoint": ["entrypoint.sh"],
            "Cmd": ["--unsafe-mode"],
            "Env": [],
        },
        "Mounts": [{"Destination": "/mzdata"}],
    }
    image = {
        "Id": "sha256:running",
        "Config": {"Entrypoint": ["entrypoint.sh"], "Env": []},
    }
    inspect = Mock(
        side_effect=lambda args: json.dumps([image if args[1] == "image" else running])
    )
    monkeypatch.setattr("materialize.mzcompose.composition.spawn.capture", inspect)
    return c, running, image, inspect


@pytest.mark.parametrize(
    "source,target,outer,rollback,reason",
    [
        (OLD, NATIVE, NATIVE, False, "source=historical"),
        (NATIVE, OLD, OLD, False, "source=native"),
        (OLD, OLD, NATIVE, False, "restored outer definition=native"),
        (OLD, OLD, OLD, True, "rollback=native"),
        (UNKNOWN, OLD, OLD, False, "cannot classify image provenance"),
        (OLD, "materialize/materialized:latest", OLD, False, "target=unknown"),
        (OLD, OLD, UNKNOWN, False, "restored outer definition=unknown"),
        (
            "materialize/materialized:v26.46.0--pr.gabc123",
            OLD,
            "materialize/materialized:v26.46.0--pr.gabc123",
            False,
            "source=unknown",
        ),
    ],
)
def test_rejected_path_leaves_workload_and_cleanup(
    hook, monkeypatch, source, target, outer, rollback, reason
):
    c, running, _, _ = hook
    running["Config"]["Image"] = source
    c.compose["services"]["materialized"]["image"] = outer
    monkeypatch.setenv("CI_FINAL_PREFLIGHT_CHECK_VERSION", target.rsplit(":", 1)[1])
    monkeypatch.setenv("CI_FINAL_PREFLIGHT_CHECK_ROLLBACK", "1" if rollback else "0")
    with c.test_case("workflow"):
        c.final_preflight_check()
    c.down()
    c.kill.assert_not_called()
    assert c.test_results["workflow"].skipped_reason is None
    assert not c.test_results["workflow"].errors
    for name in ("final-preflight-1", "final-preflight-2"):
        assert reason in c.test_results[name].skipped_reason
    c.kill.assert_not_called()
    c.up.assert_not_called()
    c.override.assert_not_called()
    c.capture_logs.assert_called_once()
    c.invoke.assert_called_with("down", "--volumes", "--remove-orphans")


@pytest.mark.parametrize(
    "ambiguity",
    [
        "mount",
        "entrypoint",
        "retag",
        "environment",
        "command",
        "inspect",
        "outer-mount",
        "outer-entrypoint",
    ],
)
def test_unknown_executable_provenance(hook, ambiguity):
    c, running, image, inspect = hook
    if ambiguity == "mount":
        running["Mounts"].append({"Destination": "/usr/local/bin"})
    elif ambiguity == "entrypoint":
        running["Config"]["Entrypoint"] = []
    elif ambiguity == "retag":
        image["Id"] = "sha256:replacement"
    elif ambiguity == "environment":
        running["Config"]["Env"] = ["MZ_ADAPTERD_IMAGE=/mzdata/adapterd"]
    elif ambiguity == "command":
        running["Config"]["Cmd"] = ["--adapterd-image=/mzdata/adapterd"]
    elif ambiguity == "inspect":
        inspect.side_effect = OSError("Docker unavailable")
    elif ambiguity == "outer-mount":
        c.compose["services"]["materialized"]["volumes"] = ["binary:/usr/local/bin"]
    else:
        c.compose["services"]["materialized"]["entrypoint"] = ["custom"]
    c.final_preflight_check()
    assert (
        "cannot classify image provenance"
        in c.test_results["final-preflight-1"].skipped_reason
    )
    c.kill.assert_not_called()
    c.override.assert_not_called()


@pytest.mark.parametrize(
    "source,rollback", [(OLD, False), (NATIVE, False), (NATIVE, True)]
)
def test_supported_path_retains_preflight_and_restart(
    hook, monkeypatch, source, rollback
):
    c, running, _, _ = hook
    running["Config"]["Image"] = source
    c.compose["services"]["materialized"]["image"] = source
    monkeypatch.setenv("CI_FINAL_PREFLIGHT_CHECK_VERSION", source.rsplit(":", 1)[1])
    monkeypatch.setenv("CI_FINAL_PREFLIGHT_CHECK_ROLLBACK", "1" if rollback else "0")
    c.final_preflight_check()
    assert not c.test_results
    assert c.kill.call_count == 2
    assert c.up.call_count == (3 if rollback else 2)
    assert c.promote_mz.call_count == (0 if rollback else 1)
    c.sql.assert_called_once_with("SELECT 1")
    c.validate_sources_sinks_clusters.assert_called_once()
    c.down()
    c.capture_logs.assert_called_once()
    c.invoke.assert_called_with("down", "--volumes", "--remove-orphans")


@pytest.mark.parametrize(
    "image,family",
    [
        ("ghcr.io/materializeinc/materialize/materialized:v0.140.0", "historical"),
        ("materialize/materialized:mzbuild-unrecognized", "unknown"),
        ("custom/materialized:v0.140.0", "unknown"),
        ("materialize/materialized:latest", "unknown"),
    ],
)
def test_image_provenance_requires_known_artifacts(hook, image, family):
    c, _, _, _ = hook
    assert c._preflight_image_family(image) == family


def test_supported_failure_propagates(hook):
    c, _, _, _ = hook
    c.up.side_effect = RuntimeError("preflight failed")
    with pytest.raises(RuntimeError, match="preflight failed"):
        c.final_preflight_check()
    assert not c.test_results


@pytest.mark.parametrize("absent", ["opt-in", "service", "container"])
def test_noop(hook, monkeypatch, absent):
    c, _, _, inspect = hook
    if absent == "opt-in":
        monkeypatch.delenv("CI_FINAL_PREFLIGHT_CHECK_VERSION")
    elif absent == "service":
        c.compose["services"].clear()
    else:
        c.invoke.return_value.stdout = ""
    c.final_preflight_check()
    inspect.assert_not_called()
    c.kill.assert_not_called()
    assert not c.test_results
