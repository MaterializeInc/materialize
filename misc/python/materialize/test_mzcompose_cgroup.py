# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import copy
import subprocess
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
import yaml

from materialize import mzbuild
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service
from materialize.ui import UIError


@pytest.fixture
def composition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[[], Composition]:
    monkeypatch.delenv("MZCOMPOSE_CGROUP_PARENT", raising=False)
    (tmp_path / "mzcompose.py").write_text("""\
from materialize.mzcompose.service import Service

app = Service("app", {"image": "alpine:3.21", "command": ["sleep", "60"]})
app.companions = [Service("companion", {"image": "alpine:3.21"})]
SERVICES = [
    app,
    Service("explicit", {"image": "alpine:3.21", "cgroup_parent": "fixture.slice"}),
]
""")
    repo = cast(
        mzbuild.Repository,
        SimpleNamespace(
            compositions={"cgroup-test": tmp_path},
            rd=SimpleNamespace(coverage=False),
        ),
    )
    return lambda: Composition(
        repo, "cgroup-test", resolve_image_specs=False, silent=True
    )


@pytest.fixture
def emitted(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    configs: list[dict[str, Any]] = []

    def run(cmd: list[Any], **kwargs: Any) -> subprocess.CompletedProcess:
        assert cmd[:2] == ["docker", "compose"]
        path = next(arg[2:] for arg in cmd if str(arg).startswith("-f/dev/fd/"))
        with open(path) as file:
            configs.append(yaml.safe_load(file))
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    return configs


@pytest.mark.parametrize("parent", [None, ""])
def test_default_preserves_explicit_parent(
    composition: Callable[[], Composition],
    emitted: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
    parent: str | None,
) -> None:
    if parent is not None:
        monkeypatch.setenv("MZCOMPOSE_CGROUP_PARENT", parent)
    c = composition()
    original = copy.deepcopy(c.compose)

    c.invoke("config")

    assert emitted == [original]
    assert c.compose == original
    assert emitted[0]["services"]["explicit"]["cgroup_parent"] == "fixture.slice"
    assert "cgroup_parent" not in emitted[0]["services"]["app"]


def test_parent_applies_to_companions_and_replacements(
    composition: Callable[[], Composition],
    emitted: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MZCOMPOSE_CGROUP_PARENT", "fixture.slice")
    c = composition()
    original = copy.deepcopy(c.compose)
    # The setting belongs to the composition, not the invocation's environment.
    monkeypatch.setenv("MZCOMPOSE_CGROUP_PARENT", "other.slice")

    c.invoke("config")
    c.invoke("up", "-d", "--force-recreate")
    assert emitted[0] == emitted[1]
    assert c.compose == original

    with c.override(
        Service("app", {"image": "alpine:3.21", "command": ["sleep", "120"]})
    ):
        overridden = copy.deepcopy(c.compose)
        c.invoke("up", "-d", "--force-recreate")
        assert emitted[-1]["services"]["app"]["command"] == ["sleep", "120"]
        assert c.compose == overridden

    c.invoke("up", "-d", "--force-recreate")
    assert emitted[-1] == emitted[0]
    assert c.compose == original
    for config in emitted:
        assert set(config["services"]) == {"app", "explicit", "companion"}
        assert {
            service["cgroup_parent"] for service in config["services"].values()
        } == {"fixture.slice"}


def test_conflicting_explicit_parent_rejected(
    composition: Callable[[], Composition],
    emitted: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MZCOMPOSE_CGROUP_PARENT", "other.slice")
    c = composition()
    original = copy.deepcopy(c.compose)

    with pytest.raises(UIError) as error:
        c.invoke("up", "-d")

    for detail in ("explicit", "fixture.slice", "other.slice"):
        assert detail in str(error.value)
    assert emitted == []
    assert c.compose == original
