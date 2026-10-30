# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from materialize import mzbuild


@pytest.mark.parametrize("base_state", ["missing", "pull_error", "remote", "local"])
def test_acquire_builds_missing_images_after_their_dependencies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, base_state: str
) -> None:
    monkeypatch.delenv("CI", raising=False)
    available = {"base"} if base_state == "local" else set()
    built = []
    images = []
    for name, publish, dependencies in [
        ("base", True, []),
        ("middle", False, ["base"]),
        ("top", True, ["middle"]),
        ("leaf", False, ["top"]),
    ]:
        path = tmp_path / name
        path.mkdir()
        (path / "mzbuild.yml").write_text(
            json.dumps({"name": name, "publish": publish})
        )
        (path / "Dockerfile").write_text(
            "".join(f"MZFROM {dependency}\n" for dependency in dependencies)
            or "FROM scratch\n"
        )
        images.append(mzbuild.Image(Mock(), path))

    def pull(command: list[str], **kwargs: Any) -> None:
        assert command[:2] == ["docker", "pull"]
        name = command[-1]
        if name == "base" and base_state == "remote":
            available.add(name)
            return
        if base_state == "pull_error":
            raise OSError("registry unavailable")
        raise subprocess.CalledProcessError(1, command)

    def build(
        image: mzbuild.ResolvedImage,
        prep: dict[type[mzbuild.PreImage], Any],
        push: bool = False,
    ) -> None:
        missing = image.dependencies.keys() - available
        assert not missing, f"cannot build {image.name} before {sorted(missing)}"
        available.add(image.name)
        built.append(image.name)

    monkeypatch.setattr(mzbuild, "docker_images", lambda: available)
    monkeypatch.setattr(mzbuild.ResolvedImage, "spec", lambda image: image.name)
    monkeypatch.setattr(mzbuild.ResolvedImage, "build", build)
    monkeypatch.setattr(mzbuild.spawn, "runv", pull)

    mzbuild.DependencySet(images).acquire(max_retries=1)

    assert built == (
        ["middle", "top", "leaf"]
        if base_state in ("local", "remote")
        else ["base", "middle", "top", "leaf"]
    )
    assert available == {"base", "middle", "top", "leaf"}
