# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest

from materialize.antithesis import local


def test_missing_unpulled_platform_blob_retries_a_filtered_archive(monkeypatch):
    image = "registry.example/test:v1"
    imported = []
    monkeypatch.setattr(local, "CLUSTER", "owned")

    def run(command, **kwargs):
        if command[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(command, 0)
        return subprocess.CompletedProcess(
            command, 1, "", "ctr: content digest sha256:unpulled: not found"
        )

    def runv(command, **kwargs):
        if command[:3] == ["docker", "image", "save"]:
            assert command[command.index("--platform") + 1].startswith("linux/")
            Path(command[command.index("--output") + 1]).write_bytes(b"host platform")
        else:
            assert command[:3] == ["kind", "load", "image-archive"]
            assert command[-2:] == ["--name", "owned"]
            assert Path(command[3]).read_bytes() == b"host platform"
            imported.append(command[3])

    monkeypatch.setattr(local.subprocess, "run", run)
    monkeypatch.setattr(local.spawn, "runv", runv)
    local.load_images([image])
    assert len(imported) == 1 and not Path(imported[0]).exists()


def test_unrelated_kind_failure_is_preserved(monkeypatch):
    def run(command, **kwargs):
        if command[0] == "docker":
            return subprocess.CompletedProcess(command, 0)
        return subprocess.CompletedProcess(command, 1, "", "cluster unavailable")

    fallback = Mock()
    monkeypatch.setattr(local.subprocess, "run", run)
    monkeypatch.setattr(local.spawn, "runv", fallback)
    with pytest.raises(subprocess.CalledProcessError) as raised:
        local.load_images(["registry.example/test:v1"])
    assert raised.value.stderr == "cluster unavailable"
    fallback.assert_not_called()
