# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from materialize.antithesis import images


@pytest.mark.parametrize(
    "registry",
    [
        "ghcr.io/example",
        "docker.io/example",
        images.CONFIG_REGISTRY + "-other",
    ],
)
def test_config_rejects_other_registries_before_building(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    registry: str,
) -> None:
    monkeypatch.setenv("MZ_CI_LICENSE_KEY", "test-license")
    monkeypatch.setattr(
        sys, "argv", ["images", "--registry", registry, "--commit", "abc", "--config"]
    )
    acquire, run = Mock(), Mock()
    monkeypatch.setattr(images, "acquire", acquire)
    monkeypatch.setattr(images.spawn, "runv", run)
    with pytest.raises(SystemExit) as error:
        images.main()
    assert error.value.code == 2
    assert "Antithesis CI registry" in capsys.readouterr().err
    acquire.assert_not_called()
    run.assert_not_called()


def test_config_requires_license_before_building(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("MZ_CI_LICENSE_KEY", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        ["images", "--registry", images.CONFIG_REGISTRY, "--commit", "abc", "--config"],
    )
    acquire, run = Mock(), Mock()
    monkeypatch.setattr(images, "acquire", acquire)
    monkeypatch.setattr(images.spawn, "runv", run)
    with pytest.raises(SystemExit) as error:
        images.main()
    assert error.value.code == 2
    assert "MZ_CI_LICENSE_KEY must be set" in capsys.readouterr().err
    acquire.assert_not_called()
    run.assert_not_called()


def test_image_publication_without_config_does_not_require_a_license(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("MZ_CI_LICENSE_KEY", raising=False)
    monkeypatch.setattr(
        sys, "argv", ["images", "--registry", "ghcr.io/example", "--commit", "abc"]
    )
    acquire = Mock(return_value={"environmentd": "local-image"})
    publish = Mock(return_value={"environmentd": "published-image"})
    config = Mock()
    monkeypatch.setattr(images, "acquire", acquire)
    monkeypatch.setattr(images, "publish", publish)
    monkeypatch.setattr(images, "publish_config", config)
    assert images.main() == 0
    assert json.loads(capsys.readouterr().out) == {"environmentd": "published-image"}
    config.assert_not_called()


def test_config_cannot_stage_inside_a_public_image_source_checkout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MZ_CI_LICENSE_KEY", "test-license")
    monkeypatch.setattr(images.tempfile, "tempdir", str(tmp_path))
    run = Mock()
    monkeypatch.setattr(images.spawn, "runv", run)
    with pytest.raises(ValueError, match="outside the source checkout"):
        images.publish_config({}, images.CONFIG_REGISTRY, "test", tmp_path)
    run.assert_not_called()
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("upgrade", [False, True])
def test_config_artifact_uses_just_published_images_and_a_private_digest(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, upgrade: bool
) -> None:
    monkeypatch.setenv("MZ_CI_LICENSE_KEY", "test-license")
    root = tmp_path / "checkout"
    root.mkdir()
    artifact = tmp_path / "images.json"
    tag = "v26.49.0-dev.0--antithesis.gabc"
    refs = {name: f"{images.CONFIG_REGISTRY}/{name}:{tag}" for name in images.IMAGES}
    bases = {
        "upgrade-base-version": "v26.48.0",
        **{
            f"upgrade-base-{name}": f"{images.CONFIG_REGISTRY}/{name}:v26.48.0--antithesis.upgrade-base"
            for name in images.UPGRADE_BASE_IMAGES
        },
    }
    monkeypatch.setattr(images, "acquire", Mock(return_value={}))
    monkeypatch.setattr(images, "publish", Mock(return_value=refs.copy()))
    monkeypatch.setattr(images, "publish_upgrade_base", Mock(return_value=bases))
    monkeypatch.setattr(images, "target_tag", lambda commit: tag)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "images",
            "--registry",
            images.CONFIG_REGISTRY,
            "--commit",
            "abc",
            "--root",
            str(root),
            "--config",
            "--output",
            str(artifact),
            *(["--upgrade-base"] if upgrade else []),
        ],
    )
    commands = []
    contexts = []

    def run(command, **kwargs):
        command = list(map(str, command))
        commands.append(command)
        assert "test-license" not in " ".join(command)
        if command[0] == "bin/pyactivate":
            assert command[1:3] == ["-m", "materialize.antithesis.render"]
            assert kwargs["cwd"] == root
            options = dict(zip(command[3::2], command[4::2]))
            assert options["--environmentd-image"] == refs["environmentd"]
            assert options["--clusterd-image"] == refs["clusterd"]
            assert options["--orchestratord-image"] == refs["orchestratord"]
            assert options["--workload-image"] == refs["antithesis-workload"]
            if upgrade:
                assert (
                    options["--upgrade-from-environmentd-image"]
                    == bases["upgrade-base-environmentd"]
                )
                assert (
                    options["--upgrade-from-clusterd-image"]
                    == bases["upgrade-base-clusterd"]
                )
            else:
                assert "--upgrade-from-environmentd-image" not in options
                assert "--upgrade-from-clusterd-image" not in options
            output = Path(options["--output"])
            output.mkdir()
            (output / "manifest.yaml").write_text("kind: List\nitems: []\n")
            contexts.append(output.parent)
            assert not output.parent.is_relative_to(root)
            assert output.parent.stat().st_mode & 0o077 == 0
        elif command[1] == "build":
            context = Path(command[-1])
            assert context == contexts[0]
            assert (
                context / "Dockerfile"
            ).read_text() == "FROM scratch\nCOPY manifests /manifests\n"
            assert {p.name for p in context.iterdir()} == {"Dockerfile", "manifests"}

    monkeypatch.setattr(images.spawn, "runv", run)
    digest = f"{images.CONFIG_REGISTRY}/antithesis-config@sha256:" + "a" * 64
    inspect = Mock(
        return_value=json.dumps(["ghcr.io/example/config@sha256:" + "b" * 64, digest])
    )
    monkeypatch.setattr(images.spawn, "capture", inspect)
    assert images.main() == 0
    expected = {**refs, **(bases if upgrade else {}), "config": digest}
    assert json.loads(artifact.read_text()) == expected
    assert commands[-1] == [
        "docker",
        "push",
        f"{images.CONFIG_REGISTRY}/antithesis-config:{tag}",
    ]
    assert not contexts[0].exists()


@pytest.mark.parametrize("failure", ["render", "build", "push", "digest"])
def test_config_failures_clean_up_and_do_not_publish_an_artifact(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str
) -> None:
    monkeypatch.setenv("MZ_CI_LICENSE_KEY", "test-license")
    refs = {name: f"{images.CONFIG_REGISTRY}/{name}:test" for name in images.IMAGES}
    monkeypatch.setattr(images, "acquire", Mock(return_value={}))
    monkeypatch.setattr(images, "publish", Mock(return_value=refs))
    artifact = tmp_path / "images.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "images",
            "--registry",
            images.CONFIG_REGISTRY,
            "--commit",
            "abc",
            "--config",
            "--output",
            str(artifact),
        ],
    )
    contexts = []
    commands = []

    def run(command, **kwargs):
        command = list(map(str, command))
        commands.append(command)
        if command[0] == "bin/pyactivate":
            output = Path(command[command.index("--output") + 1])
            contexts.append(output.parent)
            if failure == "render":
                raise subprocess.CalledProcessError(1, command)
            output.mkdir()
            (output / "manifest.yaml").write_text("kind: List\nitems: []\n")
        elif command[1] == failure:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(images.spawn, "runv", run)
    monkeypatch.setattr(images.spawn, "capture", Mock(return_value="[]"))
    with pytest.raises((subprocess.CalledProcessError, RuntimeError)) as error:
        images.main()
    assert "test-license" not in str(error.value)
    assert not artifact.exists()
    assert contexts and not contexts[0].exists()
    if failure in ("render", "build"):
        assert not any(command[:2] == ["docker", "push"] for command in commands)
