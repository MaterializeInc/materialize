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
from materialize.mz_version import MzVersion


def test_upgrade_base_does_not_reuse_images_without_termination_proof() -> None:
    tag = images.upgrade_base_tag(MzVersion.parse_mz("v26.48.2"))
    assert tag.startswith("v26.48.2--antithesis.upgrade-base.halt1.")
    assert len(tag.rsplit(".", 1)[1]) == 16


def test_upgrade_base_cache_changes_with_recorder_and_overlay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "halt_evidence.rs"
    source.write_text(images.HALT_EVIDENCE_SOURCE.read_text())
    monkeypatch.setattr(images, "HALT_EVIDENCE_SOURCE", source)
    version = MzVersion.parse_mz("v26.48.2")
    original = images.upgrade_base_tag(version)
    source.write_text(source.read_text() + "\n")
    recorder_changed = images.upgrade_base_tag(version)
    assert recorder_changed != original
    monkeypatch.setattr(
        images, "UPGRADE_BASE_RECORDER", images.UPGRADE_BASE_RECORDER + "\n"
    )
    assert images.upgrade_base_tag(version) not in (original, recorder_changed)


def test_upgrade_base_patch_preserves_unrelated_release_code(tmp_path: Path) -> None:
    process = tmp_path / "src" / "ore" / "src" / "process.rs"
    process.parent.mkdir(parents=True)
    original = (
        "pub const RELEASE_MARKER: i32 = 168;\n"
        + images.UPGRADE_BASE_ORIGINAL_HALT
        + "\npub fn release_exit() { unsafe { libc::_exit(168) } }\n"
    )
    process.write_text(original)
    images.patch_upgrade_base_halt_proof(tmp_path)
    patched = process.read_text()
    release, recorder = patched.split('\n#[cfg(target_os = "linux")]', 1)
    assert (
        release.replace(
            images.UPGRADE_BASE_RECORDED_HALT, images.UPGRADE_BASE_ORIGINAL_HALT
        )
        == original
    )
    assert images.HALT_EVIDENCE_SOURCE.read_text() in recorder
    assert "antithesis_sdk" not in recorder
    assert "antithesis::" not in patched
    macro = patched.split("macro_rules! halt {", 1)[1].split("\n}", 1)[0]
    assert macro.count("format!") == 1
    assert "::std::file!()" in macro and "::std::line!()" in macro


@pytest.mark.parametrize("kind", ["changed", "duplicate", "already_instrumented"])
def test_upgrade_base_patch_rejects_unreviewed_source(
    tmp_path: Path, kind: str
) -> None:
    process = tmp_path / "src" / "ore" / "src" / "process.rs"
    process.parent.mkdir(parents=True)
    text = images.UPGRADE_BASE_ORIGINAL_HALT
    if kind == "changed":
        text = text.replace("HALT_EXIT_CODE", "RESTART_IN_PLACE_EXIT_CODE")
    elif kind == "duplicate":
        text += "\n" + text
    else:
        text += "\npub fn record_halt() {}"
    process.write_text(text)
    with pytest.raises(ValueError, match="unsupported halt macro"):
        images.patch_upgrade_base_halt_proof(tmp_path)
    assert process.read_text() == text


def test_upgrade_base_build_binds_both_image_fingerprints_to_the_proof(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    worktree = tmp_path / "release"
    commands = []

    def run(command, **kwargs):
        command = list(map(str, command))
        commands.append(command)
        if command[:3] == ["git", "worktree", "add"]:
            process = worktree / "src" / "ore" / "src" / "process.rs"
            process.parent.mkdir(parents=True)
            process.write_text(images.UPGRADE_BASE_ORIGINAL_HALT)
            for name in images.UPGRADE_BASE_IMAGES:
                dockerfile = worktree / "src" / name / "ci" / "Dockerfile"
                dockerfile.parent.mkdir(parents=True)
                dockerfile.write_text("FROM scratch\n")
        elif command[:2] == ["bin/mzimage", "acquire"]:
            assert kwargs["cwd"] == worktree
            assert kwargs["env"]["AWS_LC_SYS_NO_JITTER_ENTROPY"] == "1"
            assert kwargs["env"]["CARGO_TARGET_DIR"] == str(
                worktree / "target-xcompile"
            )
            dockerfile = worktree / "src" / command[-1] / "ci" / "Dockerfile"
            assert images.UPGRADE_BASE_LABEL in dockerfile.read_text()
            assert images.upgrade_base_patch_digest() in dockerfile.read_text()
            assert (
                "MZHALT1"
                in (worktree / "src" / "ore" / "src" / "process.rs").read_text()
            )

    monkeypatch.setattr(images.spawn, "runv", run)
    monkeypatch.setattr(
        images.spawn, "capture", lambda command, **kwargs: f"local/{command[-1]}:proof"
    )
    specs = images.build_upgrade_base(MzVersion.parse_mz("v26.48.2"), worktree)
    assert specs == {name: f"local/{name}:proof" for name in images.UPGRADE_BASE_IMAGES}
    assert len([c for c in commands if c[:2] == ["bin/mzimage", "acquire"]]) == 2


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
