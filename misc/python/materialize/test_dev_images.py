# Copyright Materialize, Inc. and contributors. All rights reserved.
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.

import json
from types import SimpleNamespace
from typing import cast

import pytest

from materialize import ci_util, mzbuild, ui


@pytest.mark.parametrize("lto,profile", [("1", "release"), ("0", "optimized")])
@pytest.mark.parametrize("sanitizer", ["none", "address"])
def test_profile_tags_preserve_legacy_tags(monkeypatch, lto, profile, sanitizer):
    monkeypatch.setenv("CI_LTO", lto)
    monkeypatch.setenv("CI_SANITIZER", sanitizer)
    monkeypatch.setattr(ci_util, "get_mz_version", lambda: "26.51.0-dev.0")
    monkeypatch.setattr(ci_util.git, "rev_parse", lambda _: "abcdef")
    suffix = "pr" if sanitizer == "none" else "pr-address"
    legacy = f"v26.51.0-dev.0--{suffix}.gabcdef"
    assert ci_util.dev_docker_tag() == legacy
    assert ci_util.dev_docker_tag(include_profile=True) == f"{legacy}.{profile}"


def manifest(arch, digest):
    return {
        "Descriptor": {
            "platform": {"os": "linux", "architecture": arch},
            "digest": digest,
        }
    }


@pytest.mark.parametrize(
    "problem", [None, "wrong-profile", "missing-arch", "extra-arch"]
)
def test_verify_runtime_image_digests(monkeypatch, problem):
    arm, amd = manifest("arm64", "arm-lto"), manifest("amd64", "amd-lto")
    destination = [arm, amd]
    if problem == "wrong-profile":
        destination = [manifest("arm64", "arm-optimized"), amd]
    elif problem == "missing-arch":
        destination = [arm]
    elif problem == "extra-arch":
        destination = [arm, amd, manifest("arm", "other")]
    responses = {
        "runtime:release": destination,
        "runtime:arm-fingerprint": arm,
        "runtime:amd-fingerprint": amd,
    }
    monkeypatch.setattr(
        mzbuild.spawn, "capture", lambda args: json.dumps(responses[args[-1]])
    )
    sources = ["runtime:arm-fingerprint", "runtime:amd-fingerprint"]
    if problem:
        with pytest.raises(ui.UIError, match="Image digest mismatch"):
            mzbuild.verify_multiarch_image("runtime:release", sources)
    else:
        mzbuild.verify_multiarch_image("runtime:release", sources)


def test_existing_tag_is_verified_before_acceptance(monkeypatch):
    monkeypatch.delenv("GITHUB_GHCR_TOKEN", raising=False)
    monkeypatch.setattr(mzbuild, "is_docker_image_pushed", lambda _: True)

    def fail(name, sources):
        assert name == "materialize/environmentd:release"
        assert sources == ["arm-fingerprint", "amd-fingerprint"]
        raise ui.UIError("existing tag has the wrong profile")

    monkeypatch.setattr(mzbuild, "verify_multiarch_image", fail)
    image = SimpleNamespace(
        name="environmentd", docker_name=lambda tag: f"materialize/environmentd:{tag}"
    )
    deps = [
        [SimpleNamespace(image=image, spec=lambda: "arm-fingerprint")],
        [SimpleNamespace(image=image, spec=lambda: "amd-fingerprint")],
    ]
    with pytest.raises(ui.UIError, match="wrong profile"):
        mzbuild.publish_multiarch_images(
            "release", cast(list[list[mzbuild.ResolvedImage]], deps), verify=True
        )
