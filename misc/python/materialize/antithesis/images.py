# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The images the Antithesis harness runs, and publishing them under one tag.

orchestratord derives the clusterd image from the environmentd reference by
swapping the image name, so environmentd and clusterd must share a repository
prefix and tag. mzbuild tags each image by its own fingerprint, so `publish`
retags every image to a common tag.

The upgrade scenario also needs environmentd and clusterd at the release the
environment starts on. The published release images seed AWS-LC from CPU
jitter, which aborts on Antithesis's deterministic CPU, so `build_upgrade_base`
rebuilds them from the release tag with `rustc_flags.antithesis_env`. They carry
no coverage instrumentation: the release tag has no Antithesis flavor.

`python -m materialize.antithesis.images --registry R --commit C` acquires the
images, pushes them to `R/<name>:<target_tag(C)>`, and prints the references as
JSON. With
`--upgrade-base`, it also builds and pushes the base images for the release
`upgrade_base_version` picks, unless the registry already has them. It expects
`docker login` for the registry to have happened already.
`--config` adds a config image pinned to its pushed digest, and requires
`MZ_CI_LICENSE_KEY` and the destination `CONFIG_REGISTRY`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from materialize import MZ_ROOT, mzbuild, rustc_flags, spawn
from materialize.mz_version import MzVersion
from materialize.version_list import get_published_minor_mz_versions
from materialize.xcompile import Arch

IMAGES = ["environmentd", "clusterd", "orchestratord", "antithesis-workload"]
UPGRADE_BASE_IMAGES = ["environmentd", "clusterd"]
CONFIG_REGISTRY = (
    "us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository"
)

# Appended to the base images' Dockerfiles at the release tag. The build
# environment is not part of an mzbuild fingerprint, so without a source change
# `mzimage acquire` would pull the published release image instead of building.
UPGRADE_BASE_LABEL = "LABEL io.materialize.antithesis.aws-lc-jitter-entropy=disabled"
HALT_EVIDENCE_SOURCE = MZ_ROOT / "src" / "ore" / "src" / "halt_evidence.rs"
UPGRADE_BASE_ORIGINAL_HALT = r"""macro_rules! halt {
    ($($arg:expr),* $(,)?) => {{
        $crate::__private::tracing::warn!("halting process: {}", format!($($arg),*));
        $crate::process::exit_thread_safe($crate::process::HALT_EXIT_CODE);
    }}
}"""
UPGRADE_BASE_RECORDED_HALT = r"""macro_rules! halt {
    ($($arg:expr),* $(,)?) => {{
        let message = format!($($arg),*);
        $crate::__private::tracing::warn!("halting process: {}", message);
        $crate::process::record_halt(
            $crate::process::HALT_EXIT_CODE,
            ::std::concat!(::std::file!(), ":", ::std::line!()),
            &message,
        );
        $crate::process::exit_thread_safe($crate::process::HALT_EXIT_CODE);
    }}
}"""
UPGRADE_BASE_RECORDER = """#[cfg(target_os = "linux")]
mod halt_evidence {
%s
}

/// Records bounded halt evidence in Antithesis upgrade-base builds.
#[doc(hidden)]
pub fn record_halt(code: i32, location: &str, message: &str) {
    #[cfg(target_os = "linux")]
    halt_evidence::record(code, location, message);
    #[cfg(not(target_os = "linux"))]
    let _ = (code, location, message);
}
"""


def upgrade_base_patch_digest() -> str:
    sources = [
        HALT_EVIDENCE_SOURCE.read_text(),
        UPGRADE_BASE_ORIGINAL_HALT,
        UPGRADE_BASE_RECORDED_HALT,
        UPGRADE_BASE_RECORDER,
        UPGRADE_BASE_LABEL,
    ]
    return hashlib.sha256("\0".join(sources).encode()).hexdigest()[:16]


def patch_upgrade_base_halt_proof(worktree: Path) -> None:
    """Add only the halt recorder to the freshly checked-out release source."""
    path = worktree / "src" / "ore" / "src" / "process.rs"
    original = path.read_text()
    if (
        original.count(UPGRADE_BASE_ORIGINAL_HALT) != 1
        or "pub fn record_halt(" in original
    ):
        raise ValueError("upgrade-base process.rs has an unsupported halt macro")
    recorded = original.replace(UPGRADE_BASE_ORIGINAL_HALT, UPGRADE_BASE_RECORDED_HALT)
    path.write_text(
        recorded + "\n" + UPGRADE_BASE_RECORDER % HALT_EVIDENCE_SOURCE.read_text()
    )


def acquire(root: Path, arch: Arch = Arch.host()) -> dict[str, str]:
    """Build or pull the Antithesis flavor of each image and return its spec."""
    repo = mzbuild.Repository(root, arch=arch, antithesis=True)
    deps = repo.resolve_dependencies(repo.images[name] for name in IMAGES)
    deps.acquire()
    return {name: deps[name].spec() for name in IMAGES}


def publish(specs: dict[str, str], prefix: str, tag: str, push: bool) -> dict[str, str]:
    """Tag each image as `prefix/<name>:tag`, pushing it if `push`."""
    refs = {}
    for name, spec in specs.items():
        ref = f"{prefix}/{name}:{tag}"
        spawn.runv(["docker", "tag", spec, ref])
        if push:
            spawn.runv(["docker", "push", ref])
        refs[name] = ref
    return refs


def upgrade_base_version() -> MzVersion:
    """The newest release of the previous minor version.

    The same choice platform-checks makes for `UpgradeEntireMz`. persist only
    lets a `-dev` build write over data from an older release, so the current
    minor's own releases are excluded.
    """
    return get_published_minor_mz_versions(
        exclude_current_minor_version=True,
        max_version=MzVersion.parse_cargo(),
        limit=1,
    )[0]


def image_tag(version: MzVersion, build: str) -> str:
    """A Docker tag orchestratord parses as `version`.

    orchestratord gates environmentd arguments and upgrade checks on the version
    in the image tag, and treats a tag it cannot parse as satisfying every gate.
    Docker tags cannot contain `+`, so build metadata follows `--`.
    """
    return f"{version}--{build}"


def target_tag(commit: str) -> str:
    return image_tag(MzVersion.parse_cargo(), f"antithesis.g{commit[:12]}")


def upgrade_base_tag(version: MzVersion) -> str:
    return image_tag(
        version, f"antithesis.upgrade-base.halt1.{upgrade_base_patch_digest()}"
    )


def build_upgrade_base(
    version: MzVersion, worktree: Path, arch: Arch = Arch.host()
) -> dict[str, str]:
    """Build environmentd and clusterd at the release tag and return their specs.

    Checks the tag out afresh into `worktree`, replacing whatever is there, and
    builds with that revision's own mzbuild.
    """
    # CI agents keep the checkout between builds, and `git clean` empties this
    # ignored directory without unregistering the worktree, so a leftover is
    # never reused.
    shutil.rmtree(worktree, ignore_errors=True)
    spawn.runv(["git", "worktree", "prune"], cwd=MZ_ROOT)
    spawn.runv(
        ["git", "worktree", "add", "--detach", str(worktree), str(version)],
        cwd=MZ_ROOT,
    )
    patch_upgrade_base_halt_proof(worktree)
    proof_label = (
        f"LABEL io.materialize.antithesis.halt-proof={upgrade_base_patch_digest()}"
    )
    for name in UPGRADE_BASE_IMAGES:
        dockerfile = worktree / "src" / name / "ci" / "Dockerfile"
        text = dockerfile.read_text()
        dockerfile.write_text(
            text.rstrip("\n") + "\n" + UPGRADE_BASE_LABEL + "\n" + proof_label + "\n"
        )
    env = {
        **os.environ,
        **rustc_flags.antithesis_env,
        # mzbuild refuses to build publishable images in CI by default.
        "CI_ALLOW_LOCAL_BUILD": "1",
        # The CI builder points `CARGO_TARGET_DIR` at the main checkout's
        # target directory. The release's mzbuild looks for binaries under its
        # own root, and sharing the directory would overwrite this checkout's.
        "CARGO_TARGET_DIR": str(worktree / "target-xcompile"),
    }
    specs = {}
    for name in UPGRADE_BASE_IMAGES:
        spawn.runv(
            ["bin/mzimage", "acquire", "--arch", str(arch), name], cwd=worktree, env=env
        )
        specs[name] = (
            spawn.capture(
                ["bin/mzimage", "spec", "--arch", str(arch), name],
                cwd=worktree,
                env=env,
            )
            .strip()
            .splitlines()[-1]
        )
    return specs


def registry_has(refs: list[str]) -> bool:
    return all(
        subprocess.run(
            ["docker", "manifest", "inspect", ref], capture_output=True
        ).returncode
        == 0
        for ref in refs
    )


def publish_upgrade_base(registry: str, arch: Arch) -> dict[str, str]:
    """Push the base images to `registry`, building them only if it lacks them.

    Returns the references keyed `upgrade-base-<name>`, plus the release under
    `upgrade-base-version`.
    """
    version = upgrade_base_version()
    tag = upgrade_base_tag(version)
    refs = {name: f"{registry}/{name}:{tag}" for name in UPGRADE_BASE_IMAGES}
    if registry_has(list(refs.values())):
        print(f"--- Upgrade base images for {version} already in {registry}")
    else:
        print(f"--- Building upgrade base images for {version}")
        specs = build_upgrade_base(
            version, MZ_ROOT / "target-antithesis-upgrade-base", arch
        )
        refs = publish(specs, registry, tag, push=True)
    return {
        "upgrade-base-version": str(version),
        **{f"upgrade-base-{name}": ref for name, ref in refs.items()},
    }


def config_publish_preflight(registry: str) -> None:
    """Validate the config image destination and required license environment."""
    # Config images contain the license, so restrict their destination to CI's registry.
    if registry != CONFIG_REGISTRY:
        raise ValueError(
            "config images must be published to the Antithesis CI registry"
        )
    if not os.environ.get("MZ_CI_LICENSE_KEY"):
        raise ValueError("MZ_CI_LICENSE_KEY must be set to publish a config image")


def publish_config(refs: dict[str, str], registry: str, tag: str, root: Path) -> str:
    """Publish a rendered config against the given image references and return its digest."""
    config_publish_preflight(registry)
    base_keys = ("upgrade-base-environmentd", "upgrade-base-clusterd")
    if (base_keys[0] in refs) != (base_keys[1] in refs):
        raise ValueError("the upgrade-base images must be given together")
    ref = f"{registry}/antithesis-config:{tag}"
    with tempfile.TemporaryDirectory(prefix="mz-antithesis-config-") as directory:
        context = Path(directory)
        if context.resolve().is_relative_to(root.resolve()):
            raise ValueError("config staging must be outside the source checkout")
        command: list[str | Path] = [
            "bin/pyactivate",
            "-m",
            "materialize.antithesis.render",
            "--output",
            context / "manifests",
            "--environmentd-image",
            refs["environmentd"],
            "--clusterd-image",
            refs["clusterd"],
            "--orchestratord-image",
            refs["orchestratord"],
            "--workload-image",
            refs["antithesis-workload"],
        ]
        if base_keys[0] in refs:
            command.extend(
                [
                    "--upgrade-from-environmentd-image",
                    refs[base_keys[0]],
                    "--upgrade-from-clusterd-image",
                    refs[base_keys[1]],
                ]
            )
        spawn.runv(command, cwd=root)
        (context / "Dockerfile").write_text("FROM scratch\nCOPY manifests /manifests\n")
        spawn.runv(["docker", "build", "--tag", ref, context])
        spawn.runv(["docker", "push", ref])
    digests = json.loads(
        spawn.capture(
            ["docker", "image", "inspect", ref, "--format", "{{json .RepoDigests}}"]
        )
    )
    pattern = re.escape(f"{registry}/antithesis-config") + r"@sha256:[0-9a-f]{64}"
    matches = {
        digest
        for digest in digests or []
        if isinstance(digest, str) and re.fullmatch(pattern, digest)
    }
    if len(matches) != 1:
        raise RuntimeError("the pushed config image must have one registry digest")
    return matches.pop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", required=True)
    parser.add_argument(
        "--commit", required=True, help="the commit the images are built from"
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=MZ_ROOT,
        help="git checkout to build images from (mzbuild needs a git work tree)",
    )
    parser.add_argument("--arch", type=Arch, default=Arch.host())
    parser.add_argument(
        "--upgrade-base",
        action="store_true",
        help="also publish environmentd and clusterd at the upgrade base release",
    )
    parser.add_argument(
        "--config",
        action="store_true",
        help="also render and publish the licensed Kubernetes config to the Antithesis CI registry",
    )
    parser.add_argument(
        "--output", type=Path, help="also write the references to this JSON file"
    )
    args = parser.parse_args()
    if args.config:
        try:
            config_publish_preflight(args.registry)
        except ValueError as error:
            parser.error(str(error))
    tag = target_tag(args.commit)
    refs = publish(
        acquire(args.root, args.arch),
        args.registry,
        tag,
        push=True,
    )
    if args.upgrade_base:
        refs.update(publish_upgrade_base(args.registry, args.arch))
    if args.config:
        refs["config"] = publish_config(refs, args.registry, tag, args.root)
    rendered = json.dumps(refs, indent=2)
    print(rendered)
    if args.output:
        args.output.write_text(rendered + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
