#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from materialize import mzbuild, spawn, ui
from materialize.ci_util.upload_debug_symbols_to_s3 import (
    DEBUGINFO_BINS,
    upload_debuginfo_to_s3,
)
from materialize.mzbuild import (
    CargoBuild,
    Repository,
    ResolvedImage,
    RustIncrementalBuildFailure,
)
from materialize.rustc_flags import Sanitizer
from materialize.xcompile import Arch


class ImagesNotPublicError(Exception):
    """Raised when one or more built images are not public on Docker Hub/GHCR."""


def main() -> None:
    try:
        set_build_status("pending")
        coverage = ui.env_is_truthy("CI_COVERAGE_ENABLED")
        sanitizer = Sanitizer[os.getenv("CI_SANITIZER", "none")]

        repo = mzbuild.Repository(
            Path("."),
            coverage=coverage,
            sanitizer=sanitizer,
            image_registry="materialize",
        )

        # Build and push any images that are not already available on Docker Hub,
        # so they are accessible to other build agents.
        print("--- Acquiring mzbuild images")
        deps = repo.resolve_dependencies(image for image in repo if image.publish)
        # An image's registry visibility is the same across every build step, so
        # only verify it from the primary x86_64 build. That keeps the anonymous
        # registry-API load to one set of queries per pipeline instead of one per
        # arch/coverage/sanitizer variant, and the check is independent of this
        # build, so we run it concurrently with the slow build+push and join it
        # below before reporting success.
        check_public = (
            repo.rd.arch == Arch.X86_64 and not coverage and sanitizer == Sanitizer.none
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            public_check = (
                executor.submit(check_images_public, deps) if check_public else None
            )
            deps.ensure(pre_build=lambda images: upload_debuginfo(repo, images))
            if public_check is not None:
                public_check.result()
        set_build_status("success")
        annotate_buildkite_with_tags(repo.rd.arch, deps)
    except RustIncrementalBuildFailure:
        # We retry twice automatically, see mkpipeline.py
        if int(os.getenv("BUILDKITE_RETRY_COUNT", "0")) >= 2:
            set_build_status("failed")
        print(
            "--- Detected incremental build failure, clearing cargo target directories"
        )
        for dir in ["target", "target-xcompile"]:
            if os.path.exists(dir):
                shutil.rmtree(dir, ignore_errors=True)
        sys.exit(199)
    except:
        set_build_status("failed")
        raise


def set_build_status(status: str) -> None:
    if step_key := os.getenv("BUILDKITE_STEP_KEY"):
        spawn.runv(
            [
                "buildkite-agent",
                "meta-data",
                "set",
                step_key,
                status,
            ]
        )


def check_images_public(deps: mzbuild.DependencySet) -> None:
    """Fail the build if any image's repository is private on Docker Hub or GHCR.

    Repositories must be made public manually, so a new (or silently-regressed)
    image would otherwise only surface when an unauthenticated consumer fails to
    pull it. `is_public()` is tri-state: a registry that we genuinely couldn't
    reach (None) is reported as a non-fatal warning rather than masquerading as a
    private repo, so a registry blip doesn't turn into a misleading red build.

    Only ever invoked from this CI build step; running it elsewhere would fire
    one anonymous registry request per image.
    """
    publishable = [dep for dep in deps if dep.publish]
    if not publishable:
        return

    # Plain log line, not a `---` Buildkite section marker: this runs in a
    # background thread concurrently with deps.ensure()'s own sections, and a
    # competing marker would scramble the log grouping.
    print("Checking that mzbuild images are public on Docker Hub and GHCR")
    with ThreadPoolExecutor(max_workers=min(len(publishable), 16)) as executor:
        results = zip(
            publishable, executor.map(lambda dep: dep.is_public(), publishable)
        )

    private = []
    undetermined = []
    for dep, (dockerhub, ghcr) in results:
        registries = [("DockerHub", dockerhub), ("GHCR", ghcr)]
        if bad := [r for r, ok in registries if ok is False]:
            private.append(
                f"Image {dep.name} is not public on {' and '.join(bad)}, "
                "ask @team-testing to make it public"
            )
        if unknown := [r for r, ok in registries if ok is None]:
            undetermined.append(
                f"Could not determine whether image {dep.name} is public on "
                f"{' and '.join(unknown)} (registry error); will recheck next build"
            )

    for message in undetermined + private:
        print(message)
    if ui.env_is_truthy("CI"):
        if private:
            _annotate("error", "images-not-public", private)
        if undetermined:
            _annotate("warning", "images-public-undetermined", undetermined)

    if private:
        raise ImagesNotPublicError(f"{len(private)} image(s) are not public")


def _annotate(style: str, context: str, messages: list[str]) -> None:
    spawn.runv(
        ["buildkite-agent", "annotate", f"--style={style}", f"--context={context}"],
        stdin="\n".join(f"* {m}" for m in messages).encode(),
    )


def annotate_buildkite_with_tags(arch: Arch, deps: mzbuild.DependencySet) -> None:
    if not ui.env_is_truthy("CI"):
        return

    tags = "\n".join([f"* `{dep.spec()}`" for dep in deps])
    markdown = f"""<details><summary>{arch} Docker tags produced in this build</summary>

{tags}
</details>"""
    spawn.runv(
        ["buildkite-agent", "annotate", "--style=info", f"--context=build-{arch}"],
        stdin=markdown.encode(),
    )


def upload_debuginfo(
    repo: mzbuild.Repository, built_images: list[ResolvedImage], max_tries: int = 3
) -> None:
    """Uploads debuginfo to `DEBUGINFO_S3_BUCKET` if any DEBUGINFO_BINS were built."""

    # Find all binaries created by the `cargo-bin` pre-image.
    bins = find_binaries_created_by_cargo_bin(repo, built_images, DEBUGINFO_BINS)
    if len(bins) == 0:
        print("No debuginfo bins were built")
        return

    ui.section(f"Uploading debuginfo for {', '.join(bins)}...")

    is_tag_build = ui.env_is_truthy("BUILDKITE_TAG")

    for bin in bins:
        bin_path = get_bin_path(repo, bin)

        dbg_path = bin_path.with_suffix(bin_path.suffix + ".debug")
        spawn.runv(
            [
                *repo.rd.tool("objcopy"),
                bin_path,
                dbg_path,
                "--only-keep-debug",
            ],
        )

        # Upload binary and debuginfo to S3 bucket, regardless of whether this
        # is a tag build or not. S3 is cheap.
        build_id = upload_debuginfo_to_s3(bin_path, dbg_path, is_tag_build)
        print(f"Uploaded debuginfo to S3 with build_id {build_id}")


def find_binaries_created_by_cargo_bin(
    repo: Repository, built_images: list[ResolvedImage], bin_names: set[str]
) -> set[str]:
    bins: set[str] = set()
    for image in built_images:
        for pre_image in image.image.pre_images:
            if isinstance(pre_image, CargoBuild):
                for bin in pre_image.bins:
                    if bin in bin_names:
                        bins.add(bin)

    return bins


def get_bin_path(repo: Repository, bin: str) -> Path:
    cargo_profile = (
        "release"
        if repo.rd.profile == mzbuild.Profile.RELEASE
        else ("optimized" if repo.rd.profile == mzbuild.Profile.OPTIMIZED else "debug")
    )
    return repo.rd.cargo_target_dir() / cargo_profile / bin


if __name__ == "__main__":
    main()
