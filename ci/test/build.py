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
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path

from materialize import bazel, mzbuild, spawn, ui
from materialize.ci_util.upload_debug_symbols_to_s3 import (
    DEBUGINFO_BINS,
    upload_debuginfo_to_s3,
)
from materialize.mzbuild import CargoBuild, Repository, ResolvedImage
from materialize.rustc_flags import Sanitizer
from materialize.xcompile import Arch


def main() -> None:
    try:
        set_build_status("pending")
        coverage = ui.env_is_truthy("CI_COVERAGE_ENABLED")
        sanitizer = Sanitizer[os.getenv("CI_SANITIZER", "none")]
        bazel = ui.env_is_truthy("CI_BAZEL_BUILD")
        bazel_remote_cache = os.getenv("CI_BAZEL_REMOTE_CACHE")
        bazel_lto = ui.env_is_truthy("CI_BAZEL_LTO")

        repo = mzbuild.Repository(
            Path("."),
            coverage=coverage,
            sanitizer=sanitizer,
            bazel=bazel,
            bazel_remote_cache=bazel_remote_cache,
            bazel_lto=bazel_lto,
        )

        # Build and push any images that are not already available on Docker Hub,
        # so they are accessible to other build agents.
        print("--- Acquiring mzbuild images")
        built_images = set()
        deps = repo.resolve_dependencies(image for image in repo if image.publish)
        deps.ensure(post_build=lambda image: built_images.add(image))
        set_build_status("success")

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(maybe_upload_debuginfo, repo, built_images),
                executor.submit(annotate_buildkite_with_tags, repo.rd.arch, deps),
            ]

            # Wait until all tasks are complete
            wait(futures)
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


def annotate_buildkite_with_tags(arch: Arch, deps: mzbuild.DependencySet) -> None:
    tags = "\n".join([f"* `{dep.spec()}`" for dep in deps])
    markdown = f"""<details><summary>{arch} Docker tags produced in this build</summary>

{tags}
</details>"""
    spawn.runv(
        ["buildkite-agent", "annotate", "--style=info", f"--context=build-{arch}"],
        stdin=markdown.encode(),
    )


def maybe_upload_debuginfo(
    repo: mzbuild.Repository, built_images: set[ResolvedImage], max_tries: int = 3
) -> None:
    """Uploads debuginfo to `DEBUGINFO_S3_BUCKET` and Polar Signals if any
    DEBUGINFO_BINS were built."""

    # Find all binaries created by the `cargo-bin` pre-image.
    bins, bazel_bins = find_binaries_created_by_cargo_bin(
        repo, built_images, DEBUGINFO_BINS
    )
    if len(bins) == 0:
        print("No debuginfo bins were built")
        return

    ui.section(f"Uploading debuginfo for {', '.join(bins)}...")

    is_tag_build = ui.env_is_truthy("BUILDKITE_TAG")

    for bin in bins:
        bin_path = get_bin_path(repo, bin, bazel_bins)

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
    repo: Repository, built_images: set[ResolvedImage], bin_names: set[str]
) -> tuple[set[str], dict[str, str]]:
    bins: set[str] = set()
    bazel_bins: dict[str, str] = dict()
    for image in built_images:
        for pre_image in image.image.pre_images:
            if isinstance(pre_image, CargoBuild):
                for bin in pre_image.bins:
                    if bin in bin_names:
                        bins.add(bin)
                    if repo.rd.bazel:
                        bazel_bins[bin] = pre_image.bazel_bins[bin]

    return bins, bazel_bins


def get_bin_path(repo: Repository, bin: str, bazel_bins: dict[str, str]) -> Path:
    if repo.rd.bazel:
        options = repo.rd.bazel_config()
        paths = bazel.output_paths(bazel_bins[bin], options)
        assert len(paths) == 1, f"{bazel_bins[bin]} output more than 1 file"
        return paths[0]
    else:
        cargo_profile = (
            "release"
            if repo.rd.profile == mzbuild.Profile.RELEASE
            else (
                "optimized" if repo.rd.profile == mzbuild.Profile.OPTIMIZED else "debug"
            )
        )
        return repo.rd.cargo_target_dir() / cargo_profile / bin


if __name__ == "__main__":
    main()
