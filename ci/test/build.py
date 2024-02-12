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
import subprocess
import tempfile
from pathlib import Path

import boto3

from materialize import elf, mzbuild, spawn, ui
from materialize.mzbuild import CargoBuild, ResolvedImage
from materialize.xcompile import Arch

# The S3 bucket in which to store debuginfo.
DEBUGINFO_S3_BUCKET = "materialize-debuginfo"

# The binaries for which debuginfo should be uploaded to S3 and Polar Signals.
DEBUGINFO_BINS = ["environmentd", "clusterd"]


def main() -> None:
    coverage = ui.env_is_truthy("CI_COVERAGE_ENABLED")
    repo = mzbuild.Repository(Path("."), coverage=coverage)

    # Build and push any images that are not already available on Docker Hub,
    # so they are accessible to other build agents.
    print("--- Acquiring mzbuild images")
    built_images = set()
    deps = repo.resolve_dependencies(image for image in repo if image.publish)
    deps.ensure(post_build=lambda image: built_images.add(image))
    maybe_upload_debuginfo(repo, built_images)
    annotate_buildkite_with_tags(repo.rd.arch, deps)


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
    repo: mzbuild.Repository, built_images: set[ResolvedImage]
) -> None:
    """Uploads debuginfo to `DEBUGINFO_S3_BUCKET` and Polar Signals if any
    DEBUGINFO_BINS were built."""

    # Find all binaries created by the `cargo-bin` pre-image.
    bins: set[str] = set()
    for image in built_images:
        for pre_image in image.image.pre_images:
            if isinstance(pre_image, CargoBuild):
                for bin in pre_image.bins:
                    if bin in DEBUGINFO_BINS:
                        bins.add(bin)
    if not bins:
        return

    print(f"Uploading debuginfo for {', '.join(bins)}...")

    s3 = boto3.client("s3")
    is_tag_build = ui.env_is_truthy("BUILDKITE_TAG")
    polar_signals_api_token = os.environ["POLAR_SIGNALS_API_TOKEN"]

    for bin in bins:
        cargo_profile = "release" if repo.rd.release_mode else "debug"
        bin_path = repo.rd.cargo_target_dir() / cargo_profile / bin
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
        with open(bin_path, "rb") as exe, open(dbg_path, "rb") as dbg:
            build_id = elf.get_build_id(exe)
            assert build_id.isalnum()
            assert len(build_id) > 0

            dbg_build_id = elf.get_build_id(dbg)
            assert build_id == dbg_build_id

            for fileobj, name in [
                (exe, "executable"),
                (dbg, "debuginfo"),
            ]:
                key = f"buildid/{build_id}/{name}"
                print(f"Uploading {name} to s3://{DEBUGINFO_S3_BUCKET}/{key}...")
                fileobj.seek(0)
                s3.upload_fileobj(
                    Fileobj=fileobj,
                    Bucket=DEBUGINFO_S3_BUCKET,
                    Key=key,
                    ExtraArgs={
                        "Tagging": f"ephemeral={'false' if is_tag_build else 'true'}",
                    },
                )

        # Upload debuginfo and sources to Polar Signals (our continuous
        # profiling provider), but only if this is a tag build. Polar Signals is
        # expensive, so we don't want to upload development or unstable builds
        # that won't ever be profiled by Polar Signals.
        if is_tag_build:
            print(f"Uploading debuginfo for {bin} to Polar Signals...")
            spawn.runv(
                [
                    "parca-debuginfo",
                    "upload",
                    "--store-address=grpc.polarsignals.com:443",
                    "--no-extract",
                    dbg_path,
                ],
                cwd=repo.rd.root,
                env=dict(
                    os.environ, PARCA_DEBUGINFO_BEARER_TOKEN=polar_signals_api_token
                ),
            )

            print(f"Constructing source tarball for {bin}...")
            with tempfile.NamedTemporaryFile() as tarball:
                p1 = subprocess.Popen(
                    ["llvm-dwarfdump", "--show-sources", bin_path],
                    stdout=subprocess.PIPE,
                )
                p2 = subprocess.Popen(
                    [
                        "tar",
                        "-cf",
                        tarball.name,
                        "--zstd",
                        "-T",
                        "-",
                        "--ignore-failed-read",
                    ],
                    stdin=p1.stdout,
                    # Suppress noisy warnings about missing files.
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

                # This causes p1 to receive SIGPIPE if p2 exits early,
                # like in the shell.
                assert p1.stdout
                p1.stdout.close()

                for p in [p1, p2]:
                    if p.wait():
                        raise subprocess.CalledProcessError(p.returncode, p.args)

                print(f"Uploading source tarball for {bin} to Polar Signals...")
                spawn.runv(
                    [
                        "parca-debuginfo",
                        "upload",
                        "--store-address=grpc.polarsignals.com:443",
                        "--type=sources",
                        f"--build-id={build_id}",
                        tarball.name,
                    ],
                    cwd=repo.rd.root,
                    env=dict(
                        os.environ, PARCA_DEBUGINFO_BEARER_TOKEN=polar_signals_api_token
                    ),
                )


if __name__ == "__main__":
    main()
