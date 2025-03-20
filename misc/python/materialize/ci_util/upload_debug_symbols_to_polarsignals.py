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
from tempfile import _TemporaryFileWrapper

import boto3

from materialize import mzbuild, spawn, ui
from materialize.ci_util.upload_debug_symbols_to_s3 import (
    DEBUGINFO_BINS,
    DEBUGINFO_S3_BUCKET,
)
from materialize.mzbuild import Repository, ResolvedImage
from materialize.rustc_flags import Sanitizer

# Upload debuginfo and sources to Polar Signals (our continuous
# profiling provider).
# This script is only invoked for build tags. Polar Signals is
# expensive, so we don't want to upload development or unstable builds
# that won't ever be profiled by Polar Signals.


def main() -> None:
    coverage = ui.env_is_truthy("CI_COVERAGE_ENABLED")
    sanitizer = Sanitizer[os.getenv("CI_SANITIZER", "none")]
    bazel = ui.env_is_truthy("CI_BAZEL_BUILD")
    bazel_remote_cache = os.getenv("CI_BAZEL_REMOTE_CACHE")

    repo = mzbuild.Repository(
        Path("."),
        coverage=coverage,
        sanitizer=sanitizer,
        bazel=bazel,
        bazel_remote_cache=bazel_remote_cache,
    )

    collect_and_upload_debug_data_to_polarsignals(repo, DEBUGINFO_BINS)


def collect_and_upload_debug_data_to_polarsignals(
    repo: mzbuild.Repository, debuginfo_bins: set[str]
) -> None:
    ui.section("Collecting and uploading debug data to PolarSignals...")

    relevant_images_by_name = get_build_images(repo, debuginfo_bins)
    print(f"Considered images are: {relevant_images_by_name.keys()}")

    for image_name, image in relevant_images_by_name.items():
        container_name = create_docker_container(image_name, image)
        print(
            f"Created docker container from image {image_name} (spec: {image.spec()})"
        )

        path_to_binary = copy_binary_from_image(image_name, container_name)
        print(f"Copied binary from image {image_name}")

        build_id = get_build_id(repo, path_to_binary)
        print(f"{image_name} has build_id {build_id}")

        bin_path, dbg_path = fetch_debug_symbols_from_s3(build_id)
        print(f"Fetched debug symbols of {image_name} from S3")

        upload_completed = upload_debug_data_to_polarsignals(
            repo, build_id, bin_path, dbg_path
        )
        if upload_completed:
            print(f"Uploaded debug symbols of {image_name} to PolarSignals")
        else:
            print(f"Did not upload debug symbols of {image_name} to PolarSignals")


def get_build_images(
    repo: mzbuild.Repository, image_names: set[str]
) -> dict[str, ResolvedImage]:
    relevant_images = []
    for image_name, image in repo.images.items():
        if image_name in image_names:
            relevant_images.append(image)

    dependency_set = repo.resolve_dependencies(relevant_images)

    resolved_images = dict()
    for image_name in image_names:
        resolved_images[image_name] = dependency_set[image_name]

    return resolved_images


def create_docker_container(image_name: str, image: ResolvedImage) -> str:
    try:
        image_spec = image.spec()
        docker_container_name = image_name
        command = ["docker", "create", "--name", docker_container_name, image_spec]
        subprocess.run(command, check=True)
        return docker_container_name
    except subprocess.CalledProcessError as e:
        if "manifest unknown" in str(e):
            raise RuntimeError(f"Docker image not found: {image.spec()}")
        print(f"Error creating docker container: {e}")
        raise e


def copy_binary_from_image(image_name: str, docker_container_name: str) -> str:
    try:
        source_path = f"/usr/local/bin/{image_name}"
        target_path = f"./{image_name}"
        command = [
            "docker",
            "cp",
            f"{docker_container_name}:{source_path}",
            target_path,
        ]
        subprocess.run(command, check=True)

        return target_path
    except subprocess.CalledProcessError as e:
        print(f"Error copying file: {e}")
        raise e


def get_build_id(repo: mzbuild.Repository, path_to_binary: str) -> str:
    return spawn.run_with_retries(
        lambda: spawn.capture(
            ["parca-debuginfo", "buildid", path_to_binary],
            cwd=repo.rd.root,
        ).strip()
    )


def fetch_debug_symbols_from_s3(build_id: str) -> tuple[str, str]:
    s3 = boto3.client("s3")

    file_names = [
        "executable",
        "debuginfo",
    ]

    downloaded_file_paths = dict()

    for file_name in file_names:
        key = f"buildid/{build_id}/{file_name}"
        target_file_name = key.replace("/", "_")
        print(
            f"Downloading {file_name} from s3://{DEBUGINFO_S3_BUCKET}/{key} to {target_file_name}"
        )

        with open(target_file_name, "wb") as data:
            s3.download_fileobj(DEBUGINFO_S3_BUCKET, key, data)

        downloaded_file_paths[file_name] = target_file_name

    return downloaded_file_paths["executable"], downloaded_file_paths["debuginfo"]


def upload_debug_data_to_polarsignals(
    repo: Repository,
    build_id: str,
    bin_path: Path | str,
    dbg_path: Path | str,
) -> bool:
    polar_signals_api_token = os.environ["POLAR_SIGNALS_API_TOKEN"]
    _upload_debug_info_to_polarsignals(repo, dbg_path, polar_signals_api_token)

    with tempfile.NamedTemporaryFile() as tarball:
        _create_source_tarball(repo, bin_path, tarball)
        return _upload_source_tarball_to_polarsignals(
            repo, bin_path, tarball, build_id, polar_signals_api_token
        )


def _upload_debug_info_to_polarsignals(
    repo: mzbuild.Repository, dbg_path: Path | str, polar_signals_api_token: str
) -> None:
    print(f"Uploading debuginfo for {dbg_path} to Polar Signals...")
    spawn.run_with_retries(
        lambda: spawn.runv(
            [
                "parca-debuginfo",
                "upload",
                "--store-address=grpc.polarsignals.com:443",
                "--no-extract",
                dbg_path,
            ],
            cwd=repo.rd.root,
            env=dict(os.environ, PARCA_DEBUGINFO_BEARER_TOKEN=polar_signals_api_token),
        )
    )


def _create_source_tarball(
    repo: mzbuild.Repository, bin_path: Path | str, tarball: _TemporaryFileWrapper
) -> None:
    print(f"Constructing source tarball for {bin_path}...")
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


def _upload_source_tarball_to_polarsignals(
    repo: mzbuild.Repository,
    bin_path: Path | str,
    tarball: _TemporaryFileWrapper,
    build_id: str,
    polar_signals_api_token: str,
) -> bool:
    print(f"Uploading source tarball for {bin_path} to Polar Signals...")
    output = spawn.run_with_retries(
        lambda: spawn.capture(
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
                os.environ,
                PARCA_DEBUGINFO_BEARER_TOKEN=polar_signals_api_token,
            ),
        ).strip()
    )

    if "Skipping upload of" in output:
        return False

    return True


if __name__ == "__main__":
    main()
