#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import gzip
import subprocess
from pathlib import Path

from materialize import ui
from materialize.ci_util.upload_debug_symbols_to_polarsignals import (
    fetch_debug_symbols_from_s3,
)
from materialize.visualize_pprof_profile.profile_pb2 import (
    Profile,  # type: ignore , Generated from protobuf
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Symbolize and view pprof profiles offline using debug symbols from S3"
    )
    parser.add_argument(
        "profile_path",
        type=Path,
        help="Path to the pprof profile file",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port to run the pprof web UI on (default: 8080)",
    )
    return parser.parse_args()


def get_build_id(pprof_path: Path) -> str:
    # Read raw binary data
    with gzip.open(pprof_path, "rb") as f:
        data = f.read()

    # Parse the profile
    profile = Profile()
    profile.ParseFromString(data)
    # Access string_table[5]
    # Extract build_ids from mappings
    build_ids = []
    for mapping in profile.mapping:
        if mapping.build_id:
            build_id_str = profile.string_table[mapping.build_id]
            build_ids.append(build_id_str)

    if len(build_ids) == 0:
        raise ui.UIError(
            f"No build ID found in {pprof_path}. Please check if the profile is valid."
        )
    # We assume that the first build ID is the one we've stored in S3.
    return build_ids[0]


def create_pprof_container(container_name: str) -> str:
    """Create a Docker container with pprof installed using golang image."""
    try:
        # Remove existing container if it exists
        subprocess.run(["docker", "rm", "-f", container_name], check=False)

        command = [
            "docker",
            "run",
            "-d",  # Run in detached mode
            "-e",
            # Set the path to the binary and debug info used for symbolization
            "PPROF_BINARY_PATH=/tmp",
            "--name",
            container_name,
            "--network",
            "host",
            "golang:latest",  # Official Golang image which includes an outdated versino of pprof
            "tail",
            "-f",
            "/dev/null",  # Keep container running
        ]
        subprocess.run(command, check=True)
        return container_name
    except subprocess.CalledProcessError as e:
        raise ui.UIError(f"Error creating docker container: {e}")


def run_pprof_ui(
    profile_path: Path,
    binary_path: Path,
    debug_info_path: Path,
    build_id: str,
    port: int,
) -> None:
    """Run pprof web UI with the symbolized profile."""
    try:
        container_name = "pprof-viewer"

        # Create and start container
        create_pprof_container(container_name)

        # Create build_id directory in container first
        subprocess.run(
            ["docker", "exec", container_name, "mkdir", "-p", f"/tmp/{build_id}"],
            check=True,
        )

        # Copy files into container
        for path, dest in [
            (profile_path, "/tmp/profile.pprof"),
            # We need this specific path and file format in order for pprof to read the binary/debug info
            (binary_path, f"/tmp/{build_id}/binary"),
            (debug_info_path, "/tmp/binary.debug"),
        ]:
            subprocess.run(
                ["docker", "cp", str(path), f"{container_name}:{dest}"], check=True
            )

        # Install graphviz in the container
        subprocess.run(
            ["docker", "exec", container_name, "apt-get", "update"], check=True
        )
        subprocess.run(
            ["docker", "exec", container_name, "apt-get", "install", "-y", "graphviz"],
            check=True,
        )

        # Install pprof in the container
        # Although the official golang image includes a version of pprof,
        # there exists a bug where it cannot read the binary/debug info with error "build-id mismatch" and
        # will just show the profile as a flat profile. Thus we install the latest version of pprof.
        subprocess.run(
            [
                "docker",
                "exec",
                container_name,
                "go",
                "install",
                "github.com/google/pprof@latest",
            ],
            check=True,
        )

        # Run pprof in the container
        cmd = [
            "docker",
            "exec",
            container_name,
            # Run the latest version of pprof
            "/go/bin/pprof",
            "-http",
            f"localhost:{port}",
            "/tmp/profile.pprof",
        ]
        print(f"Starting pprof web UI in Docker on http://localhost:{port}")
        subprocess.run(cmd, check=True)

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to run pprof: {e}")


def main() -> None:
    args = parse_args()

    if not args.profile_path.exists():
        raise ui.UIError(f"Profile file not found: {args.profile_path}")

    Path(args.profile_path)
    # Get build ID from pprof file
    build_id = get_build_id(args.profile_path)
    print(f"Found profile for {args.profile_path} with build ID {build_id}")

    # Fetch debug symbols from S3
    binary_path, debug_path = fetch_debug_symbols_from_s3(build_id)
    binary_path = Path(binary_path)
    debug_path = Path(debug_path)
    try:
        # Run pprof UI
        run_pprof_ui(args.profile_path, binary_path, debug_path, build_id, args.port)
    finally:
        # Cleanup temporary files
        binary_path.unlink(missing_ok=True)
        debug_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
