# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import hashlib
import pathlib
import subprocess

import requests

from materialize import MZ_ROOT, ui

"""Utilities for interacting with Bazel from python scripts"""

# Path where we put the current revision of the repo that we can side channel
# into Bazel.
MZ_GIT_HASH_FILE = "/tmp/mz_git_hash.txt"


def output_paths(target, options=[]) -> list[pathlib.Path]:
    """Returns the absolute path of outputs from the built Bazel target."""

    cmd_args = ["bazel", "cquery", f"{target}", *options, "--output=files"]
    paths = subprocess.check_output(
        cmd_args, text=True, stderr=subprocess.DEVNULL
    ).splitlines()
    return [pathlib.Path(path) for path in paths]


def write_git_hash():
    """
    Temporary file where we write the current git hash, so we can side channel
    it into Bazel.

    For production releases we stamp builds with the `workspace_status_command`
    but this workflow is not friendly to remote caching. Specifically, the
    "volatile status" of a workspace is not supposed to cause builds to get
    invalidated, and it doesn't when the result is cached locally, but it does
    when it's cached remotely.

    See: <https://bazel.build/docs/user-manual#workspace-status>
         <https://github.com/bazelbuild/bazel/issues/10075>
    """

    repo = MZ_ROOT / ".git"
    cmd_args = ["git", f"--git-dir={repo}", "rev-parse", "HEAD"]
    result = subprocess.run(
        cmd_args, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )

    if result.returncode == 0:
        with open(MZ_GIT_HASH_FILE, "w") as f:
            f.write(result.stdout.strip())
    else:
        ui.warn(f"Failed to get current revision of {MZ_ROOT}, falling back to all 0s")


def calc_ingerity(path) -> str:
    """
    Calculate the 'integrity' for a given file.

    'integrity' is a hash of the file used in rules like 'http_archive'.

    See: <https://bazel.build/rules/lib/repo/http#http_archive-integrity>
    """

    digest = subprocess.run(
        ["openssl", "dgst", "-sha256", "-binary", str(path)], stdout=subprocess.PIPE
    )
    base64 = subprocess.run(
        ["openssl", "base64", "-A"], input=digest.stdout, stdout=subprocess.PIPE
    )
    formatted = subprocess.run(
        ["sed", "s/^/sha256-/"], input=base64.stdout, stdout=subprocess.PIPE
    )

    return formatted.stdout.decode("utf-8")


def toolchain_hashes(stable, nightly) -> dict[str, dict[str, str]]:
    """
    Generates the hashes for our Bazel toolchains.

    Fetches the specified Stable and Nightly version of the Rust compiler from our toolchains repo,
    hashes the downloaded files, and returns a properly formatted dictionary for Bazel.
    """

    ARCHS = [
        "aarch64-apple-darwin",
        "aarch64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "x86_64-unknown-linux-gnu",
    ]
    TOOLS = [
        "cargo",
        "clippy",
        "llvm-tools",
        "rust-std",
        "rustc",
    ]
    VERSIONS = {"stable": stable, "nightly": nightly}
    URL_TEMPLATE = "https://github.com/MaterializeInc/toolchains/releases/download/rust-{version}/{tool}-{channel}-{arch}.tar.zst"

    hashes = {}

    for arch in ARCHS:
        hashes[arch] = {}
        for channel, version in VERSIONS.items():
            hashes[arch][channel] = {}
            for tool in TOOLS:
                if channel == "stable":
                    url_channel = version
                else:
                    url_channel = channel

                print(f"Processing {tool} {version} {arch}")

                # Download the file.
                url = URL_TEMPLATE.format(
                    version=version, tool=tool, channel=url_channel, arch=arch
                )
                response = requests.get(url, stream=True)
                response.raise_for_status()

                # Hash the response.
                sha256_hash = hashlib.sha256()
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        sha256_hash.update(chunk)
                hashes[arch][channel][tool] = sha256_hash.hexdigest()

    return hashes
