# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Defines third party Rust dependencies (generally binaries) that cannot be
included via crates_repository.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("@rules_rust//crate_universe:defs.bzl", "crate", "crates_repository")

def rust_repositories():
    """Download third-party Rust repositories and their dependencies."""

    CXX_VERSION = "1.0.153"
    CXXBRIDGE_CMD_INTEGRITY = "sha256-fLxBkzdnlV0EwqkBUYBgKbk99f2LaCuiKpZ0MzR0gKk="
    maybe(
        http_archive,
        name = "cxxbridge-cmd",
        build_file = Label("//misc/bazel/rust_deps:cxxbridge-cmd/include.BUILD.bazel"),
        integrity = CXXBRIDGE_CMD_INTEGRITY,
        strip_prefix = "cxxbridge-cmd-{0}".format(CXX_VERSION),
        type = "tar.gz",
        urls = ["https://crates.io/api/v1/crates/cxxbridge-cmd/{0}/download".format(CXX_VERSION)],
    )

    crates_repository(
        name = "cxxbridge",
        cargo_lockfile = "@cxxbridge-cmd//:Cargo.lock",
        lockfile = "//misc/bazel/rust_deps:cxxbridge-cmd/Cargo.cxxbridge-cmd.lock",
        manifests = ["@cxxbridge-cmd//:Cargo.toml"],
        # Restricting the number of platforms we support _greatly_ reduces the
        # amount of time it takes to "Splice Cargo Workspace".
        supported_platform_triples = [
            "aarch64-unknown-linux-gnu",
            "x86_64-unknown-linux-gnu",
            "aarch64-apple-darwin",
            "x86_64-apple-darwin",
            "wasm32-unknown-unknown",
        ],
        generator_sha256s = {
            "aarch64-apple-darwin": "c38c9c0efc11fcf9c32b9e0f4f4849df7c823f207c7f5ba5f6ab1e0e2167693d",
            "aarch64-unknown-linux-gnu": "5bdc9a10ec5f17f5140a81ce7cb0c0ce6e82d4d862d3ce3a301ea23f72f20630",
            "x86_64-unknown-linux-gnu": "abcd8212d64ea4c0f5e856af663c05ebeb2800a02c251f6eb62061f4e8ca1735",
        },
        generator_urls = {
            "aarch64-apple-darwin": "https://github.com/MaterializeInc/rules_rust/releases/download/mz-0.59.3/cargo-bazel-aarch64-apple-darwin",
            "aarch64-unknown-linux-gnu": "https://github.com/MaterializeInc/rules_rust/releases/download/mz-0.59.3/cargo-bazel-aarch64-unknown-linux-gnu",
            "x86_64-unknown-linux-gnu": "https://github.com/MaterializeInc/rules_rust/releases/download/mz-0.59.3/cargo-bazel-x86_64-unknown-linux-gnu",
        },
        isolated = False,
        # Only used if developing rules_rust.
        # generator = "@cargo_bazel_bootstrap//:cargo-bazel",
    )
