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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("@rules_rust//rust:repositories.bzl", "DEFAULT_TOOLCHAIN_TRIPLES", "rust_repository_set")

def rust_toolchains(versions, targets):
    """
    Macro that registers Rust toolchains for the provided targets.

    `rules_rust` provides a `rust_register_toolchains` macro but it fetches
    more Rust toolchains than we need, i.e. Linux, macOS, FreeBSD, and WASM.
    Instead the alternative of `rust_repostitory_set` is verbose enough that we
    abstract it away into this macro.
    """

    for version in versions:
        # Release channel for the current version.
        if version.startswith("nightly"):
            channel = "nightly"
        elif version.startswith("beta"):
            channel = "beta"
        else:
            channel = "stable"

        # Namespace for the GitHub release URLs.
        if channel == "stable":
            url_namespace = version
        else:
            url_namespace = channel

        for (target, versioned_components) in targets.items():
            # Support cross compiling to all other targets we specify.
            extra_targets = [t for t in targets.keys()]
            extra_targets.remove(target)

            # Get the hashes for the current version.
            components = versioned_components[channel]

            integrity = {}
            for (component, sha256) in components.items():
                key = _integrity_key(version, target, component)
                integrity[key] = sha256

            # These components are used when cross compiling and so we include
            # their hashes in the integrity for every target.
            CROSS_COMPILING_COMPONENTS = ["rust-std"]
            for extra_target in extra_targets:
                for component in CROSS_COMPILING_COMPONENTS:
                    sha256 = targets[extra_target][channel].get(component)
                    if not sha256:
                        continue
                    key = _integrity_key(version, extra_target, component)
                    integrity[key] = sha256

            rust_repository_set(
                name = DEFAULT_TOOLCHAIN_TRIPLES[target],
                edition = "2021",
                exec_triple = target,
                extra_target_triples = extra_targets,
                versions = [version],
                sha256s = integrity,
                urls = [
                    "https://github.com/MaterializeInc/toolchains/releases/download/rust-{NS}/{{}}.tar.zst".format(NS = url_namespace),
                ],
            )

def _integrity_key(version, target, component):
    if version.startswith("nightly") or version.startswith("beta"):
        parts = version.split("/", 1)
        channel = parts[0]
        date = parts[1]
        return "{0}/{1}-{2}-{3}".format(date, component, channel, target)
    else:
        return "{0}-{1}-{2}".format(component, version, target)

_BINDGEN_TOOLCHAIN_BUILD_FILE = """
package(default_visibility = ["//visibility:public"])

sh_binary(
    name = "clang",
    srcs = ["bin/clang"],
)

cc_import(
    name = "libclang",
    shared_library = "lib/libclang.{SHARED_EXTENSION}",
)

cc_import(
    name = "libc++",
    shared_library = "lib/{STDCXX}"
)
"""

def bindgen_toolchains(clang_release, targets):
    """
    Macro that registers [Rust bindgen] toolchains for the provided targets.

    [Rust bindgen](https://github.com/rust-lang/rust-bindgen)

    Args:
        clang_release (string): Name of the clang dependency we'll fetch.
        targets (dict[string, string]): Map of platform to the integrity for
            the libclang toolchain we fetch.
    """

    for (platform, integrity) in targets.items():
        if platform.startswith("darwin"):
            shared_extension = "dylib"
            stdcxx = "libc++.1.0.dylib"
        else:
            shared_extension = "so"
            stdcxx = "libc++.so.1.0"

        maybe(
            http_archive,
            name = "rust_bindgen__{0}".format(platform),
            build_file_content = _BINDGEN_TOOLCHAIN_BUILD_FILE.format(SHARED_EXTENSION = shared_extension, STDCXX = stdcxx),
            integrity = integrity,
            url = "https://github.com/MaterializeInc/toolchains/releases/download/clang-{0}/{1}_libclang.tar.zst".format(clang_release, platform),
        )

        native.register_toolchains("@//misc/bazel/toolchains:rust_bindgen_toolchain__{0}".format(platform))
