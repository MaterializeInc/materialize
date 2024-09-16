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
