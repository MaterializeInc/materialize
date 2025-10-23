# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Support for cross-compiling to Linux."""

import os
import platform
import sys
from enum import Enum

from materialize import MZ_ROOT, spawn
from materialize.rustc_flags import Sanitizer


class Arch(Enum):
    """A CPU architecture."""

    X86_64 = "x86_64"
    """The 64-bit x86 architecture."""

    AARCH64 = "aarch64"
    """The 64-bit ARM architecture."""

    def __str__(self) -> str:
        return self.value

    def go_str(self) -> str:
        """Return the architecture name in Go nomenclature: amd64 or arm64."""
        if self == Arch.X86_64:
            return "amd64"
        elif self == Arch.AARCH64:
            return "arm64"
        else:
            raise RuntimeError("unreachable")

    @staticmethod
    def host() -> "Arch":
        if platform.machine() == "x86_64":
            return Arch.X86_64
        elif platform.machine() in ["aarch64", "arm64"]:
            return Arch.AARCH64
        else:
            raise RuntimeError(f"unknown host architecture {platform.machine()}")


def target(arch: Arch) -> str:
    """Construct a Linux target triple for the specified architecture."""
    return f"{arch}-unknown-linux-gnu"


def target_cpu(arch: Arch) -> str:
    """
    Return the CPU micro architecture, assuming a Linux target, we should use for Rust compilation.

    Sync: This target-cpu should be kept in sync with the one in ci-builder and .cargo/config.
    """
    if arch == Arch.X86_64:
        return "x86-64-v3"
    elif arch == Arch.AARCH64:
        return "neoverse-n1"
    else:
        raise RuntimeError("unreachable")


def target_features(arch: Arch) -> list[str]:
    """
    Returns a list of CPU features we should enable for Rust compilation.

    Note: We also specify the CPU target when compiling Rust which should enable the majority of
    available CPU features.

    Sync: This list of features should be kept in sync with the one in ci-builder and .cargo/config.
    """
    if arch == Arch.X86_64:
        return ["+aes", "+pclmulqdq"]
    elif arch == Arch.AARCH64:
        return ["+aes", "+sha2"]
    else:
        raise RuntimeError("unreachable")


def cargo(
    arch: Arch,
    subcommand: str,
    rustflags: list[str],
    channel: str | None = None,
    extra_env: dict[str, str] = {},
) -> list[str]:
    """Construct a Cargo invocation for cross compiling.

    Args:
        arch: The CPU architecture to build for.
        subcommand: The Cargo subcommand to invoke.
        rustflags: Override the flags passed to the Rust compiler. If the list
            is empty, the default flags are used.
        channel: The Rust toolchain channel to use. Either None/"stable" or "nightly".

    Returns:
        A list of arguments specifying the beginning of the command to invoke.
    """
    _target = target(arch)
    _target_env = _target.upper().replace("-", "_")
    _target_cpu = target_cpu(arch)
    _target_features = ",".join(target_features(arch))

    env = {
        **extra_env,
    }

    rustflags += [
        "-Clink-arg=-Wl,--compress-debug-sections=zlib",
        "-Clink-arg=-Wl,-O3",
        "-Csymbol-mangling-version=v0",
        f"-Ctarget-cpu={_target_cpu}",
        f"-Ctarget-feature={_target_features}",
        "--cfg=tokio_unstable",
    ]

    if sys.platform == "darwin":
        _bootstrap_darwin(arch)
        lld_prefix = spawn.capture(["brew", "--prefix", "lld"]).strip()
        sysroot = spawn.capture([f"{_target}-cc", "-print-sysroot"]).strip()
        rustflags += [
            f"-L{sysroot}/lib",
            "-Clink-arg=-fuse-ld=lld",
            f"-Clink-arg=-B{lld_prefix}/bin",
        ]
        env.update(
            {
                "CMAKE_SYSTEM_NAME": "Linux",
                f"CARGO_TARGET_{_target_env}_LINKER": f"{_target}-cc",
                "CARGO_TARGET_DIR": str(MZ_ROOT / "target-xcompile"),
                "TARGET_AR": f"{_target}-ar",
                "TARGET_CPP": f"{_target}-cpp",
                "TARGET_CC": f"{_target}-cc",
                "TARGET_CXX": f"{_target}-c++",
                "TARGET_CXXSTDLIB": "static=stdc++",
                "TARGET_LD": f"{_target}-ld",
                "TARGET_RANLIB": f"{_target}-ranlib",
            }
        )
    else:
        # NOTE(benesch): The required Rust flags have to be duplicated with
        # their definitions in ci/builder/Dockerfile because `rustc` has no way
        # to merge together Rust flags from different sources.
        rustflags += [
            "-Clink-arg=-fuse-ld=lld",
            f"-L/opt/x-tools/{_target}/{_target}/sysroot/lib",
        ]

    env.update({"RUSTFLAGS": " ".join(rustflags)})

    return [
        *_enter_builder(arch, channel),
        "env",
        *(f"{k}={v}" for k, v in env.items()),
        "cargo",
        subcommand,
        "--target",
        _target,
    ]


def tool(
    arch: Arch, name: str, channel: str | None = None, prefix_name: bool = True
) -> list[str]:
    """Constructs a cross-compiling binutils tool invocation.

    Args:
        arch: The CPU architecture to build for.
        name: The name of the binutils tool to invoke.
        channel: The Rust toolchain channel to use. Either None/"stable" or "nightly".
        prefix_name: Whether or not the tool name should be prefixed with the target
            architecture.

    Returns:
        A list of arguments specifying the beginning of the command to invoke.
    """
    if sys.platform == "darwin":
        _bootstrap_darwin(arch)
    tool_name = f"{target(arch)}-{name}" if prefix_name else name
    return [
        *_enter_builder(arch, channel),
        tool_name,
    ]


def _enter_builder(arch: Arch, channel: str | None = None) -> list[str]:
    if "MZ_DEV_CI_BUILDER" in os.environ or sys.platform == "darwin":
        return []
    else:
        default_channel = (
            "stable"
            if Sanitizer[os.getenv("CI_SANITIZER", "none")] == Sanitizer.none
            else "nightly"
        )
        return [
            "env",
            f"MZ_DEV_CI_BUILDER_ARCH={arch}",
            "bin/ci-builder",
            "run",
            channel if channel else default_channel,
        ]


def _bootstrap_darwin(arch: Arch) -> None:
    # Building in Docker for Mac is painfully slow, so we install a
    # cross-compiling toolchain on the host and use that instead.

    BOOTSTRAP_VERSION = "5"
    BOOTSTRAP_FILE = MZ_ROOT / "target-xcompile" / target(arch) / ".xcompile-bootstrap"
    try:
        contents = BOOTSTRAP_FILE.read_text()
    except FileNotFoundError:
        contents = ""
    if contents == BOOTSTRAP_VERSION:
        return

    spawn.runv(["brew", "install", "lld", f"materializeinc/crosstools/{target(arch)}"])
    spawn.runv(["rustup", "target", "add", target(arch)])

    BOOTSTRAP_FILE.parent.mkdir(parents=True, exist_ok=True)
    BOOTSTRAP_FILE.write_text(BOOTSTRAP_VERSION)
