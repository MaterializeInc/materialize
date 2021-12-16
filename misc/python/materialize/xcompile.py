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
from pathlib import Path
from typing import List

from materialize import spawn


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


# Hardcoded autoconf test results for libkrb5 about features available in the
# cross toolchain that its configure script cannot auto-detect when cross
# compiling.
KRB5_CONF_OVERRIDES = {
    "krb5_cv_attr_constructor_destructor": "yes",
    "ac_cv_func_regcomp": "yes",
    "ac_cv_printf_positional": "yes",
}


def target(arch: Arch) -> str:
    """Construct a Linux target triple for the specified architecture."""
    return f"{arch}-unknown-linux-gnu"


def cargo(arch: Arch, subcommand: str, rustflags: List[str]) -> List[str]:
    """Construct a Cargo invocation for cross compiling.

    Args:
        arch: The CPU architecture to build for.
        subcommand: The Cargo subcommand to invoke.
        rustflags: Override the flags passed to the Rust compiler. If the list
            is empty, the default flags are used.

    Returns:
        A list of arguments specifying the beginning of the command to invoke.
    """
    _target = target(arch)
    _target_env = _target.upper().replace("-", "_")

    ldflags = []
    rustflags += ["-Clink-arg=-Wl,--compress-debug-sections=zlib"]

    if not sys.platform == "darwin":
        # lld is not yet easy to install on macOS.
        ldflags += ["-fuse-ld=lld"]
        rustflags += ["-Clink-arg=-fuse-ld=lld"]

    env = {
        f"CMAKE_SYSTEM_NAME": "Linux",
        f"CARGO_TARGET_{_target_env}_LINKER": f"{_target}-cc",
        f"LDFLAGS": " ".join(ldflags),
        f"RUSTFLAGS": " ".join(rustflags),
        f"TARGET_AR": f"{_target}-ar",
        f"TARGET_CPP": f"{_target}-cpp",
        f"TARGET_CC": f"{_target}-cc",
        f"TARGET_CXX": f"{_target}-c++",
        f"TARGET_LD": f"{_target}-ld",
        f"TARGET_RANLIB": f"{_target}-ranlib",
        **KRB5_CONF_OVERRIDES,
    }

    # Handle the confusing situation of "cross compiling" from x86_64 Linux to
    # x86_64 Linux. This counts as a cross compile because, while the platform
    # is identical, we're targeting an older kernel and libc. In an ideal world,
    # our dependencies would use the native toolchain (e.g., `cc`) when compiled
    # as build dependencies, but the cross toolchain (e.g.,
    # `x86_64-unknown-linux-gnu-cc`) when compiled as normal dependencies. But
    # the build systems of several of our dependencies flub this distinction and
    # use the native toolchain (`cc`) when compiled as a normal dependency.
    # Fixing the build systems is hard, so instead we use the cross toolchain
    # for build dependencies too, by setting `CC` in addition to `TARGET_CC`,
    # `CPP` in addition to `TARGET_CPP`, etc. That means we're incorrectly using
    # the cross toolchain to compile build dependencies, but it works out
    # because we're still able to *run* those dependencies on the build machine,
    # and this way we guarantee that the buggy dependencies use the cross
    # toolchain when compiled as normal dependencies, which is the actually
    # important consideration.
    if Arch.host() == arch and sys.platform != "darwin":
        for k, v in env.copy().items():
            if k.startswith("TARGET_"):
                k = k[len("TARGET_") :]
                env[k] = v

    return [
        *_enter_builder(arch),
        "env",
        *(f"{k}={v}" for k, v in env.items()),
        "cargo",
        subcommand,
        "--target",
        _target,
    ]


def tool(arch: Arch, name: str) -> List[str]:
    """Constructs a cross-compiling binutils tool invocation.

    Args:
        arch: The CPU architecture to build for.
        name: The name of the binutils tool to invoke.

    Returns:
        A list of arguments specifying the beginning of the command to invoke.
    """
    return [
        *_enter_builder(arch),
        f"{target(arch)}-{name}",
    ]


def _enter_builder(arch: Arch) -> List[str]:
    assert (
        arch == Arch.host()
    ), f"target architecture {arch} does not match host architecture {Arch.host()}"
    if "MZ_DEV_CI_BUILDER" in os.environ:
        return []
    elif sys.platform == "darwin":
        # Building in Docker for Mac is painfully slow, so we install a
        # cross-compiling toolchain on the host and use that instead.
        _bootstrap_darwin(arch)
        return []
    else:
        return ["bin/ci-builder", "run", "stable"]


def _bootstrap_darwin(arch: Arch) -> None:
    BOOTSTRAP_VERSION = "3"
    BOOTSTRAP_FILE = (
        Path(os.environ["MZ_ROOT"]) / "target" / target(arch) / ".xcompile-bootstrap"
    )
    try:
        contents = BOOTSTRAP_FILE.read_text()
    except FileNotFoundError:
        contents = ""
    if contents == BOOTSTRAP_VERSION:
        return

    if arch == Arch.AARCH64:
        spawn.runv(
            ["brew", "install", f"messense/macos-cross-toolchains/{target(arch)}"]
        )
    elif arch == Arch.X86_64:
        # Get rid of one we used for all arches in boostrap v2 just in case a conflict makes things tricky
        spawn.runv(
            [
                "brew",
                "uninstall",
                "-f",
                f"messense/macos-cross-toolchains/{target(arch)}",
            ]
        )
        try:
            spawn.runv(["brew", "untap", "-f", f"messense/macos-cross-toolchains"])
        except Exception:
            print(
                "Could not untap messense/macos-cross-toolchain.  Perhaps because it's not tapped"
            )
        spawn.runv(["brew", "install", f"SergioBenitez/osxct/{target(arch)}"])
        spawn.runv(
            ["cargo", "clean", "--target", f"{target(arch)}"],
            cwd=Path(os.environ["MZ_ROOT"]),
        )
    else:
        raise RuntimeError("python enums are worse than rust enums")

    spawn.runv(["rustup", "target", "add", target(arch)])
    BOOTSTRAP_FILE.parent.mkdir(parents=True, exist_ok=True)
    BOOTSTRAP_FILE.write_text(BOOTSTRAP_VERSION)
