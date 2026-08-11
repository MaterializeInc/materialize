# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum
from pathlib import Path

"""rustc flags."""


class Pgo(Enum):
    """What role a build plays in the profile-guided-optimization cycle."""

    instrument = "instrument"
    optimize = "optimize"
    none = "none"

    def __str__(self) -> str:
        return self.value


# Host directory the profiles land in, relative to the composition running the
# training workload, and the path it is bind-mounted to. `-Cprofile-generate`
# bakes the latter into the binary, so it is a path inside whatever container
# the binary later runs in, not one on the build host.
PGO_HOST_DIR = "pgo"
PGO_PROFILE_DIR = f"/{PGO_HOST_DIR}"


def pgo_generate(profile_dir: str = PGO_PROFILE_DIR) -> list[str]:
    # `--cfg=pgo_instrument` compiles the shutdown hook that lets a
    # signal-terminated clusterd flush its profile. Cluster replicas run the
    # large majority of the CPU work, so without the hook the profile covers
    # only a small share of it.
    return [f"-Cprofile-generate={profile_dir}", "--cfg=pgo_instrument"]


def pgo_use(profdata: Path) -> list[str]:
    return [f"-Cprofile-use={profdata}"]


# Flags to enable code coverage.
#
# Note that because clusterd gets terminated by a signal in most
# cases, it needs to use LLVM's continuous profiling mode, and though
# the documentation is contradictory about this, on Linux this
# requires the additional -runtime-counter-relocation flag or you'll
# get errors of the form "__llvm_profile_counter_bias is undefined"
# and no profiles will be written.
coverage = [
    "-Cinstrument-coverage",
    "-Cllvm-args=-runtime-counter-relocation",
]


class Sanitizer(Enum):
    """What sanitizer to use"""

    address = "address"
    """The AddressSanitizer, see https://clang.llvm.org/docs/AddressSanitizer.html"""

    hwaddress = "hwaddress"
    """The HWAddressSanitizer, see https://clang.llvm.org/docs/HardwareAssistedAddressSanitizerDesign.html"""

    cfi = "cfi"
    """Control Flow Integrity, see https://clang.llvm.org/docs/ControlFlowIntegrity.html"""

    thread = "thread"
    """The ThreadSanitizer, see https://clang.llvm.org/docs/ThreadSanitizer.html"""

    leak = "leak"
    """The LeakSanitizer, see https://clang.llvm.org/docs/LeakSanitizer.html"""

    undefined = "undefined"
    """The UndefinedBehavior, see https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html"""

    none = "none"
    """No sanitizer"""

    def __str__(self) -> str:
        return self.value


sanitizer = {
    Sanitizer.address: [
        "-Zsanitizer=address",
        "-Cllvm-args=-asan-use-after-scope",
        "-Cllvm-args=-asan-use-after-return=always",
        # "-Cllvm-args=-asan-stack=false",  # Remove when database-issues#7468 is fixed
        "-Cdebug-assertions=on",
        "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
        "-Clinker=clang++",
    ],
    Sanitizer.hwaddress: [
        "-Zsanitizer=hwaddress",
        "-Ctarget-feature=+tagged-globals",
        "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
        "-Clinker=clang++",
    ],
    Sanitizer.cfi: [
        "-Zsanitizer=cfi",
        "-Clto",  # error: `-Zsanitizer=cfi` requires `-Clto` or `-Clinker-plugin-lto`
        "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
        "-Clinker=clang++",
    ],
    Sanitizer.thread: [
        "-Zsanitizer=thread",
        "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
        "-Clinker=clang++",
    ],
    Sanitizer.leak: [
        "-Zsanitizer=leak",
        "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
        "-Clinker=clang++",
    ],
    Sanitizer.undefined: ["-Clink-arg=-fsanitize=undefined", "-Clinker=clang++"],
}

sanitizer_cflags = {
    Sanitizer.address: ["-fsanitize=address"],
    Sanitizer.hwaddress: ["-fsanitize=hwaddress"],
    Sanitizer.cfi: ["-fsanitize=cfi"],
    Sanitizer.thread: ["-fsanitize=thread"],
    Sanitizer.leak: ["-fsanitize=leak"],
    Sanitizer.undefined: ["-fsanitize=undefined"],
}
