# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum

"""rustc flags."""

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

# Flags for Antithesis coverage instrumentation. The runtime shim comes from
# the `antithesis-instrumentation` crate, which `mz-ore/antithesis` links.
# Symbolization needs GNU build ids and DWARF inside the shipped binary, so
# debuginfo is not split out. See
# https://antithesis.com/docs/reference/sdk/rust/instrumentation/
antithesis = [
    "--cfg=tokio_unstable",
    "-Csplit-debuginfo=off",
    "-Ccodegen-units=1",
    "-Cpasses=sancov-module",
    "-Cllvm-args=-sanitizer-coverage-level=3",
    "-Cllvm-args=-sanitizer-coverage-trace-pc-guard",
    "-Clink-args=-Wl,--build-id",
]

# Build environment for the Antithesis flavor. AWS-LC seeds its DRBG from CPU
# timing jitter and calls `abort()` when the jitter health tests fail, which
# they do on Antithesis's deterministic CPU. Without jitter entropy AWS-LC
# seeds from the OS, with RDRAND as the second source.
antithesis_env = {"AWS_LC_SYS_NO_JITTER_ENTROPY": "1"}


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
