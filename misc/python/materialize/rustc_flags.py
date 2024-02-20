# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

sanitizer = {
    "address": [
        "-Zsanitizer=address",
        # "-Cllvm-args=-asan-use-after-scope",
        # "-Cllvm-args=-asan-use-after-return=always",
        # "-Cllvm-args=-asan-recover",
        "-Cllvm-args=-asan-stack=false",  # Remove when #25017 is fixed
        # "-Cdebug-assertions=on",
        "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
        # "-Clink-arg=-fsanitize=address",
        "-Clinker=clang",  # doesn't work for all, still using ~/.local/bin/cc
        # "-Clink-arg=-Wl,-rpath=/usr/lib/llvm-17/lib/clang/17/lib/linux",
        # "-Clink-arg=-lasan",
        # "-Clink-arg=-static-libsan",
        # "-Clink-arg=-Wl,-rpath=/home/deen/.rustup/toolchains/nightly-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/lib",
        # "-Clink-arg=-Wl,-rpath=/usr/lib/llvm-17/lib/clang/17/lib/linux",
    ],
    "hwaddress": [
        "-Zsanitizer=hwaddress",
        "-Ctarget-feature=+tagged-globals",
        "-Cdebug-assertions=off",
        "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
        "-Clink-arg=-fsanitize=hwaddress",
        "-Clinker=cc",
    ],
    "cfi": [
        "-Zsanitizer=cfi",
        "-Cdebug-assertions=off",
        "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
        "-Clink-arg=-fsanitize=cfi",
        "-Clinker=cc",
    ],
    "thread": [
        "-Zsanitizer=thread",
        "-Cdebug-assertions=off",
        "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
        "-Clink-arg=-fsanitize=thread",
        "-Clinker=cc",
    ],
    "leak": [
        "-Zsanitizer=leak",
        "-Cdebug-assertions=off",
        "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
        "-Clink-arg=-fsanitize=leak",
        "-Clinker=cc",
    ],
    "undefined": [],
}

# TODO: My issue: https://github.com/rust-lang/rust/issues/114127
# TODO: export LD_LIBRARY_PATH="/usr/lib/llvm-17/lib/clang/17/lib/linux"
# export LD_PRELOAD=/usr/bin/../lib/gcc/x86_64-linux-gnu/13/libasan.so
# export ASAN_OPTIONS=detect_leaks=0:verify_asan_link_order=0
# Alternatively make cc detect when it's building the script, then without asan
sanitizer_cflags = {
    "address": [
        "-fsanitize=address",
        # "-static-libsan",
        # "-Wl,-rpath=/home/deen/.rustup/toolchains/nightly-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/lib",
        ##"-Wl,-rpath=/usr/lib/llvm-17/lib/clang/17/lib/linux",
        # "-fsanitize-address-use-after-scope",
    ],
    "hwaddress": ["-fsanitize=hwaddress"],
    "cfi": ["-fsanitize=cfi"],
    "thread": ["-fsanitize=thread"],
    "leak": ["-fsanitize=leak"],
    "undefined": ["-fsanitize=undefined"],
}
