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

asan = [
    #"-Zsanitizer=address",
    #"-Cllvm-args=-asan-use-after-scope",
    #"-Cllvm-args=-asan-use-after-return=always",
    #"-Cllvm-args=-asan-recover",
    #"-Cllvm-args=-asan-stack=false",  # Remove when #25017 is fixed
    # "-Zsanitizer-ignorelist=.config/sanitizer-ignorelist.txt",
    "-Cdebug-assertions=on",
    "-Zstrict-init-checks",
    "-Zextra-const-ub-checks",
    "-Clink-arg=-fuse-ld=lld",  # access beyond end of merged section
]
