# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import os
from pathlib import Path

from materialize import spawn
from materialize.xcompile import KRB5_CONF_OVERRIDES, Arch

from . import deploy_util


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("arch", choices=[Arch.X86_64, Arch.AARCH64])
    args = parser.parse_args()

    target = f"{args.arch}-apple-darwin"
    print(f"Target: {target}")

    print("--- Building materialized release binary")
    spawn.runv(
        ["cargo", "build", "--target", target, "--bin", "materialized", "--release"],
        env=dict(
            os.environ,
            # Cross compiling from x86_64-apple-darwin to aarch64-apple-darwin
            # or vice-versa is unusual because you don't need a purpose-built
            # cross compiler. Instead, you just pass the `--target` flag to
            # clang. CMake understands this, but autoconf does not. So
            # explicitly set the `--target` flag to help our autoconf-based C
            # dependencies along.
            CFLAGS=f"--target={target}",
            **KRB5_CONF_OVERRIDES,
        ),
    )

    print("--- Uploading binary tarball")
    deploy_util.deploy_tarball(
        target, Path("target") / target / "release" / "materialized"
    )


if __name__ == "__main__":
    main()
