# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Build profile-guided-optimization binaries.

Two steps, with a training workload run between them:

    bin/pgo-build instrument
    <run a representative workload against the instrumented binaries>
    bin/pgo-build optimize

Only a process that exits through its exit handlers writes a usable profile,
which is what `TRAINING_RUN_ARGS` below is about. A training run that skips it
collects nothing from the cluster replicas, where most of the work happens.
"""

import argparse
import os
import shlex
import shutil
from pathlib import Path

import toml

from materialize import MZ_ROOT, rustc_flags, spawn

TARGET_DIR = MZ_ROOT / "target"
PGO_PROFILE_DIR = TARGET_DIR / "pgo"
MERGED_PROFDATA = PGO_PROFILE_DIR / "merged.profdata"

DEFAULT_BINS = ["environmentd", "clusterd"]

# Training runs must reach the binaries through a command wrapper, because the
# process orchestrator only sends SIGTERM to replicas when one is configured
# and otherwise goes straight to SIGKILL. `env` execs its argument in the same
# process, so it changes nothing except making the signal arrive.
TRAINING_RUN_ARGS = "--orchestrator-process-wrapper=env"


def host_triple() -> str:
    for line in spawn.capture(["rustc", "-vV"]).splitlines():
        if line.startswith("host: "):
            return line.removeprefix("host: ")
    raise RuntimeError("could not determine host triple from `rustc -vV`")


def base_rustflags(triple: str) -> list[str]:
    """Compute the rustflags that would apply to a normal build.

    Setting the RUSTFLAGS environment variable makes cargo ignore every
    rustflags entry in .cargo/config.toml, so to append flags we must
    replicate the flags that would otherwise apply. Cargo uses the first
    match of: the RUSTFLAGS environment variable, target.<triple>.rustflags,
    build.rustflags.
    """
    env_flags = os.environ.get("RUSTFLAGS")
    if env_flags is not None:
        return shlex.split(env_flags)
    config = toml.load(MZ_ROOT / ".cargo" / "config.toml")
    target_flags = config.get("target", {}).get(triple, {}).get("rustflags")
    if target_flags is not None:
        return list(target_flags)
    return list(config.get("build", {}).get("rustflags", []))


def cargo_build(
    args: argparse.Namespace, extra_rustflags: list[str], triple: str
) -> list[Path]:
    env = dict(
        os.environ,
        RUSTFLAGS=" ".join(base_rustflags(triple) + extra_rustflags),
    )
    # Pass --target explicitly even though we only build for the host. With an
    # explicit target, cargo applies RUSTFLAGS only to target artifacts,
    # keeping build scripts and proc macros out of the instrumented build, and
    # puts those artifacts under target/<triple>/, where they do not displace
    # an ordinary `cargo build`.
    cmd = ["cargo", "build", "--profile", args.cargo_profile, "--target", triple]
    for bin in args.bins:
        cmd += ["--bin", bin]
    spawn.runv(cmd, cwd=MZ_ROOT, env=env)
    out_profile = "debug" if args.cargo_profile == "dev" else args.cargo_profile
    return [TARGET_DIR / triple / out_profile / bin for bin in args.bins]


def find_profdata(triple: str) -> Path | str:
    """Find an llvm-profdata that matches rustc's LLVM version.

    The profraw format is not stable across LLVM versions, so prefer the
    tools shipped with the toolchain over whatever is on the PATH.
    """
    sysroot = Path(spawn.capture(["rustc", "--print", "sysroot"]).strip())
    tool = sysroot / "lib" / "rustlib" / triple / "bin" / "llvm-profdata"
    if tool.exists():
        return tool
    for name in ["rust-profdata", "llvm-profdata"]:
        if shutil.which(name):
            return name
    raise SystemExit(
        "error: llvm-profdata not found\n"
        "hint: run `rustup component add llvm-tools` to install it"
    )


def do_instrument(args: argparse.Namespace) -> None:
    triple = host_triple()
    PGO_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    for stale in PGO_PROFILE_DIR.glob("*.profraw"):
        stale.unlink()
    bins = cargo_build(args, rustc_flags.pgo_generate(str(PGO_PROFILE_DIR)), triple)
    print("\nInstrumented binaries:")
    for bin in bins:
        print(f"  {bin}")
    print(
        "\nRun a representative workload against them. Profiles are written to\n"
        f"  {PGO_PROFILE_DIR}\n"
        "automatically, one merge pool per binary. Then run\n"
        "`bin/pgo-build optimize`.\n"
        "\n"
        "IMPORTANT: pass\n"
        f"  {TRAINING_RUN_ARGS}\n"
        "to the workload, or cluster replicas contribute no profile. Check\n"
        "that the pool count matches the number of instrumented binaries\n"
        "before trusting a training run: a missing pool means that binary\n"
        "gets built as if every function in it were cold, which is worse\n"
        "than not applying PGO to it at all."
    )


def do_optimize(args: argparse.Namespace) -> None:
    triple = host_triple()
    profraws = sorted(PGO_PROFILE_DIR.glob("*.profraw"))
    if not profraws:
        raise SystemExit(
            f"error: no .profraw files in {PGO_PROFILE_DIR}\n"
            "hint: run `bin/pgo-build instrument` and exercise the "
            "instrumented binaries first"
        )
    if len(profraws) < len(args.bins):
        print(
            f"warning: {len(profraws)} profile pool(s) for {len(args.bins)} "
            "binaries. A binary with no profile is built as if all of it were "
            "cold, which is worse than not applying PGO to it at all.\nfound: "
            + ", ".join(p.name for p in profraws)
        )
    spawn.runv(
        [find_profdata(triple), "merge", "-o", MERGED_PROFDATA, *profraws],
        cwd=MZ_ROOT,
    )
    bins = cargo_build(args, rustc_flags.pgo_use(MERGED_PROFDATA), triple)
    print("\nPGO-optimized binaries:")
    for bin in bins:
        print(f"  {bin}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pgo-build",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )
    parser.add_argument(
        "--bin",
        dest="bins",
        action="append",
        metavar="NAME",
        help=f"binary to build (default: {', '.join(DEFAULT_BINS)})",
    )
    parser.add_argument(
        "--cargo-profile",
        default="release",
        help="cargo profile to build with (default: %(default)s)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    p = subparsers.add_parser(
        "instrument", help="build binaries instrumented for PGO profile collection"
    )
    p.set_defaults(func=do_instrument)
    p = subparsers.add_parser(
        "optimize", help="merge collected profiles and rebuild with PGO"
    )
    p.set_defaults(func=do_optimize)
    args = parser.parse_args()
    if args.bins is None:
        args.bins = DEFAULT_BINS
    args.func(args)


if __name__ == "__main__":
    main()
