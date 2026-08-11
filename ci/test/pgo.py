# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Collect the profile that the LTO build optimizes against.

An LTO build is profile-guided, so it has to produce its own profile first:
this builds instrumented binaries, runs a workload against them, and merges
what they leave behind. Nothing is carried between builds, so a profile can
never go stale against the source it is applied to. Both builds share
`target-xcompile` and their rustflags differ, so an LTO build ends up
compiling the workspace twice.
"""

import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from materialize import MZ_ROOT, mzbuild, rustc_flags, spawn, ui
from materialize.mzcompose.composition import Composition, Service
from materialize.rustc_flags import Pgo
from materialize.ui import UIError

# The sqllogictest image is the only one that has to be built instrumented,
# and it covers both halves of the profile: sqllogictest embeds environmentd,
# so the adapter and optimizer code the environmentd binary is built from runs
# in-process, and it spawns real clusterd replicas for the compute code. A
# profile is keyed on function names, so counts from the embedded copy apply
# to the same functions in the environmentd binary.
IMAGE = "sqllogictest"

# The mzbuild image behind the composition's default `postgres-metadata`
# service, which serves persist consensus for the run.
METADATA_IMAGE = "postgres"

# Only the top level of the sqllogictest tree. The sqlite/ and cockroach/
# trees below it are ports of other engines' suites, large enough to dominate
# the run and skewed towards SQL parsing rather than the code we care about.
TRAINING_FILES = "test/sqllogictest/*.slt"

# Each worker takes one shard, so a run covers WORKERS/SHARDS of the files.
# Training on all of them takes far longer than a build can spare, and PGO
# needs to learn which code is hot, not to reach every branch. Every worker
# is a full instrumented Materialize, so the worker count is bounded by
# memory, not cores.
WORKERS = 4
SHARDS = 16

# The two binaries a run profiles, sqllogictest and clusterd. Fewer means one
# of them ran without leaving anything behind.
EXPECTED_PROFILED_BINARIES = 2

PROFDATA = MZ_ROOT / "target-xcompile" / "pgo.profdata"


def train(profile: mzbuild.Profile) -> Path:
    """Run the training workload and return the merged profile.

    Instruments with the cargo profile the result will be applied to, so that
    the binaries the counts come from are compiled like the ones that consume
    them.
    """
    # An absolute root, as the mzcompose CLI uses. Services that bind-mount a
    # generated file build the mount from the composition's path, and docker
    # reads a relative source as a named volume rather than a bind mount.
    repo = mzbuild.Repository(
        MZ_ROOT,
        profile=profile,
        pgo=Pgo.instrument,
        image_registry="materialize",
    )

    ui.section("Building instrumented images for PGO training")
    # Only the images the training run actually starts. Nothing is published:
    # an instrumented image is useful to nothing but the run it was built for,
    # and it is far slower than the image whose name it shares. The metadata
    # store holds no Rust, so it resolves to its ordinary tag and is pulled
    # rather than built.
    images = [repo.images[IMAGE], repo.images[METADATA_IMAGE]]
    repo.resolve_dependencies(images).ensure(push=False)

    composition = Composition(repo, IMAGE)
    profile_dir = composition.path / rustc_flags.PGO_HOST_DIR
    shutil.rmtree(profile_dir, ignore_errors=True)
    profile_dir.mkdir(parents=True)
    # The profile runtime creates the files itself, from whatever user the
    # image runs as.
    profile_dir.chmod(0o777)

    files = sorted(str(path) for path in Path(".").glob(TRAINING_FILES))
    if not files:
        raise UIError(f"PGO training workload {TRAINING_FILES} matched no files")

    ui.section(f"Training on {len(files)} sqllogictest files, {WORKERS} at a time")
    try:
        composition.up(
            composition.metadata_store(),
            *[Service(f"slt_{i + 1}", idle=True) for i in range(WORKERS)],
        )
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [
                executor.submit(_train_worker, composition, shard, files)
                for shard in range(WORKERS)
            ]
            for future in as_completed(futures):
                # A worker that dies still leaves behind whatever it had
                # written, and one crashing shard is not worth failing every
                # release build over. The profile pool check below is what
                # decides whether the run was usable.
                try:
                    future.result()
                except Exception as e:
                    print(f"PGO training worker failed, continuing: {e}")
    finally:
        try:
            composition.down()
        except Exception as e:
            # Not worth failing a build over once the profiles are on disk.
            # Log capture in particular fails outright against a container
            # that was killed mid-run.
            print(f"PGO training cleanup failed, continuing: {e}")

    # `%m` expands to <binary signature>_<pool index>, behind the name of the
    # service that wrote it, so distinct signatures are distinct binaries.
    profraws = sorted(profile_dir.glob("*.profraw"))
    binaries = {p.stem.rsplit("-", 1)[-1].split("_")[0] for p in profraws}
    print(f"Collected {len(profraws)} profile(s) from {len(binaries)} binaries")
    if len(binaries) < EXPECTED_PROFILED_BINARIES:
        raise UIError(
            f"PGO training profiled {len(binaries)} binaries, expected "
            f"{EXPECTED_PROFILED_BINARIES}",
            hint="A binary that leaves no profile is compiled as if all of it "
            "were cold, which is worse than not applying PGO to it at all.",
        )

    ui.section("Merging profiles")
    # Discards profiles truncated by a process that died mid-write, which a
    # plain `llvm-profdata merge` would fail the whole merge over.
    spawn.runv(["bin/ci-validate-profraws", PROFDATA], cwd=MZ_ROOT)
    return PROFDATA


def _train_worker(composition: Composition, shard: int, files: list[str]) -> None:
    service = f"slt_{shard + 1}"
    composition.exec(
        service,
        "sqllogictest",
        # Training cares about which code runs, not about whether the
        # assertions hold. Failing here would make every LTO build hostage to
        # an unrelated SLT regression.
        "--no-fail",
        # Without a command wrapper the process orchestrator sends replicas
        # SIGKILL, which no handler can catch, and the clusterd half of the
        # profile is lost. `env` execs its argument in the same process, so it
        # changes nothing but making the signal arrive.
        "--orchestrator-process-wrapper=env",
        f"--postgres-url=postgres://root@{composition.metadata_store()}:26257",
        f"--prefix={service}",
        f"--shard={shard}",
        f"--shard-count={SHARDS}",
        *files,
        capture=True,
        capture_stderr=True,
    )
    print(f"PGO training worker {service} finished")
