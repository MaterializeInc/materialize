# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Create-or-connect: if you already have a running scratch instance, SSH into
it. Otherwise, create one from the given machine config and SSH in."""

import argparse
import datetime

from materialize.cli.scratch import check_required_vars
from materialize.cli.scratch.create import MAX_AGE_DAYS, multi_json
from materialize.scratch import (
    MZ_ROOT,
    MachineDesc,
    get_instance,
    launch_cluster,
    mssh,
    name,
    print_instances,
    say,
    tags,
    whoami,
)


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "machine",
        help=(
            "Machine config name from misc/scratch (e.g. dev-box). "
            "Used only when creating a new instance."
        ),
    )
    parser.add_argument(
        "--max-age-days",
        type=float,
        default=MAX_AGE_DAYS,
        help="Maximum age for scratch instance in days. Defaults to 1.5",
    )


def run(args: argparse.Namespace) -> None:
    # Read the machine config
    with open(MZ_ROOT / "misc" / "scratch" / f"{args.machine}.json") as f:
        descs = [MachineDesc.model_validate(obj) for obj in multi_json(f.read())]

    if len(descs) != 1:
        raise RuntimeError(
            f"'go' expects a single-machine config, got {len(descs)} machines"
        )

    check_required_vars()

    # Try to find an existing instance
    try:
        instance = get_instance("mine")
        say(f"Found existing instance {instance.instance_id} ({name(tags(instance))})")
        print_instances([instance], "table")
        print("Connecting...")
        mssh(instance, "")
        return
    except RuntimeError:
        pass

    # No existing instance — create one
    say(f"No existing instance found, creating from {args.machine}...")

    max_age = datetime.timedelta(days=args.max_age_days)
    extra_tags = {"LaunchedBy": whoami()}
    instances = launch_cluster(
        descs,
        extra_tags=extra_tags,
        delete_after=datetime.datetime.utcnow() + max_age,
    )

    print("Launched:")
    print_instances(instances, "table")
    print("Connecting...")
    mssh(instances[0], "")
