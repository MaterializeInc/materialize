# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""One-click: create-or-reuse a scratch instance and start Claude Code."""

import argparse
import datetime

from materialize.cli.scratch import check_required_vars, list_all_instances
from materialize.cli.scratch.create import MAX_AGE_DAYS, multi_json, pick_machine
from materialize.scratch import (
    MZ_ROOT,
    MachineDesc,
    launch_cluster,
    mssh,
    print_instances,
    say,
    whoami,
)


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "machine",
        nargs="?",
        default=None,
        help="Machine config name from misc/scratch (e.g. dev-box).",
    )
    parser.add_argument(
        "--max-age-days",
        type=float,
        default=MAX_AGE_DAYS,
        help="Maximum age for scratch instance in days. Defaults to 1.5",
    )


def run(args: argparse.Namespace) -> None:
    machine = args.machine or pick_machine()

    with open(MZ_ROOT / "misc" / "scratch" / f"{machine}.json") as f:
        descs = [MachineDesc.model_validate(obj) for obj in multi_json(f.read())]

    if len(descs) != 1:
        raise RuntimeError(
            f"'claude' expects a single-machine config, got {len(descs)} machines"
        )

    check_required_vars()

    # Find existing instances
    existing = list_all_instances()
    active = [
        i for i in existing if i.state and i.state["Name"] in ("running", "pending")
    ]

    if active:
        print_instances(active, numbered=True)
        print(f"  {len(active) + 1}) Create new instance ({machine})")
        while True:
            choice = input("Select an instance or create new [#]: ").strip()
            try:
                idx = int(choice)
                if 1 <= idx <= len(active):
                    instance = active[idx - 1]
                    break
                if idx == len(active) + 1:
                    instance = None
                    break
            except ValueError:
                pass
            print(f"Invalid choice. Enter a number 1-{len(active) + 1}.")
    else:
        instance = None

    if instance is None:
        say(f"Creating from {machine}...")
        max_age = datetime.timedelta(days=args.max_age_days)
        extra_tags = {"LaunchedBy": whoami()}
        instances = launch_cluster(
            descs,
            extra_tags=extra_tags,
            delete_after=datetime.datetime.utcnow() + max_age,
        )

        print("Launched:")
        print_instances(instances, "table")
        instance = instances[0]

    import shlex

    extra_args = getattr(args, "extra_args", [])
    extra = " ".join(shlex.quote(a) for a in extra_args)
    claude_cmd = f"cd materialize && claude --dangerously-skip-permissions --remote-control {extra}".rstrip()
    print("Starting Claude Code in screen session 'claude'...")
    mssh(
        instance,
        f"screen -xRR claude bash -c {shlex.quote(claude_cmd)}",
        extra_ssh_args=["-o", "RequestTTY=force"],
    )
