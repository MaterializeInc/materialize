# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import os

from mypy_boto3_ec2.service_resource import Instance


def check_required_vars() -> None:
    """Set reasonable default values for the
    environment variables necessary to interact with AWS."""
    if not os.environ.get("MZ_SCRATCH_NO_DEFAULT_ENV"):
        os.environ["AWS_PROFILE"] = "mz-scratch-admin"
        if not os.environ.get("AWS_DEFAULT_REGION"):
            os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


def list_all_instances(
    owners: list[str] | None = None,
    all: bool = False,
) -> list[Instance]:
    """List instances."""
    check_required_vars()
    from materialize.scratch import list_instances

    return list_instances(owners=owners, all=all)


def get_instance(instance_id: str) -> Instance:
    """Resolve an instance by ID."""
    check_required_vars()
    from materialize.scratch import get_instance as _get

    return _get(instance_id)


def pick_instance() -> Instance:
    """Show running instances and let the user pick one."""
    from materialize.scratch import print_instances

    instances = list_all_instances()
    active = [
        i
        for i in instances
        if i.state and i.state["Name"] in ("running", "pending", "stopping", "stopped")
    ]

    if not active:
        raise RuntimeError("No active instances found.")

    if len(active) == 1:
        return active[0]

    print_instances(active, numbered=True)

    while True:
        choice = input("Select an instance [#]: ").strip()
        try:
            idx = int(choice)
            if 1 <= idx <= len(active):
                return active[idx - 1]
        except ValueError:
            pass
        print(f"Invalid choice: {choice!r}. Enter a number 1-{len(active)}.")
