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
import sys
from collections.abc import Callable
from typing import TypeVar

from materialize.scratch import ScratchInstance

T = TypeVar("T")

PROVIDERS = ("aws", "hetzner")
PROVIDER_DISPLAY = {"aws": "AWS", "hetzner": "Hetzner"}


def check_required_vars(provider: str = "aws") -> None:
    """Set reasonable default values for the
    environment variables necessary to interact with the chosen provider."""
    if provider == "hetzner":
        return
    if not os.environ.get("MZ_SCRATCH_NO_DEFAULT_ENV"):
        os.environ["AWS_PROFILE"] = "mz-scratch-admin"
        if not os.environ.get("AWS_DEFAULT_REGION"):
            os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


def _for_providers(
    provider: str | None,
    fn: Callable[[str], T],
) -> tuple[list[T], list[str]]:
    """Run fn for each provider, collecting results and warnings.
    If provider is specified, errors are raised; otherwise they're warnings."""
    results: list[T] = []
    warnings: list[str] = []
    for p in PROVIDERS if provider is None else (provider,):
        try:
            check_required_vars(p)
            results.append(fn(p))
        except Exception as e:
            if provider is not None:
                raise
            warnings.append(f"{PROVIDER_DISPLAY.get(p, p)}: {e}")
    return results, warnings


def list_all_instances(
    provider: str | None = None,
    owners: list[str] | None = None,
    all: bool = False,
) -> tuple[list[ScratchInstance], list[str]]:
    """List instances from providers, returning instances and warnings."""

    def _list(p: str) -> list[ScratchInstance]:
        if p == "hetzner":
            from materialize import scratch_hetzner

            return scratch_hetzner.list_instances(owners=owners, all=all)
        else:
            from materialize.scratch import list_instances

            return list_instances(owners=owners, all=all)

    batches, warnings = _for_providers(provider, _list)
    return [i for batch in batches for i in batch], warnings


def get_instance(instance_id: str, provider: str | None = None) -> ScratchInstance:
    """Resolve an instance by ID, auto-detecting the provider if not specified."""

    def _get(p: str) -> ScratchInstance:
        if p == "hetzner":
            from materialize.scratch_hetzner import get_instance as hetzner_get

            return hetzner_get(instance_id)
        else:
            from materialize.scratch import get_instance as aws_get

            return aws_get(instance_id)

    results, warnings = _for_providers(provider, _get)
    if results:
        return results[0]
    raise RuntimeError(
        f"Could not find instance '{instance_id}' in any provider:\n"
        + "\n".join(f"  {w}" for w in warnings)
    )


def pick_instance(provider: str | None = None) -> ScratchInstance:
    """Show running instances and let the user pick one."""
    from materialize.scratch import print_instances

    instances, warnings = list_all_instances(provider=provider)
    active = [
        i for i in instances if i.state in ("running", "pending", "stopping", "stopped")
    ]

    if not active:
        for w in warnings:
            print(f"WARNING: {w}", file=sys.stderr)
        raise RuntimeError("No active instances found.")

    if len(active) == 1:
        return active[0]

    print_instances(active, numbered=True)

    for w in warnings:
        print(f"WARNING: {w}", file=sys.stderr)

    while True:
        choice = input("Select an instance [#]: ").strip()
        try:
            idx = int(choice)
            if 1 <= idx <= len(active):
                return active[idx - 1]
        except ValueError:
            pass
        print(f"Invalid choice: {choice!r}. Enter a number 1-{len(active)}.")
