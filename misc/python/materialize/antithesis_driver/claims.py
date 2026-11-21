# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""What the Materialize CR's status claims about the active generation.

`UpToDate=True/Applied` claims that the active environmentd StatefulSet runs
the spec, and that the completed request and image are the spec's.
`UpToDate=False/WaitingForApproval` claims that the spec has changes which
that StatefulSet does not run. `spec_model.status_check` judges one CR read
against one StatefulSet.

A claim is judged only when the CR did not change while the StatefulSets were
listed. A violation is asserted only once a second observation, at least
`STATUS_SETTLE_SECONDS` later, still sees it under the same key, so it
describes a settled state and not one read in the middle of a reconcile pass.

A spec change during `Promoting` is a filed cause of violations (CLO-339).
Those get messages of their own, so they hide no other cause. The messages are
those of the legacy rollout observer.
"""

from __future__ import annotations

import time
from typing import Any

from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    reachable,
)

from materialize import orchestratord
from materialize.antithesis import spec_model

STATUS_SETTLE_SECONDS = 30.0


def assert_status_claim(cause: str, holds: bool, details: dict[str, Any]) -> None:
    """The only site of each status claim message. `cause` is a
    `spec_model.claim_cause` value."""
    details = {**details, "cause": cause}
    if cause == "applied":
        always(
            holds,
            "Applied status implies the active StatefulSet runs the spec at the observed generation, and the completed request and image are the spec's",
            details,
        )
    elif cause == "applied_promoting_same_request":
        always_or_unreachable(
            holds,
            "Applied status after a spec change during Promoting without a new requestRollout implies the active StatefulSet runs the spec (CLO-339)",
            details,
        )
    elif cause == "applied_promoting_new_request":
        always_or_unreachable(
            holds,
            "Applied status after a spec change during Promoting with a new requestRollout implies the active StatefulSet runs the spec (CLO-339)",
            details,
        )
    elif cause == "applied_promoted_across":
        always_or_unreachable(
            holds,
            "Applied status for a generation promoted across a spec change during Promoting implies the active StatefulSet runs the spec (CLO-339)",
            details,
        )
    elif cause == "waiting":
        always_or_unreachable(
            holds,
            "WaitingForApproval is reported only while the spec differs from the active StatefulSet",
            details,
        )
    elif cause == "waiting_empty_hash":
        always_or_unreachable(
            holds,
            "WaitingForApproval with an empty resourcesHash is reported only while the spec differs from the active StatefulSet (known: full revert after Applying)",
            details,
        )
    elif cause == "waiting_promoted_across":
        always_or_unreachable(
            holds,
            "WaitingForApproval for a generation promoted across a spec change during Promoting is reported only while the spec differs from the active StatefulSet (CLO-339)",
            details,
        )
    else:
        raise ValueError(f"unknown status claim cause {cause}")


def resource_version(cr: dict[str, Any]) -> str | None:
    return (cr.get("metadata") or {}).get("resourceVersion")


class Claims:
    def __init__(self) -> None:
        self._promoting_specs: dict[int, dict[str, Any]] = {}
        """The rendered spec each generation was `Promoting` away from with,
        keyed by the generation that was active then."""
        self._promoted_across: set[int] = set()
        self._promoting_patch: dict[int, str] = {}
        self._pending: tuple[tuple[Any, ...], float] | None = None

    @property
    def pending(self) -> bool:
        """Whether a violated claim awaits its confirming observation."""
        return self._pending is not None

    def patched_while_promoting(self, candidate: int, request_mode: str) -> None:
        """The driver changed a rendered spec field while `candidate` was being
        promoted.

        `_track_promotion` can miss such a promotion: when orchestratord
        restarts in the middle of it, the `Promoting` status may never be
        observed at the patched spec generation.
        """
        self._promoting_patch[candidate] = request_mode
        self._promoted_across.add(candidate)

    def _track_promotion(self, cr: dict[str, Any]) -> None:
        """Record a generation promoted with a spec whose rendered fields
        differ from the spec it was applied with.

        The `Promoting` status is written by the pass that applied the
        candidate's StatefulSet, at that pass's spec. The promotion writes
        `Applied` at the spec of its own pass without applying the StatefulSet
        again.
        """
        active = (cr.get("status") or {}).get("activeGeneration")
        condition = spec_model.up_to_date(cr)
        generation = (cr.get("metadata") or {}).get("generation")
        if not isinstance(active, int) or generation is None:
            return
        if condition.get("observedGeneration") != generation:
            return
        if condition.get("reason") == "Promoting":
            self._promoting_specs.setdefault(active, spec_model.deployed_spec(cr))
            for old in [g for g in self._promoting_specs if g < active - 1]:
                del self._promoting_specs[old]
        elif condition.get("reason") == "Applied" and condition.get("status") == "True":
            applied_with = self._promoting_specs.pop(active - 1, None)
            promoted_with = spec_model.deployed_spec(cr)
            if applied_with is not None and applied_with != promoted_with:
                self._promoted_across.add(active)
                reachable(
                    "Observer saw a promotion complete with a spec changed during Promoting",
                    {
                        "active": active,
                        "applied_with": applied_with,
                        "promoted_with": promoted_with,
                    },
                )

    def observe(
        self, materialize: orchestratord.Materialize, cr: dict[str, Any]
    ) -> None:
        """Judge the status claim of `cr`, a read of the CR just taken."""
        self._track_promotion(cr)
        active = (cr.get("status") or {}).get("activeGeneration")
        if not isinstance(active, int):
            return
        statefulset = next(
            (
                g
                for g in materialize.environmentd_generations()
                if g.generation == active and not g.deleting
            ),
            None,
        )
        if statefulset is None:
            return
        check = spec_model.status_check(
            cr,
            spec_model.Deployed(
                statefulset.image,
                statefulset.force,
                statefulset.args,
                statefulset.resources,
            ),
        )
        if check is None:
            return
        again = materialize.get()
        if again is None or resource_version(again) != resource_version(cr):
            return
        cause = spec_model.claim_cause(
            check, self._promoting_patch.get(active), active in self._promoted_across
        )
        status = cr.get("status") or {}
        details = {
            "active": active,
            "generation": (cr.get("metadata") or {}).get("generation"),
            "claim": check.kind,
            "mismatches": check.mismatches,
            "resources_hash_empty": check.empty_resources_hash,
            "statefulset": statefulset.statefulset_name,
            "spec": spec_model.deployed_spec(cr),
            "request": (cr.get("spec") or {}).get("requestRollout"),
            "last_completed": status.get("lastCompletedRolloutRequest"),
        }
        now = time.monotonic()
        if not check.violated:
            self._pending = None
            assert_status_claim(cause, True, details)
        elif self._pending is None or self._pending[0] != check.key:
            self._pending = (check.key, now)
        elif now - self._pending[1] >= STATUS_SETTLE_SECONDS:
            assert_status_claim(
                cause,
                False,
                {**details, "violated_for_seconds": now - self._pending[1]},
            )
