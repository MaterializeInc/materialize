# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""What a Materialize CR spec deploys, and the spec-field-by-phase cells.

Pure logic shared by the rollout observer and the spec-phase driver, kept free
of the Antithesis SDK and the Kubernetes client so it can be unit tested.

orchestratord renders four spec inputs into the environmentd StatefulSet of a
generation: the image, the extra args, the force value (`spec.forceRollout`
joined with the `materialize.cloud/force-rollout` CR annotation, written as
the StatefulSet's `materialize.cloud/force` annotation), and the resource
requirements. `requestRollout`, `rolloutStrategy`, `forcePromote` and
`rolloutRequestTimeout` steer the rollout but are not rendered.
"""

from __future__ import annotations

import random
import re
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

FORCE_ROLLOUT_ANNOTATION = "materialize.cloud/force-rollout"
NIL_UUID = "00000000-0000-0000-0000-000000000000"
DEFAULT_ROLLOUT_STRATEGY = "WaitUntilReady"
MIGRATION_ARG_PREFIX = "--unsafe-force-builtin-schema-migration="

# Spec keys rendered into the environmentd StatefulSet.
DEPLOYED_SPEC_KEYS = (
    "environmentdImageRef",
    "environmentdExtraArgs",
    "forceRollout",
    "environmentdResourceRequirements",
)

FIELDS = (
    "image",
    "extra_args",
    "force_rollout",
    "request_rollout",
    "rollout_strategy",
    "resources",
    "revert",
)
PHASES = ("Applying", "ReadyToPromote", "Promoting", "Applied", "WaitingForApproval")
IN_PROGRESS_PHASES = ("Applying", "ReadyToPromote", "Promoting")
# `keep` leaves `requestRollout` as it is, `fresh` sets a new one, `restore`
# sets the one the spec carried before the cell's setup patch.
REQUEST_MODES = ("keep", "fresh", "restore")

# The forced-migration args `rollouts.toggled_migration_args` uses. Each is a
# full value of `environmentdExtraArgs`; None removes the field.
EXTRA_ARGS_MENU: tuple[tuple[str, ...] | None, ...] = (
    None,
    ("--unsafe-mode", f"{MIGRATION_ARG_PREFIX}replacement"),
    ("--unsafe-mode", f"{MIGRATION_ARG_PREFIX}evolution"),
)

# Requests vary, the memory limit stays at the manifest's 2Gi so the resource
# kill check's notion of environmentd's own limit does not move. The first
# entry is the manifest's value.
RESOURCES_MENU: tuple[dict[str, dict[str, str]], ...] = (
    {"requests": {"cpu": "50m", "memory": "512Mi"}, "limits": {"memory": "2Gi"}},
    {"requests": {"cpu": "100m", "memory": "512Mi"}, "limits": {"memory": "2Gi"}},
    {"requests": {"cpu": "50m", "memory": "768Mi"}, "limits": {"memory": "2Gi"}},
)

STRATEGIES = ("WaitUntilReady", "ManuallyPromote")


def merge_patch(target: Any, patch: Any) -> Any:
    """`target` after the JSON merge patch `patch` (RFC 7386)."""
    if not isinstance(patch, dict):
        return patch
    result = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        else:
            result[key] = merge_patch(result.get(key), value)
    return result


_QUANTITY = re.compile(
    r"^([+-]?(?:\d+\.?\d*|\.\d+))(?:[eE]([+-]?\d+))?(Ki|Mi|Gi|Ti|Pi|Ei|n|u|m|k|M|G|T|P|E)?$"
)
_SUFFIX = {
    None: Decimal(1),
    "n": Decimal("1e-9"),
    "u": Decimal("1e-6"),
    "m": Decimal("1e-3"),
    "k": Decimal("1e3"),
    "M": Decimal("1e6"),
    "G": Decimal("1e9"),
    "T": Decimal("1e12"),
    "P": Decimal("1e15"),
    "E": Decimal("1e18"),
    "Ki": Decimal(2**10),
    "Mi": Decimal(2**20),
    "Gi": Decimal(2**30),
    "Ti": Decimal(2**40),
    "Pi": Decimal(2**50),
    "Ei": Decimal(2**60),
}


def parse_quantity(text: str | int | float) -> Decimal:
    """A Kubernetes resource quantity as a number. The API server may
    re-serialize a quantity in another form, so quantities are compared by
    value. Raises ValueError on a malformed quantity."""
    match = _QUANTITY.match(str(text).strip())
    if match is None:
        raise ValueError(f"not a quantity: {text!r}")
    try:
        value = Decimal(match[1])
    except InvalidOperation as e:
        raise ValueError(f"not a quantity: {text!r}") from e
    if match[2] is not None:
        value = value.scaleb(int(match[2]))
    return value * _SUFFIX[match[3]]


def canonical_resources(value: Any) -> dict[str, dict[str, str]]:
    """Resource requirements with each quantity by value and empty maps
    dropped. An unparseable quantity is kept as written."""
    result: dict[str, dict[str, str]] = {}
    for section in ("limits", "requests"):
        entries = (value or {}).get(section) if isinstance(value, dict) else None
        if not entries:
            continue
        canonical = {}
        for name, quantity in entries.items():
            try:
                canonical[name] = str(parse_quantity(quantity).normalize())
            except ValueError:
                canonical[name] = str(quantity)
        result[section] = canonical
    return result


def extra_args(spec: dict[str, Any]) -> tuple[str, ...] | None:
    args = spec.get("environmentdExtraArgs")
    return tuple(args) if args else None


def expected_force(cr: dict[str, Any]) -> str:
    """The `materialize.cloud/force` value orchestratord derives from a CR, as
    `force_rollout_value` does. An absent `forceRollout` deserializes to the
    nil UUID."""
    spec = cr.get("spec") or {}
    force = spec.get("forceRollout") or NIL_UUID
    annotation = ((cr.get("metadata") or {}).get("annotations") or {}).get(
        FORCE_ROLLOUT_ANNOTATION
    )
    return f"{force}/{annotation}" if annotation is not None else str(force)


def deployed_spec(cr: dict[str, Any]) -> dict[str, Any]:
    """The rendered inputs of a CR, JSON-serializable and comparable by `==`."""
    spec = cr.get("spec") or {}
    args = extra_args(spec)
    resources = spec.get("environmentdResourceRequirements")
    return {
        "image": spec.get("environmentdImageRef"),
        "extra_args": list(args) if args else None,
        "force": expected_force(cr),
        "resources": canonical_resources(resources) if resources else None,
    }


@dataclass(frozen=True)
class Deployed:
    """What one environmentd StatefulSet's pod template runs."""

    image: str
    force: str | None
    args: tuple[str, ...]
    resources: dict[str, dict[str, str]] | None


def spec_mismatches(cr: dict[str, Any], deployed: Deployed) -> dict[str, Any]:
    """Rendered fields where `deployed` does not run the CR's spec, keyed by field.

    Extra args are matched as a set against the container args, which also hold
    orchestratord's own flags: every spec arg must be present, and every
    `--unsafe` arg present must come from the spec. orchestratord passes no
    `--unsafe` flag of its own, and every menu entry is one. Resources are
    compared only when the spec sets them: otherwise orchestratord renders its
    configured default, which the workload does not know.
    """
    spec = cr.get("spec") or {}
    out: dict[str, Any] = {}
    image = spec.get("environmentdImageRef")
    if deployed.image != image:
        out["image"] = {"spec": image, "statefulset": deployed.image}
    want_args = list(extra_args(spec) or ())
    if not all(a in deployed.args for a in want_args) or not all(
        a in want_args for a in deployed.args if a.startswith("--unsafe")
    ):
        out["extra_args"] = {
            "spec": want_args,
            "statefulset_unsafe": [
                a for a in deployed.args if a.startswith("--unsafe")
            ],
        }
    force = expected_force(cr)
    if deployed.force != force:
        out["force"] = {"spec": force, "statefulset": deployed.force}
    resources = spec.get("environmentdResourceRequirements")
    if resources and canonical_resources(resources) != canonical_resources(
        deployed.resources
    ):
        out["resources"] = {"spec": resources, "statefulset": deployed.resources}
    return out


def up_to_date(cr: dict[str, Any]) -> dict[str, Any]:
    for condition in (cr.get("status") or {}).get("conditions") or []:
        if condition.get("type") == "UpToDate":
            return condition
    return {}


@dataclass(frozen=True)
class StatusCheck:
    """A status claim about the active generation, checked against its StatefulSet.

    `applied`: `UpToDate=True/Applied` claims the active StatefulSet runs the
    spec and the completed request and image are the spec's. Violated by any
    mismatch.

    `waiting`: `UpToDate=False/WaitingForApproval` claims the spec has changes
    the active StatefulSet does not run. Violated when there are none.
    """

    kind: str
    key: tuple[Any, ...]
    """Identifies the claim: two evaluations with equal keys judge the same
    status about the same spec generation and active generation."""
    mismatches: dict[str, Any]
    empty_resources_hash: bool

    @property
    def violated(self) -> bool:
        if self.kind == "applied":
            return bool(self.mismatches)
        return not self.mismatches


def status_check(cr: dict[str, Any], deployed: Deployed) -> StatusCheck | None:
    """The claim `cr`'s status makes about `deployed`, the StatefulSet of its
    `activeGeneration`, or None if it makes none.

    Only a condition written by a pass that saw the current spec
    (`observedGeneration == metadata.generation`) is judged. orchestratord's
    `update_status` replaces the status with the `resourceVersion` the pass
    read, so a pass that read an older spec cannot write it afterwards.
    """
    condition = up_to_date(cr)
    generation = (cr.get("metadata") or {}).get("generation")
    if generation is None or condition.get("observedGeneration") != generation:
        return None
    status = cr.get("status") or {}
    spec = cr.get("spec") or {}
    key_tail = (
        status.get("activeGeneration"),
        generation,
        status.get("lastCompletedRolloutRequest"),
    )
    empty_hash = status.get("resourcesHash") == ""
    if condition.get("status") == "True" and condition.get("reason") == "Applied":
        mismatches = spec_mismatches(cr, deployed)
        request = spec.get("requestRollout")
        if status.get("lastCompletedRolloutRequest") != request:
            mismatches["last_completed_request"] = {
                "spec": request,
                "status": status.get("lastCompletedRolloutRequest"),
            }
        image = status.get("lastCompletedRolloutEnvironmentdImageRef")
        if image != deployed.image:
            mismatches["last_completed_image"] = {
                "status": image,
                "statefulset": deployed.image,
            }
        return StatusCheck("applied", ("applied", *key_tail), mismatches, empty_hash)
    if (
        condition.get("status") == "False"
        and condition.get("reason") == "WaitingForApproval"
    ):
        return StatusCheck(
            "waiting",
            ("waiting", *key_tail),
            spec_mismatches(cr, deployed),
            empty_hash,
        )
    return None


def claim_cause(
    check: StatusCheck, promoting_request_mode: str | None, promoted_across: bool
) -> str:
    """Which known condition, if any, explains a violation of `check`.

    `promoting_request_mode` is the request mode of a patch that changed a
    rendered field while the CR was `Promoting` (CLO-339), as the caller saw
    it, or None. `promoted_across` says the observer saw the active generation
    promoted across such a change. An empty `resourcesHash` is what a revert
    to the active spec after `Applying` leaves behind: `Applying` clears the
    hash and the revert completes nothing that would restore it.
    """
    if check.kind == "applied":
        if promoting_request_mode == "keep":
            return "applied_promoting_same_request"
        if promoting_request_mode is not None:
            return "applied_promoting_new_request"
        if promoted_across:
            return "applied_promoted_across"
        return "applied"
    if promoting_request_mode is not None or promoted_across:
        return "waiting_promoted_across"
    if check.empty_resources_hash:
        return "waiting_empty_hash"
    return "waiting"


@dataclass(frozen=True)
class Cell:
    field: str
    phase: str
    request_mode: str
    kill: str | None
    """`candidate`, `orchestratord`, or None."""

    def key(self) -> str:
        return f"{self.field}@{self.phase}"


def request_modes(field: str) -> tuple[str, ...]:
    if field == "request_rollout":
        return ("fresh",)
    if field == "revert":
        return REQUEST_MODES
    return ("keep", "fresh")


def kill_targets(phase: str) -> tuple[str | None, ...]:
    """A candidate exists only while a rollout is in progress."""
    if phase in IN_PROGRESS_PHASES:
        return (None, None, "candidate", "orchestratord")
    return (None, None, "orchestratord")


def all_cells() -> list[tuple[str, str]]:
    return [(f, p) for f in FIELDS for p in PHASES]


def choose_cell(rnd: random.Random) -> Cell:
    field = rnd.choice(FIELDS)
    phase = rnd.choice(PHASES)
    return Cell(
        field, phase, rnd.choice(request_modes(field)), rnd.choice(kill_targets(phase))
    )


@dataclass(frozen=True)
class Images:
    initial: str | None
    """The image the environment started on."""
    target: str | None
    """The image to upgrade to, or None if the timeline does not upgrade."""
    last_completed: str | None
    """`status.lastCompletedRolloutEnvironmentdImageRef`."""


def image_options(images: Images, spec_image: str | None, phase: str) -> list[str]:
    """Images the driver may set: the current one, the upgrade target while
    the spec does not name it, and the initial release while the upgrade has
    not completed.

    Going back after the upgrade completed is a downgrade, which orchestratord
    refuses when both tags parse and which the older release cannot boot over
    an upgraded catalog when they do not. Going back during `Promoting` is
    excluded too: the promotion may complete with the newer release while the
    status records the older one (CLO-339), and the next rollout would then
    deploy the older release over the upgraded catalog.
    """
    options = [spec_image] if spec_image else []
    if images.target is None:
        return options
    if spec_image != images.target:
        options.append(images.target)
    elif (
        images.initial is not None
        and images.last_completed != images.target
        and phase != "Promoting"
    ):
        options.append(images.initial)
    return options


def _different(rnd: random.Random, menu: list[Any], current: Any) -> Any:
    others = [m for m in menu if m != current]
    return rnd.choice(others or menu)


def cell_changes(
    cell: Cell,
    rnd: random.Random,
    spec_now: dict[str, Any],
    prior_spec: dict[str, Any],
    images: Images,
    new_uuid: Callable[[], str],
) -> dict[str, Any]:
    """The merge patch a cell sends, given the spec when it acts and the spec
    before its setup patch."""
    changes: dict[str, Any] = {}
    if cell.field == "image":
        changes["environmentdImageRef"] = rnd.choice(
            image_options(images, spec_now.get("environmentdImageRef"), cell.phase)
            or [spec_now.get("environmentdImageRef")]
        )
    elif cell.field == "extra_args":
        chosen = _different(rnd, list(EXTRA_ARGS_MENU), extra_args(spec_now))
        changes["environmentdExtraArgs"] = list(chosen) if chosen else None
    elif cell.field == "force_rollout":
        changes["forceRollout"] = new_uuid()
    elif cell.field == "rollout_strategy":
        changes["rolloutStrategy"] = _different(
            rnd,
            list(STRATEGIES),
            spec_now.get("rolloutStrategy", DEFAULT_ROLLOUT_STRATEGY),
        )
    elif cell.field == "resources":
        current = canonical_resources(spec_now.get("environmentdResourceRequirements"))
        menu = [m for m in RESOURCES_MENU if canonical_resources(m) != current]
        changes["environmentdResourceRequirements"] = rnd.choice(
            menu or list(RESOURCES_MENU)
        )
    elif cell.field == "revert":
        for key in (*DEPLOYED_SPEC_KEYS, "rolloutStrategy"):
            changes[key] = prior_spec.get(key)
    if cell.request_mode == "fresh":
        changes["requestRollout"] = new_uuid()
    elif cell.request_mode == "restore":
        changes["requestRollout"] = prior_spec.get("requestRollout") or new_uuid()
    return changes


def changes_deployed_fields(
    spec_before: dict[str, Any], changes: dict[str, Any]
) -> bool:
    """Whether applying `changes` to `spec_before` changes a rendered field.
    The force annotation lives on the CR metadata, which the driver never
    patches."""
    after = merge_patch(spec_before, changes)
    return any(spec_before.get(k) != after.get(k) for k in DEPLOYED_SPEC_KEYS)
