# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import itertools
import random
from decimal import Decimal
from typing import Any

import pytest

from materialize.antithesis.spec_model import (
    EXTRA_ARGS_MENU,
    FIELDS,
    NIL_UUID,
    PHASES,
    RESOURCES_MENU,
    Cell,
    Deployed,
    Images,
    all_cells,
    canonical_resources,
    cell_changes,
    changes_deployed_fields,
    choose_cell,
    claim_cause,
    deployed_spec,
    expected_force,
    image_options,
    merge_patch,
    parse_quantity,
    spec_mismatches,
    status_check,
)

OLD = "materialize/environmentd:v26.1.3"
NEW = "materialize/environmentd:under-test"
FORCE = "11111111-1111-1111-1111-111111111111"
REQUEST = "22222222-2222-2222-2222-222222222222"
MIGRATION = ["--unsafe-mode", "--unsafe-force-builtin-schema-migration=evolution"]


def cr(
    spec: dict[str, Any] | None = None,
    reason: str = "Applied",
    condition_status: str = "True",
    generation: int = 7,
    observed: int | None = 7,
    status: dict[str, Any] | None = None,
    annotations: dict[str, str] | None = None,
) -> dict[str, Any]:
    base_spec = {
        "environmentdImageRef": NEW,
        "forceRollout": FORCE,
        "requestRollout": REQUEST,
        "environmentdResourceRequirements": RESOURCES_MENU[0],
    }
    return {
        "metadata": {"generation": generation, "annotations": annotations or {}},
        "spec": {**base_spec, **(spec or {})},
        "status": {
            "activeGeneration": 3,
            "lastCompletedRolloutRequest": REQUEST,
            "lastCompletedRolloutEnvironmentdImageRef": NEW,
            "resourcesHash": "abc",
            "conditions": [
                {
                    "type": "UpToDate",
                    "status": condition_status,
                    "reason": reason,
                    "observedGeneration": observed,
                }
            ],
            **(status or {}),
        },
    }


def deployed(**overrides: Any) -> Deployed:
    fields: dict[str, Any] = {
        "image": NEW,
        "force": FORCE,
        "args": ("--environment-id=x", "--listeners-config-path=/l.json"),
        "resources": {
            "requests": {"cpu": "50m", "memory": "512Mi"},
            "limits": {"memory": "2Gi"},
        },
    }
    fields.update(overrides)
    return Deployed(**fields)


def test_merge_patch_removes_merges_and_replaces() -> None:
    target = {"a": 1, "b": {"x": 1, "y": 2}, "c": [1, 2]}
    patch = {"a": None, "b": {"y": None, "z": 3}, "c": [3]}
    assert merge_patch(target, patch) == {"b": {"x": 1, "z": 3}, "c": [3]}
    assert target == {"a": 1, "b": {"x": 1, "y": 2}, "c": [1, 2]}, "not mutated"
    assert merge_patch({"a": 1}, {"a": {"b": 2}}) == {"a": {"b": 2}}


def test_parse_quantity_by_value() -> None:
    assert parse_quantity("512Mi") == 512 * 2**20
    assert parse_quantity("0.5") == parse_quantity("500m")
    assert parse_quantity("1e3") == parse_quantity("1k") == Decimal(1000)
    assert parse_quantity(2) == 2
    with pytest.raises(ValueError):
        parse_quantity("lots")


def test_canonical_resources_ignores_form_and_empty_maps() -> None:
    a = {"requests": {"cpu": "0.1", "memory": "1Gi"}, "limits": {}}
    b = {"requests": {"cpu": "100m", "memory": "1024Mi"}}
    assert canonical_resources(a) == canonical_resources(b)
    assert canonical_resources(None) == {}
    assert canonical_resources(RESOURCES_MENU[0]) != canonical_resources(
        RESOURCES_MENU[1]
    )


def test_expected_force() -> None:
    assert expected_force(cr()) == FORCE
    assert expected_force(cr(spec={"forceRollout": None})) == NIL_UUID
    annotated = cr(annotations={"materialize.cloud/force-rollout": "node-upgrade"})
    assert expected_force(annotated) == f"{FORCE}/node-upgrade"


def test_spec_mismatches_matching() -> None:
    assert spec_mismatches(cr(), deployed()) == {}


def test_spec_mismatches_each_field() -> None:
    assert set(spec_mismatches(cr(spec={"environmentdImageRef": OLD}), deployed())) == {
        "image"
    }
    assert set(spec_mismatches(cr(), deployed(force="other"))) == {"force"}
    with_args = cr(spec={"environmentdExtraArgs": MIGRATION})
    assert set(spec_mismatches(with_args, deployed())) == {"extra_args"}
    assert spec_mismatches(with_args, deployed(args=("--x", *MIGRATION))) == {}
    stray = deployed(args=("--x", "--unsafe-mode"))
    assert set(spec_mismatches(cr(), stray)) == {"extra_args"}, "unsafe arg not in spec"
    more = cr(spec={"environmentdResourceRequirements": RESOURCES_MENU[1]})
    assert set(spec_mismatches(more, deployed())) == {"resources"}


def test_spec_mismatches_resources_by_value_and_only_when_set() -> None:
    same = deployed(
        resources={
            "requests": {"cpu": "0.05", "memory": "512Mi"},
            "limits": {"memory": "2048Mi"},
        }
    )
    assert spec_mismatches(cr(), same) == {}
    unset = cr(spec={"environmentdResourceRequirements": None})
    assert spec_mismatches(unset, deployed(resources=None)) == {}
    assert spec_mismatches(unset, deployed()) == {}, "default is not known"


def test_status_check_applied() -> None:
    check = status_check(cr(), deployed())
    assert check is not None and check.kind == "applied" and not check.violated
    drift = status_check(cr(spec={"environmentdImageRef": OLD}), deployed())
    assert drift is not None and drift.violated
    assert set(drift.mismatches) == {"image"}


def test_status_check_applied_completed_request_and_image() -> None:
    other = cr(
        status={"lastCompletedRolloutRequest": "33333333-0000-0000-0000-000000000000"}
    )
    check = status_check(other, deployed())
    assert check is not None and set(check.mismatches) == {"last_completed_request"}
    stale = cr(status={"lastCompletedRolloutEnvironmentdImageRef": OLD})
    check = status_check(stale, deployed())
    assert check is not None and set(check.mismatches) == {"last_completed_image"}


def test_status_check_skips_conditions_from_older_spec() -> None:
    assert status_check(cr(generation=8, observed=7), deployed(image=OLD)) is None
    assert (
        status_check(cr(reason="Applying", condition_status="Unknown"), deployed())
        is None
    )


def test_status_check_waiting() -> None:
    waiting = cr(reason="WaitingForApproval", condition_status="False")
    check = status_check(waiting, deployed())
    assert check is not None and check.kind == "waiting"
    assert check.violated, "no change is pending"
    assert not check.empty_resources_hash
    pending = cr(
        spec={"forceRollout": "44444444-0000-0000-0000-000000000000"},
        reason="WaitingForApproval",
        condition_status="False",
        status={"resourcesHash": ""},
    )
    check = status_check(pending, deployed())
    assert check is not None and not check.violated and check.empty_resources_hash


def test_status_check_key_identifies_the_claim() -> None:
    a = status_check(cr(), deployed())
    b = status_check(cr(), deployed(image=OLD))
    c = status_check(cr(generation=9, observed=9), deployed())
    assert a is not None and b is not None and c is not None
    assert a.key == b.key, "same claim regardless of what the StatefulSet runs"
    assert a.key != c.key


def test_claim_cause() -> None:
    applied = status_check(cr(), deployed())
    waiting = status_check(
        cr(reason="WaitingForApproval", condition_status="False"), deployed()
    )
    empty = status_check(
        cr(
            reason="WaitingForApproval",
            condition_status="False",
            status={"resourcesHash": ""},
        ),
        deployed(),
    )
    assert applied is not None and waiting is not None and empty is not None
    assert claim_cause(applied, None, False) == "applied"
    assert claim_cause(applied, "keep", False) == "applied_promoting_same_request"
    assert claim_cause(applied, "fresh", True) == "applied_promoting_new_request"
    assert claim_cause(applied, "restore", False) == "applied_promoting_new_request"
    assert claim_cause(applied, None, True) == "applied_promoted_across"
    assert claim_cause(waiting, None, False) == "waiting"
    assert claim_cause(empty, None, False) == "waiting_empty_hash"
    assert claim_cause(empty, None, True) == "waiting_promoted_across"


def test_choose_cell_draws_only_valid_cells_and_covers_all() -> None:
    rnd = random.Random(1)
    seen = set()
    for _ in range(3000):
        cell = choose_cell(rnd)
        assert cell.field in FIELDS and cell.phase in PHASES
        if cell.field == "request_rollout":
            assert cell.request_mode == "fresh"
        if cell.field != "revert":
            assert cell.request_mode != "restore"
        if cell.phase in ("Applied", "WaitingForApproval"):
            assert cell.kill != "candidate"
        seen.add((cell.field, cell.phase))
    assert seen == set(all_cells())


def uuids() -> Any:
    counter = itertools.count()
    return lambda: f"uuid-{next(counter)}"


IMAGES = Images(initial=OLD, target=None, last_completed=OLD)


def changes(
    cell: Cell,
    spec: dict[str, Any],
    prior: dict[str, Any] | None = None,
    images: Images = IMAGES,
) -> dict[str, Any]:
    return cell_changes(cell, random.Random(3), spec, prior or spec, images, uuids())


def test_cell_changes_request_modes() -> None:
    spec = cr()["spec"]
    prior = {**spec, "requestRollout": "prior-request"}
    keep = changes(Cell("force_rollout", "Applying", "keep", None), spec, prior)
    assert keep == {"forceRollout": "uuid-0"}
    fresh = changes(Cell("force_rollout", "Applying", "fresh", None), spec, prior)
    assert fresh == {"forceRollout": "uuid-0", "requestRollout": "uuid-1"}
    only_request = changes(Cell("request_rollout", "Applied", "fresh", None), spec)
    assert only_request == {"requestRollout": "uuid-0"}
    restore = changes(Cell("revert", "Applying", "restore", None), spec, prior)
    assert restore["requestRollout"] == "prior-request"


def test_cell_changes_pick_a_different_value() -> None:
    spec = cr(spec={"environmentdExtraArgs": MIGRATION})["spec"]
    for seed in range(20):
        rnd = random.Random(seed)
        args = cell_changes(
            Cell("extra_args", "Promoting", "keep", None),
            rnd,
            spec,
            spec,
            IMAGES,
            uuids(),
        )
        assert args["environmentdExtraArgs"] != MIGRATION
        assert tuple(args["environmentdExtraArgs"] or ()) in [
            tuple(m or ()) for m in EXTRA_ARGS_MENU
        ]
        res = cell_changes(
            Cell("resources", "Applied", "keep", None), rnd, spec, spec, IMAGES, uuids()
        )
        assert canonical_resources(
            res["environmentdResourceRequirements"]
        ) != canonical_resources(RESOURCES_MENU[0])
        assert res["environmentdResourceRequirements"]["limits"] == {"memory": "2Gi"}
    strategy = changes(Cell("rollout_strategy", "Applying", "keep", None), spec)
    assert strategy == {"rolloutStrategy": "ManuallyPromote"}


def test_cell_changes_revert_restores_prior_spec() -> None:
    prior = cr()["spec"]
    now = {
        **prior,
        "environmentdExtraArgs": MIGRATION,
        "forceRollout": "later",
        "rolloutStrategy": "ManuallyPromote",
    }
    patch = changes(Cell("revert", "Promoting", "keep", None), now, prior)
    assert merge_patch(now, patch) == prior
    assert not changes_deployed_fields(prior, patch)
    assert changes_deployed_fields(now, patch)


def test_image_options_respect_the_upgrade_window() -> None:
    assert image_options(Images(OLD, None, OLD), OLD, "Applying") == [OLD]
    pending = Images(OLD, NEW, OLD)
    assert image_options(pending, OLD, "Applying") == [OLD, NEW]
    assert image_options(pending, NEW, "Applying") == [NEW, OLD], "cancel the upgrade"
    assert image_options(pending, NEW, "Promoting") == [
        NEW
    ], "never back while promoting"
    done = Images(OLD, NEW, NEW)
    assert image_options(done, NEW, "Applying") == [NEW], "no downgrade"


def test_deployed_spec_compares_by_value() -> None:
    a = cr()
    b = cr(
        spec={
            "environmentdResourceRequirements": {
                "requests": {"cpu": "0.05", "memory": "512Mi"},
                "limits": {"memory": "2Gi"},
            }
        }
    )
    assert deployed_spec(a) == deployed_spec(b)
    assert deployed_spec(a) != deployed_spec(
        cr(spec={"environmentdExtraArgs": MIGRATION})
    )
    assert deployed_spec(cr(spec={"environmentdExtraArgs": []})) == deployed_spec(a)
