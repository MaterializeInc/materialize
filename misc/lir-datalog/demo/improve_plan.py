#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Rewrites a cross join filtered by an OR of equalities into a union of equi-joins.

    python3 misc/lir-datalog/demo/improve_plan.py < plan.json > improved.json

Reads an LIR plan envelope, as `EXPLAIN PHYSICAL PLAN AS LIR JSON` prints it, and
finds the one linear join of two inputs that joins on the empty key and filters the
pairs with `a1 = b1 OR a2 = b2 OR ...`, where each `ai` is a column of the left input
and each `bi` a column of the right input. The optimizer plans a correlated subquery
whose correlation is such an OR in this way. The rewrite replaces the cross join with
the union of one equi-join per equality.

The union can produce a pair once per matching equality, where the cross join
produces it once. The rewrite is therefore only correct where the join's output feeds
a distinct reduction, as it does for `EXISTS`, and the script checks that it does.

Both inputs are bound with `Let` around the object's plan, so each equi-join reads a
shared arrangement instead of rebuilding its input. `--broken` leaves out every
arrangement of the left input but the first, so the second equi-join looks up an
arrangement that does not exist, which the validator rejects.

A summary of the rewrite goes to stderr.
"""

import argparse
import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datalog2lir import permutation_for_arrangement


class RewriteError(Exception):
    pass


def col(i: int) -> dict:
    return {"Column": [i, None]}


def arrangement(key: list[int], arity: int) -> list:
    """An arrangement of `key` in the layout the validator requires."""
    permutation, thinning = permutation_for_arrangement(key, arity)
    return [[col(c) for c in key], permutation, thinning]


def mfp(input_arity: int, projection: list[int]) -> dict:
    return {
        "mfp": {
            "expressions": [],
            "predicates": [],
            "projection": projection,
            "input_arity": input_arity,
        }
    }


def nodes(expr: dict):
    """Yields every relation expression in `expr`, parents before children."""
    stack = [expr]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            if "lir_id" in value and "node" in value:
                yield value
            stack.extend(value.values())
        elif isinstance(value, list):
            stack.extend(value)


def is_cross_join(expr: dict) -> bool:
    linear = expr["node"].get("Join", {}).get("plan", {}).get("Linear")
    return (
        linear is not None
        and linear["source_key"] == []
        and all(s["lookup_key"] == [] for s in linear["stage_plans"])
    )


def column(expr: dict) -> tuple[int, str]:
    if "Column" not in expr:
        raise RewriteError(f"expected a column reference, found {expr}")
    index, name = expr["Column"]
    return index, name or f"#{index}"


class Rewriter:
    def __init__(self, envelope: dict):
        self.envelope = envelope
        all_nodes = [n for o in envelope["objects"] for n in nodes(o["plan"])]
        self.next_lir_id = max(n["lir_id"] for n in all_nodes) + 1
        locals_ = [n["node"]["Let"]["id"] for n in all_nodes if "Let" in n["node"]] + [
            i
            for n in all_nodes
            if "LetRec" in n["node"]
            for i in n["node"]["LetRec"]["ids"]
        ]
        self.next_local = max(locals_, default=0) + 1

    def expr(self, node: dict) -> dict:
        lir_id = self.next_lir_id
        self.next_lir_id += 1
        return {"lir_id": lir_id, "node": node}

    def local(self) -> int:
        local = self.next_local
        self.next_local += 1
        return local

    def find_cross_join(self) -> tuple[dict, dict, dict]:
        """Returns the cross join, the distinct reduction it feeds, and its object."""
        cross_joins = [
            (expr, obj)
            for obj in self.envelope["objects"]
            for expr in nodes(obj["plan"])
            if is_cross_join(expr)
        ]
        if len(cross_joins) != 1:
            raise RewriteError(f"expected one cross join, found {len(cross_joins)}")
        join, obj = cross_joins[0]
        for parent in nodes(obj["plan"]):
            reduce = parent["node"].get("Reduce")
            if reduce is not None and reduce["input"] is join:
                if reduce["plan"] != "Distinct":
                    break
                return join, parent, obj
        raise RewriteError(
            f"the cross join (LIR node {join['lir_id']}) does not feed a distinct"
            " reduction, so a union of equi-joins could change its multiplicities"
        )

    def rewrite(self, broken: bool) -> None:
        join_expr, _, obj = self.find_cross_join()
        join = join_expr["node"]["Join"]
        linear = join["plan"]["Linear"]
        if len(join["inputs"]) != 2 or len(linear["stage_plans"]) != 1:
            raise RewriteError("expected a cross join of exactly two inputs")
        if linear["initial_closure"] or linear["final_closure"]:
            raise RewriteError("expected no closure outside the join stage")
        stage = linear["stage_plans"][0]
        before = stage["closure"]["before"]["mfp"]
        if stage["closure"]["ready_equivalences"] or before["expressions"]:
            raise RewriteError("expected a closure with only a filter and a projection")
        if len(before["predicates"]) != 1:
            raise RewriteError("expected a closure with exactly one predicate")
        disjunction = before["predicates"][0][1]

        left, right = join["inputs"]
        left_arity = len(stage["stream_thinning"])
        right_arity = len(right["node"]["ArrangeBy"]["forms"]["arranged"][0][1])
        if stage["stream_thinning"] != list(range(left_arity)):
            raise RewriteError(
                "expected the cross join to keep the left input's columns"
            )

        # The closure reads the left columns, then the right columns, because both
        # inputs are arranged by the empty key.
        equalities = []
        disjuncts = disjunction.get("CallVariadic", {})
        if disjuncts.get("func") != {"Or": None}:
            raise RewriteError(f"expected an OR of equalities, found {disjunction}")
        for eq in disjuncts["exprs"]:
            call = eq.get("CallBinary", {})
            if call.get("func") != {"Eq": None}:
                raise RewriteError(f"expected an equality, found {eq}")
            (a, a_name), (b, b_name) = sorted(
                [column(call["expr1"]), column(call["expr2"])]
            )
            if not (a < left_arity <= b):
                raise RewriteError(
                    f"{a_name} = {b_name} does not relate the left and the right input"
                )
            equalities.append((a, b - left_arity, a_name, b_name))

        print(
            f"Found a cross join (LIR node {join_expr['lir_id']}) filtered by",
            " OR ".join(f"{a} = {b}" for _, _, a, b in equalities),
            file=sys.stderr,
        )

        left_keys = list(dict.fromkeys(l for l, _, _, _ in equalities))
        right_keys = list(dict.fromkeys(r for _, r, _, _ in equalities))
        if broken:
            left_keys = left_keys[:1]
        left_local, left_value = self.rearrange(left, left_keys, left_arity)
        right_local, right_value = self.rearrange(right, right_keys, right_arity)
        print(
            "Arranging the left input by",
            ", ".join(f"[{eq[2]}]" for eq in equalities if eq[0] in left_keys),
            "and the right input by",
            ", ".join(sorted({f"[{eq[3]}]" for eq in equalities})),
            "instead of both by the empty key",
            file=sys.stderr,
        )

        joins = []
        for l, r, l_name, r_name in equalities:
            joins.append(
                self.equi_join(
                    (left_local, left_value, left_arity, l),
                    (right_local, right_value, right_arity, r),
                    before["projection"],
                )
            )
            print(f"Adding an equi-join on {l_name} = {r_name}", file=sys.stderr)

        # The union takes the cross join's place, and its LIR id.
        join_expr["node"] = {
            "Union": {
                "inputs": joins,
                "consolidate_output": False,
                "temporal_bucketing_strategies": ["Direct"] * len(joins),
            }
        }
        obj["plan"] = self.expr(
            {
                "Let": {
                    "id": left_local,
                    "value": left_value,
                    "body": self.expr(
                        {
                            "Let": {
                                "id": right_local,
                                "value": right_value,
                                "body": obj["plan"],
                            }
                        }
                    ),
                }
            }
        )

    def rearrange(self, input: dict, keys: list[int], arity: int) -> tuple[int, dict]:
        """Binds the input of the `ArrangeBy` `input`, arranged by each of `keys`."""
        arrange = copy.deepcopy(input["node"]["ArrangeBy"])
        arrange["forms"]["arranged"] = [arrangement([k], arity) for k in keys]
        return self.local(), self.expr({"ArrangeBy": arrange})

    def get(self, local: int, value: dict) -> dict:
        return self.expr(
            {
                "Get": {
                    "id": {"Local": local},
                    "keys": value["node"]["ArrangeBy"]["forms"],
                    "plan": "PassArrangements",
                }
            }
        )

    def equi_join(self, left, right, projection: list[int]) -> dict:
        """A linear join of `left` and `right` on one column of each.

        Each side is `(local, value, arity, key column)`, and `projection` lists the
        output columns as indexes into the left columns followed by the right columns.
        """
        left_local, left_value, left_arity, l = left
        right_local, right_value, right_arity, r = right
        left_rest = [c for c in range(left_arity) if c != l]
        right_rest = [c for c in range(right_arity) if c != r]

        # A join stage produces the key, then the stream's other columns, then the
        # lookup's other columns.
        def position(c: int) -> int:
            if c < left_arity:
                return 0 if c == l else 1 + left_rest.index(c)
            c -= left_arity
            return 0 if c == r else 1 + len(left_rest) + right_rest.index(c)

        closure = {
            "ready_equivalences": [],
            "before": mfp(
                1 + len(left_rest) + len(right_rest), [position(c) for c in projection]
            ),
        }
        return self.expr(
            {
                "Join": {
                    "inputs": [
                        self.get(left_local, left_value),
                        self.get(right_local, right_value),
                    ],
                    "plan": {
                        "Linear": {
                            "source_relation": 0,
                            "source_key": [col(l)],
                            "initial_closure": None,
                            "stage_plans": [
                                {
                                    "lookup_relation": 1,
                                    "stream_key": [col(l)],
                                    "stream_thinning": left_rest,
                                    "lookup_key": [col(r)],
                                    "closure": closure,
                                }
                            ],
                            "final_closure": None,
                        }
                    },
                }
            }
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "--broken",
        action="store_true",
        help="leave out an arrangement that the second equi-join needs",
    )
    args = parser.parse_args()
    envelope = json.load(sys.stdin)
    try:
        Rewriter(envelope).rewrite(args.broken)
    except RewriteError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(envelope, separators=(",", ":")))


if __name__ == "__main__":
    main()
