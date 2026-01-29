#!/usr/bin/python3
#
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# join_of_unions.py — finds and analyzes joins of unions in MIR plans
#
# the jq query of the same name is less useful, since jq can't keep state about Let nodes

import json
import sys
import traceback

total_joins = 0
total_joins_of_unions = 0


def is_union_or_arrangement_of_union(o):
    return isinstance(o, dict) and (
        ("Union" in o) or ("ArrangeBy" in o and "Union" in o["ArrangeBy"]["input"])
    )


def is_known_union(o, known_unions):
    return (
        isinstance(o, dict)
        and ("Get" in o)
        and ("Local" in o["Get"]["id"])
        and o["Get"]["id"]["Local"] in known_unions
    )


def go(o, known_unions, info):
    global total_joins, total_joins_of_unions

    if not isinstance(o, dict):
        return

    for k, v in o.items():
        if k == "Join":
            total_joins += 1
            num_unions = 0
            for input in v["inputs"]:
                if is_union_or_arrangement_of_union(input) or is_known_union(
                    input, known_unions
                ):
                    num_unions += 1
            if num_unions > 1:
                total_joins_of_unions += 1
                print(f"{info}: join of {num_unions} unions")
        elif k == "Let":
            var = v["id"]
            val = v["value"]

            if is_union_or_arrangement_of_union(val):
                known_unions.add(var)
        # no LetRec, yolo

        go(v, known_unions, info)


def look_in(file):
    o = json.load(open(file))
    assert isinstance(o, dict)
    assert "plans" in o

    for plan in o["plans"]:
        try:
            go(plan["plan"], set(), f'{file} plan {plan["id"]}')
        except Exception:
            print(f'FAILURE IN {file} plan {plan["id"]}')
            traceback.print_exc()


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        look_in(arg)

    print(f"observed {total_joins_of_unions} joins of unions out of {total_joins}")
