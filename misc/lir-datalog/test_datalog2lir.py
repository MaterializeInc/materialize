# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests for the toy Datalog compiler and the generated LIR types.

Run with `python3 -m unittest misc/lir-datalog/test_datalog2lir.py`.
"""

import ast
import json
import re
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(HERE))

import datalog2lir
import gen_lir_types
import lir_types as lt

TC = (HERE / "examples" / "tc.dl").read_text()
EDGES = {"edges": "materialize.public.edges"}


def walk(value):
    """Yields every JSON value nested in `value`, including itself."""
    stack = [value]
    while stack:
        v = stack.pop()
        yield v
        if isinstance(v, dict):
            stack.extend(v.values())
        elif isinstance(v, list):
            stack.extend(v)


def nodes(envelope, kind):
    """Yields the payloads of all LIR nodes of `kind` in a JSON envelope."""
    for v in walk(envelope):
        if isinstance(v, dict) and set(v) == {"lir_id", "node"}:
            if kind in v["node"]:
                yield v["node"][kind]


def slt_envelopes():
    """Yields (origin, JSON) for the envelopes in the LIR plan SLT files.

    These are the `EXPLAIN ... AS LIR JSON` outputs, which the optimizer wrote, and the
    plans of `FROM PLAN` statements that are JSON envelopes of the current version. Some
    of the latter are invalid plans, but all are well-formed.
    """
    for name in ["lir_plans.slt", "lir_plans_explain.slt", "lir_plans_datalog.slt"]:
        text = (REPO / "test" / "sqllogictest" / name).read_text()
        for m in re.finditer(
            r"AS LIR JSON FOR [^\n]*\n----\n(\{\n.*?\n\})\n", text, re.S
        ):
            yield f"{name}: EXPLAIN at {m.start()}", json.loads(m.group(1))
        for m in re.finditer(r"FROM PLAN '(.*?)'$", text, re.M):
            try:
                envelope = json.loads(m.group(1))
            except json.JSONDecodeError:
                continue
            if envelope.get("lir_version") == 1:
                yield f"{name}: FROM PLAN at {m.start()}", envelope


class ParseTest(unittest.TestCase):
    def test_parse_rules(self):
        rules = datalog2lir.parse(TC)
        self.assertEqual(
            rules,
            [
                datalog2lir.Rule(
                    datalog2lir.Atom("reachable", ("x", "y")),
                    (datalog2lir.Atom("edges", ("x", "y")),),
                ),
                datalog2lir.Rule(
                    datalog2lir.Atom("reachable", ("x", "z")),
                    (
                        datalog2lir.Atom("reachable", ("x", "y")),
                        datalog2lir.Atom("edges", ("y", "z")),
                    ),
                ),
            ],
        )

    def test_parse_rejects_facts_and_unbound_head_variables(self):
        with self.assertRaisesRegex(datalog2lir.DatalogError, "body"):
            datalog2lir.parse("p(x) :- .")
        with self.assertRaisesRegex(datalog2lir.DatalogError, "not bound"):
            datalog2lir.compile_program(
                datalog2lir.parse("p(x, z) :- e(x, y)."), "p", {"e": "e"}
            )


class CompileTest(unittest.TestCase):
    def compile(self, program, output, imports=EDGES, limit=10000):
        envelope = datalog2lir.compile_program(
            datalog2lir.parse(program), output, imports, limit=limit
        )
        self.assertIsInstance(envelope, lt.LirPlanEnvelope)
        return lt.to_json(envelope)

    def test_nonrecursive_join_emits_join_with_arrangements(self):
        env = self.compile(
            "two_hop(x, z) :- edges(x, y), edges(y, z).", "two_hop", EDGES
        )
        (join,) = nodes(env, "Join")
        linear = join["plan"]["Linear"]
        self.assertEqual(len(join["inputs"]), 2)
        # Every input is an `ArrangeBy` that forms the arrangement its stage reads.
        keys = []
        for inp in join["inputs"]:
            arrange = inp["node"]["ArrangeBy"]
            (arranged,) = arrange["forms"]["arranged"]
            keys.append(arranged)
        self.assertEqual(keys[0], [[{"Column": [1, None]}], [1, 0], [0]])
        self.assertEqual(keys[1], [[{"Column": [0, None]}], [0, 1], [1]])
        self.assertEqual(linear["source_relation"], 0)
        self.assertEqual(linear["source_key"], [{"Column": [1, None]}])
        (stage,) = linear["stage_plans"]
        self.assertEqual(stage["lookup_relation"], 1)
        self.assertEqual(stage["stream_key"], [{"Column": [1, None]}])
        self.assertEqual(stage["stream_thinning"], [0])
        self.assertEqual(stage["lookup_key"], [{"Column": [0, None]}])
        # The joined row is (y, x, z), the closure keeps (x, z).
        self.assertEqual(stage["closure"]["before"]["mfp"]["projection"], [1, 2])
        self.assertEqual(stage["closure"]["before"]["mfp"]["input_arity"], 3)
        # Non-recursive relations have set semantics too.
        self.assertEqual(len(list(nodes(env, "Reduce"))), 1)
        self.assertEqual(list(nodes(env, "LetRec")), [])

    def test_three_way_join_threads_the_stream(self):
        env = self.compile(
            "p(a, d) :- edges(a, b), edges(b, c), edges(c, d).", "p", EDGES
        )
        (join,) = nodes(env, "Join")
        stages = join["plan"]["Linear"]["stage_plans"]
        self.assertEqual([s["lookup_relation"] for s in stages], [1, 2])
        # After the first stage the stream is (a, c), keyed by c for the second stage.
        self.assertEqual(stages[0]["closure"]["before"]["mfp"]["projection"], [1, 2])
        self.assertEqual(stages[1]["stream_key"], [{"Column": [1, None]}])
        self.assertEqual(stages[1]["stream_thinning"], [0])

    def test_repeated_variable_filters(self):
        env = self.compile("loop(x) :- edges(x, x).", "loop", EDGES)
        (get,) = [g for g in nodes(env, "Get") if "Global" in g["id"]]
        mfp = get["plan"]["Collection"]["mfp"]["mfp"]
        ((_, pred),) = mfp["predicates"]
        self.assertEqual(pred["CallBinary"]["func"], {"Eq": None})
        self.assertEqual(mfp["projection"], [0])

    def test_recursive_emits_letrec_with_limit(self):
        env = self.compile(TC, "reachable", EDGES, limit=77)
        (letrec,) = nodes(env, "LetRec")
        self.assertEqual(letrec["ids"], [0])
        self.assertEqual(letrec["limits"], [{"max_iters": 77, "return_at_limit": True}])
        (value,) = letrec["values"]
        # Set semantics: the binding is a distinct reduction, made available as a
        # collection again.
        arrange = value["node"]["ArrangeBy"]
        self.assertEqual(arrange["forms"], {"raw": True, "arranged": []})
        reduce = arrange["input"]["node"]["Reduce"]
        self.assertEqual(reduce["plan"], "Distinct")
        self.assertIn("Union", reduce["input"]["node"])
        self.assertEqual(
            letrec["body"]["node"]["Get"]["id"], {"Local": 0}, "body reads the output"
        )

    def test_mutual_recursion_shares_one_letrec(self):
        env = self.compile(
            "even(x, y) :- edges(x, y).\n"
            "odd(x, z) :- even(x, y), edges(y, z).\n"
            "even(x, z) :- odd(x, y), edges(y, z).\n",
            "odd",
            EDGES,
        )
        (letrec,) = nodes(env, "LetRec")
        self.assertEqual(letrec["ids"], [0, 1])
        self.assertEqual(len(letrec["limits"]), 2)

    def test_output_is_valid_json_envelope(self):
        text = datalog2lir.main(
            ["prog.dl", "--output", "reachable", "--import", 'edges=db.sch."Edges"'],
            read=lambda _path: TC,
        )
        env = json.loads(text)
        self.assertEqual(env["lir_version"], 1)
        self.assertEqual(
            env["imports"],
            [
                {
                    "id": {"Transient": 1},
                    "source": {"Name": 'db.sch."Edges"'},
                    "persist": True,
                    "indexes": [],
                }
            ],
        )
        self.assertEqual(env["output"], {"Transient": 2})
        self.assertEqual([o["id"] for o in env["objects"]], [{"Transient": 2}])
        lir_ids = [
            v["lir_id"] for v in walk(env) if isinstance(v, dict) and "lir_id" in v
        ]
        self.assertEqual(len(lir_ids), len(set(lir_ids)), "lir ids are unique")
        # `$row` appears only in the value of a literal.
        literal_rows = []
        for v in walk(env):
            if isinstance(v, dict) and "Literal" in v:
                literal_rows.append(id(v["Literal"][0]["Ok"]))
        for v in walk(env):
            if isinstance(v, dict) and "$row" in v:
                self.assertIn(id(v), literal_rows)
        # The output decodes into the generated types and re-encodes unchanged.
        self.assertEqual(lt.to_json(lt.from_json(lt.LirPlanEnvelope, env)), env)

    def test_unknown_relation_is_an_error(self):
        with self.assertRaisesRegex(datalog2lir.DatalogError, "nodes"):
            datalog2lir.compile_program(
                datalog2lir.parse("p(x) :- nodes(x)."), "p", EDGES
            )


class GeneratedTypesTest(unittest.TestCase):
    def test_generated_types_are_fresh(self):
        registry = json.loads(gen_lir_types.REGISTRY.read_text())
        fresh = gen_lir_types.generate(registry)
        checked_in = (HERE / "lir_types.py").read_text()
        # Compare syntax trees, so `bin/fmt` reformatting the checked-in file does not
        # count as staleness.
        self.assertEqual(
            ast.dump(ast.parse(fresh)),
            ast.dump(ast.parse(checked_in)),
            "lir_types.py is stale, run `python3 misc/lir-datalog/gen_lir_types.py`",
        )

    def test_every_registry_type_has_a_class(self):
        registry = json.loads(gen_lir_types.REGISTRY.read_text())
        for name in registry:
            self.assertIn(name, lt.TYPES, name)

    def test_roundtrip_extracted_envelopes(self):
        count = 0
        for origin, envelope in slt_envelopes():
            with self.subTest(origin):
                decoded = lt.from_json(lt.LirPlanEnvelope, envelope)
                self.assertEqual(lt.to_json(decoded), envelope)
                count += 1
        self.assertGreater(count, 5)

    def test_encoder_rejects_ill_typed_values(self):
        with self.assertRaises(TypeError):
            lt.to_json(lt.LirId("1"))
        with self.assertRaises(TypeError):
            lt.to_json(lt.GlobalId.Transient(True))
        with self.assertRaises(TypeError):
            lt.to_json(lt.Id.Global(lt.LocalId(1)))

    def test_serde_encodings(self):
        # Unit variants are strings, newtypes are transparent, unit structs are null.
        self.assertEqual(lt.to_json(lt.GetPlan.PassArrangements()), "PassArrangements")
        self.assertEqual(lt.to_json(lt.Id.Local(lt.LocalId(3))), {"Local": 3})
        self.assertEqual(lt.to_json(lt.BinaryFunc.Eq(lt.Eq())), {"Eq": None})
        self.assertEqual(
            lt.to_json(lt.LirScalarExpr.Column(2, None)), {"Column": [2, None]}
        )
        row = lt.Row([lt.Datum.int64(5), lt.Datum.null()])
        self.assertEqual(lt.to_json(row), {"$row": [{"Int64": 5}, "Null"]})
        # The default `StableRow` form, protobuf bytes, survives a round trip.
        raw = lt.from_json(lt.LiteralValue, {"Ok": [10, 2, 8, 1]})
        self.assertEqual(lt.to_json(raw), {"Ok": [10, 2, 8, 1]})
        # Defaulted fields may be missing on input.
        agg = lt.from_json(lt.ColumnOrder, {"column": 1})
        self.assertEqual(agg, lt.ColumnOrder(column=1, desc=False, nulls_last=False))


if __name__ == "__main__":
    unittest.main()
