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

    def test_parse_rejects_anonymous_variables(self):
        with self.assertRaisesRegex(datalog2lir.DatalogError, "anonymous variables"):
            datalog2lir.parse("p(x) :- e(x, _).")

    def test_parse_integers(self):
        (rule,) = datalog2lir.parse("p(y) :- e(-3, y).")
        self.assertEqual(rule.body[0].args, ("-3", "y"))
        with self.assertRaisesRegex(datalog2lir.DatalogError, "head arguments"):
            datalog2lir.parse("p(1) :- e(x, y).")
        with self.assertRaisesRegex(datalog2lir.DatalogError, "out of range"):
            datalog2lir.parse("p(y) :- e(9223372036854775808, y).")


class CompileTest(unittest.TestCase):
    def compile(
        self, program, output, imports=EDGES, limit=10000, return_at_limit=False
    ):
        envelope = datalog2lir.compile_program(
            datalog2lir.parse(program),
            output,
            imports,
            limit=limit,
            return_at_limit=return_at_limit,
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

    def test_cross_join_chain(self):
        env = self.compile(
            "p(a, b, c) :- n(a), n(b), n(c).", "p", {"n": "materialize.public.n"}
        )
        (join,) = nodes(env, "Join")
        linear = join["plan"]["Linear"]
        self.assertEqual(linear["source_key"], [])
        self.assertEqual([s["lookup_relation"] for s in linear["stage_plans"]], [1, 2])
        for stage in linear["stage_plans"]:
            self.assertEqual(stage["stream_key"], [])
            self.assertEqual(stage["lookup_key"], [])
        # The stream grows from (a) to (a, b), and the last stage outputs (a, b, c).
        first, second = linear["stage_plans"]
        self.assertEqual(first["stream_thinning"], [0])
        self.assertEqual(first["closure"]["before"]["mfp"]["projection"], [0, 1])
        self.assertEqual(second["stream_thinning"], [0, 1])
        self.assertEqual(second["closure"]["before"]["mfp"]["input_arity"], 3)
        self.assertEqual(second["closure"]["before"]["mfp"]["projection"], [0, 1, 2])
        for inp in join["inputs"]:
            self.assertEqual(
                inp["node"]["ArrangeBy"]["forms"]["arranged"], [[[], [0], [0]]]
            )

    def test_zero_arity_relations(self):
        env = self.compile(
            "has_loop() :- edges(x, x).\nguarded(x, y) :- has_loop(), edges(x, y).",
            "guarded",
            EDGES,
        )
        reduces = list(nodes(env, "Reduce"))
        self.assertEqual(
            reduces[0]["key_val_plan"]["key_plan"]["mfp"],
            {"expressions": [], "input_arity": 0, "predicates": [], "projection": []},
        )
        (join,) = nodes(env, "Join")
        # `has_loop` is arranged by the empty key with no values, `edges` by the
        # empty key with both columns as values.
        self.assertEqual(
            [i["node"]["ArrangeBy"]["forms"]["arranged"] for i in join["inputs"]],
            [[[[], [], []]], [[[], [0, 1], [0, 1]]]],
        )
        with self.assertRaisesRegex(datalog2lir.DatalogError, "has no columns"):
            self.compile("has_loop() :- edges(x, x).", "has_loop", EDGES)

    def test_integer_arguments_filter(self):
        env = self.compile("from_one(y) :- edges(1, y).", "from_one", EDGES)
        (get,) = [g for g in nodes(env, "Get") if "Global" in g["id"]]
        mfp = get["plan"]["Collection"]["mfp"]["mfp"]
        ((position, pred),) = mfp["predicates"]
        self.assertEqual(position, 1)
        self.assertEqual(
            pred["CallBinary"]["expr2"],
            {
                "Literal": [
                    {"Ok": {"$row": [{"Int64": 1}]}},
                    {"nullable": False, "scalar_type": "Int64"},
                ]
            },
        )
        self.assertEqual(mfp["projection"], [1])

    def test_imports_only_what_the_output_reads(self):
        env = self.compile(
            "p(x) :- a(x).\nq(x) :- b(x).\nr(x) :- p(x).",
            "r",
            {"a": "materialize.public.a", "b": "materialize.public.b"},
        )
        self.assertEqual(
            [i["source"] for i in env["imports"]], [{"Name": "materialize.public.a"}]
        )
        self.assertEqual(env["output"], {"Transient": 2})

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
        # Datalog means the fixpoint, so reaching the limit is an error by default.
        self.assertEqual(
            letrec["limits"], [{"max_iters": 77, "return_at_limit": False}]
        )
        env = self.compile(TC, "reachable", EDGES, limit=77, return_at_limit=True)
        (letrec,) = nodes(env, "LetRec")
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
        program = TC + "from_one(y) :- reachable(1, y).\n"
        text = datalog2lir.main(
            [
                "prog.dl",
                "--output",
                "from_one",
                "--import",
                'edges=db.sch."Edges"',
                "--return-at-limit",
            ],
            read=lambda _path: program,
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
        self.assertEqual(len(literal_rows), 1)
        (letrec,) = nodes(env, "LetRec")
        self.assertTrue(letrec["limits"][0]["return_at_limit"])
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
        with self.assertRaisesRegex(ValueError, "out of range for U64"):
            lt.to_json(lt.LirId(-1))
        with self.assertRaisesRegex(ValueError, "out of range for U64"):
            lt.to_json(lt.LirId(2**64))
        with self.assertRaisesRegex(ValueError, "out of range for I64"):
            lt.to_json(lt.Overflowing(2**63))
        with self.assertRaisesRegex(ValueError, "out of range for I64"):
            lt.to_json(lt.Row([lt.Datum.int64(-(2**63) - 1)]))
        with self.assertRaisesRegex(ValueError, "out of range for I16"):
            lt.to_json(lt.Row([lt.Datum("Int16", 2**15)]))
        for x in [float("nan"), float("inf"), float("-inf")]:
            with self.assertRaisesRegex(ValueError, "not a finite float"):
                lt.to_json(lt.Row([lt.Datum("Float64", x)]))
        with self.assertRaisesRegex(TypeError, "expected a byte array"):
            lt.from_json(lt.LiteralValue, {"Ok": [256]})
        with self.assertRaisesRegex(ValueError, "not a finite float"):
            lt.from_json(lt.LiteralValue, {"Ok": {"$row": [{"Float64": float("nan")}]}})

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
