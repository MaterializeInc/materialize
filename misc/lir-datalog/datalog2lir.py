#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Compiles a toy Datalog program to an LIR plan envelope for `CREATE ... FROM PLAN`.

    python3 misc/lir-datalog/datalog2lir.py PROGRAM.dl --output RELATION \\
        --import NAME=db.schema.item ...

A program is a list of rules `head(x, y) :- atom(x, z), atom(z, y).`, with `%` comments.
Atom arguments are variables or integers, and head arguments are variables. Relations
named by `--import` are the facts, every other relation is defined by rules. All columns
are `int8 NOT NULL`, and every defined relation is a set. Recursive relations are bound
by a `LetRec` with an iteration limit, where the dataflow reports an error unless
`--return-at-limit` is given.

The envelope has one object, a chain of `Let` and `LetRec` bindings in dependency order
whose body reads the output relation. Each binding is a `Union` of its rules, made
distinct by a `Reduce`. A rule with several atoms is a linear join in the order the atoms
are written, joining on the variables the next atom shares with the atoms before it.
"""

import argparse
import dataclasses
import re
import sys
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import lir_types as lt

LIR_VERSION = 1
DEFAULT_LIMIT = 10000


class DatalogError(Exception):
    """An invalid program or invocation."""


@dataclasses.dataclass(frozen=True)
class Atom:
    relation: str
    args: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class Rule:
    head: Atom
    body: tuple[Atom, ...]


_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_INT = r"-?[0-9]+"
_ATOM = re.compile(rf"\s*({_IDENT})\s*\(([^()]*)\)\s*")


def _parse_atom(text: str, where: str) -> Atom:
    m = _ATOM.fullmatch(text)
    if m is None:
        raise DatalogError(
            f"{where}: expected an atom `name(var, ...)`, got {text.strip()!r}"
        )
    args = tuple(a.strip() for a in m.group(2).split(",")) if m.group(2).strip() else ()
    for a in args:
        if a == "_":
            raise DatalogError(f"{where}: anonymous variables `_` are not supported")
        if re.fullmatch(_INT, a):
            if not -(2**63) <= int(a) < 2**63:
                raise DatalogError(f"{where}: {a} is out of range for int8")
        elif not re.fullmatch(_IDENT, a):
            raise DatalogError(
                f"{where}: atom arguments must be variables or integers, got {a!r}"
            )
    return Atom(m.group(1), args)


def _is_literal(arg: str) -> bool:
    return re.fullmatch(_INT, arg) is not None


def parse(source: str) -> list[Rule]:
    """Parses a program into its rules, in order."""
    text = "\n".join(line.split("%", 1)[0] for line in source.splitlines())
    rules = []
    for n, clause in enumerate(text.split(".")[:-1], start=1):
        where = f"rule {n}"
        if ":-" not in clause:
            raise DatalogError(
                f"{where}: facts are not supported, every rule needs a body"
            )
        head, body = clause.split(":-", 1)
        atoms = re.findall(r"[^,()]*\([^()]*\)|[^,]+", body)
        atoms = [a for a in atoms if a.strip()]
        if not atoms:
            raise DatalogError(f"{where}: every rule needs a body")
        head_atom = _parse_atom(head, where)
        if any(_is_literal(a) for a in head_atom.args):
            raise DatalogError(f"{where}: head arguments must be variables")
        rules.append(Rule(head_atom, tuple(_parse_atom(a, where) for a in atoms)))
    if text.split(".")[-1].strip():
        raise DatalogError("the last rule is not terminated by `.`")
    return rules


def _sccs(nodes: list[str], edges: dict[str, list[str]]) -> list[list[str]]:
    """Tarjan's algorithm. Returns components with dependencies before dependents."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    out: list[list[str]] = []

    def visit(v: str) -> None:
        index[v] = low[v] = len(index)
        stack.append(v)
        on_stack.add(v)
        for w in edges[v]:
            if w not in index:
                visit(w)
                low[v] = min(low[v], low[w])
            elif w in on_stack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            comp = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                comp.append(w)
                if w == v:
                    break
            out.append(sorted(comp, key=nodes.index))

    for v in nodes:
        if v not in index:
            visit(v)
    return out


def _col(i: int) -> lt.LirScalarExpr:
    return lt.LirScalarExpr.Column(i, None)


def _mfp(input_arity: int, projection: list[int], predicates=()) -> lt.MapFilterProject:
    return lt.MapFilterProject(
        expressions=[],
        predicates=list(predicates),
        projection=projection,
        input_arity=input_arity,
    )


def _safe(mfp: lt.MapFilterProject) -> lt.SafeMfpPlan:
    return lt.SafeMfpPlan(mfp=mfp)


def _mfp_plan(mfp: lt.MapFilterProject) -> lt.MfpPlan:
    return lt.MfpPlan(mfp=_safe(mfp), lower_bounds=[], upper_bounds=[])


def _identity(arity: int) -> lt.MapFilterProject:
    return _mfp(arity, list(range(arity)))


def permutation_for_arrangement(
    key: list[int], arity: int
) -> tuple[list[int], list[int]]:
    """The canonical layout of an arrangement of columns `key`, as Materialize computes it.

    Mirrors `MapFilterProject::permutation_for_arrangement`. The validator rejects
    arrangements in any other layout.
    """
    in_key = {c: i for i, c in enumerate(key)}
    cursor = len(key)
    permutation = []
    for c in range(arity):
        if c in in_key:
            permutation.append(in_key[c])
        else:
            permutation.append(cursor)
            cursor += 1
    thinning = [c for c in range(arity) if c not in in_key]
    return permutation, thinning


class _Compiler:
    def __init__(
        self,
        rules: list[Rule],
        imports: dict[str, str],
        limit: int,
        return_at_limit: bool,
    ):
        self.rules = rules
        self.limit = limit
        self.return_at_limit = return_at_limit
        self.arity: dict[str, int] = {}
        for rule in rules:
            for atom in (rule.head, *rule.body):
                known = self.arity.setdefault(atom.relation, len(atom.args))
                if known != len(atom.args):
                    raise DatalogError(
                        f"relation {atom.relation} is used with arities {known} and {len(atom.args)}"
                    )
        self.idb = list(dict.fromkeys(r.head.relation for r in rules))
        for name in self.idb:
            if name in imports:
                raise DatalogError(f"relation {name} is imported and defined by rules")
        for rule in rules:
            for atom in rule.body:
                if atom.relation not in self.idb and atom.relation not in imports:
                    raise DatalogError(
                        f"relation {atom.relation} is neither imported nor defined by rules"
                    )
        self.import_names = imports
        # Filled in by `compile` with the imports the output depends on.
        self.imports: dict[str, tuple[int, str]] = {}
        self.local: dict[str, int] = {}
        self.next_lir_id = 1

    def expr(self, node: lt.LirRelationNode) -> lt.LirRelationExpr:
        # Ids are assigned after the children, so they increase bottom-up, as in
        # optimizer plans.
        lir_id = self.next_lir_id
        self.next_lir_id += 1
        return lt.LirRelationExpr(lir_id=lt.LirId(lir_id), node=node)

    def get(self, relation: str, plan: lt.GetPlan) -> lt.LirRelationExpr:
        if relation in self.imports:
            id_ = lt.Id.Global(lt.GlobalId.Transient(self.imports[relation][0]))
        else:
            id_ = lt.Id.Local(lt.LocalId(self.local[relation]))
        return self.expr(
            lt.LirRelationNode.Get(
                id=id_, keys=lt.AvailableCollections(raw=True, arranged=[]), plan=plan
            )
        )

    def read_atom(self, atom: Atom) -> tuple[lt.LirRelationExpr, list[str]]:
        """Reads an atom's relation, with one column per distinct variable.

        A variable repeated in the atom, or an integer, becomes an equality filter.
        """
        first: dict[str, int] = {}
        predicates = []
        for i, v in enumerate(atom.args):
            if _is_literal(v):
                literal = lt.LirScalarExpr.Literal(
                    lt.LiteralValue.Ok(lt.Row([lt.Datum.int64(int(v))])),
                    lt.ReprColumnType(
                        nullable=False, scalar_type=lt.ReprScalarType.Int64()
                    ),
                )
                pred = lt.LirScalarExpr.CallBinary(
                    func=lt.BinaryFunc.Eq(lt.Eq()), expr1=_col(i), expr2=literal
                )
                predicates.append((i + 1, pred))
            elif v in first:
                pred = lt.LirScalarExpr.CallBinary(
                    func=lt.BinaryFunc.Eq(lt.Eq()), expr1=_col(first[v]), expr2=_col(i)
                )
                # A predicate is stated with the number of columns it needs.
                predicates.append((i + 1, pred))
            else:
                first[v] = i
        projection = list(first.values())
        arity = len(atom.args)
        if predicates or projection != list(range(arity)):
            plan = lt.GetPlan.Collection(_mfp_plan(_mfp(arity, projection, predicates)))
        else:
            plan = lt.GetPlan.PassArrangements()
        return self.get(atom.relation, plan), list(first)

    def arrange(self, input: lt.LirRelationExpr, key: list[int], arity: int):
        permutation, thinning = permutation_for_arrangement(key, arity)
        return self.expr(
            lt.LirRelationNode.ArrangeBy(
                input_key=None,
                input=input,
                input_mfp=_mfp_plan(_identity(arity)),
                forms=lt.AvailableCollections(
                    raw=True, arranged=[([_col(c) for c in key], permutation, thinning)]
                ),
                strategy=lt.ArrangementStrategy.Direct(),
            )
        )

    def rule(self, rule: Rule) -> lt.LirRelationExpr:
        """Compiles a rule body to a collection in the column order of the head."""
        reads = [self.read_atom(a) for a in rule.body]
        bound = {v for _, vs in reads for v in vs}
        for v in rule.head.args:
            if v not in bound:
                raise DatalogError(
                    f"head variable {v} of {rule.head.relation} is not bound"
                )
        if len(reads) == 1:
            ((input, vars),) = reads
            projection = [vars.index(v) for v in rule.head.args]
            if projection == list(range(len(vars))):
                return input
            return self.expr(
                lt.LirRelationNode.Mfp(
                    input=input,
                    mfp=_mfp_plan(_mfp(len(vars), projection)),
                    input_key_val=None,
                )
            )

        # Plan the stages first, to know the key each input is arranged by.
        stream = reads[0][1]
        stages = []
        for i in range(1, len(reads)):
            lookup = reads[i][1]
            shared = [v for v in lookup if v in stream]
            stream_key = [stream.index(v) for v in shared]
            lookup_key = [lookup.index(v) for v in shared]
            _, stream_thinning = permutation_for_arrangement(stream_key, len(stream))
            _, lookup_thinning = permutation_for_arrangement(lookup_key, len(lookup))
            # The joined row is the key, then the stream's and the lookup's values.
            joined = (
                shared
                + [stream[c] for c in stream_thinning]
                + [lookup[c] for c in lookup_thinning]
            )
            if i + 1 == len(reads):
                keep = list(rule.head.args)
            else:
                later = {v for _, vs in reads[i + 1 :] for v in vs} | set(
                    rule.head.args
                )
                keep = [v for v in dict.fromkeys(joined) if v in later]
            projection = [joined.index(v) for v in keep]
            closure = lt.JoinClosure(
                ready_equivalences=[],
                before=_safe(_mfp(len(joined), projection)),
            )
            stages.append((stream_key, stream_thinning, lookup_key, closure))
            stream = keep

        inputs = []
        source_key = stages[0][0]
        inputs.append(self.arrange(reads[0][0], source_key, len(reads[0][1])))
        for i, (_, _, lookup_key, _) in enumerate(stages, start=1):
            inputs.append(self.arrange(reads[i][0], lookup_key, len(reads[i][1])))
        plan = lt.LinearJoinPlan(
            source_relation=0,
            source_key=[_col(c) for c in source_key],
            initial_closure=None,
            stage_plans=[
                lt.LinearStagePlan(
                    lookup_relation=i,
                    stream_key=[_col(c) for c in stream_key],
                    stream_thinning=stream_thinning,
                    lookup_key=[_col(c) for c in lookup_key],
                    closure=closure,
                )
                for i, (stream_key, stream_thinning, lookup_key, closure) in enumerate(
                    stages, start=1
                )
            ],
            final_closure=None,
        )
        return self.expr(
            lt.LirRelationNode.Join(inputs=inputs, plan=lt.JoinPlan.Linear(plan))
        )

    def relation(self, name: str) -> lt.LirRelationExpr:
        """The distinct union of a relation's rules, as a raw collection."""
        arity = self.arity[name]
        bodies = [self.rule(r) for r in self.rules if r.head.relation == name]
        if len(bodies) == 1:
            (union,) = bodies
        else:
            union = self.expr(
                lt.LirRelationNode.Union(
                    inputs=bodies,
                    consolidate_output=False,
                    temporal_bucketing_strategies=[
                        lt.ArrangementStrategy.Direct() for _ in bodies
                    ],
                )
            )
        distinct = self.expr(
            lt.LirRelationNode.Reduce(
                input_key=None,
                input=union,
                key_val_plan=lt.KeyValPlan(
                    key_plan=_safe(_identity(arity)), val_plan=_safe(_mfp(arity, []))
                ),
                plan=lt.ReducePlan.Distinct(),
                mfp_after=_safe(_identity(arity)),
                temporal_bucketing_strategy=lt.ArrangementStrategy.Direct(),
            )
        )
        # A reduction produces only its arrangement by the group key. Bindings and
        # unions read raw collections, so form one from that arrangement.
        return self.expr(
            lt.LirRelationNode.ArrangeBy(
                input_key=[_col(c) for c in range(arity)],
                input=distinct,
                input_mfp=_mfp_plan(_identity(arity)),
                forms=lt.AvailableCollections(raw=True, arranged=[]),
                strategy=lt.ArrangementStrategy.Direct(),
            )
        )

    def compile(self, output: str) -> lt.LirPlanEnvelope:
        if output not in self.idb:
            raise DatalogError(f"output relation {output} is not defined by rules")
        # Zero-arity relations compile, but `CREATE MATERIALIZED VIEW name ()` does not
        # parse, so the output needs a column.
        if self.arity[output] == 0:
            raise DatalogError(
                f"output relation {output} has no columns, FROM PLAN needs at least one"
            )
        deps = {
            name: list(
                dict.fromkeys(
                    a.relation
                    for r in self.rules
                    if r.head.relation == name
                    for a in r.body
                    if a.relation in self.idb
                )
            )
            for name in self.idb
        }
        # Only the relations the output depends on.
        needed, todo = set(), [output]
        while todo:
            name = todo.pop()
            if name not in needed:
                needed.add(name)
                todo.extend(deps[name])
        order = [n for n in self.idb if n in needed]
        # Placeholder ids 1.. go to the imports the output depends on, in order of first
        # use, and the object takes the next one.
        used = dict.fromkeys(
            a.relation
            for r in self.rules
            if r.head.relation in needed
            for a in r.body
            if a.relation not in self.idb
        )
        self.imports = {
            name: (n, self.import_names[name]) for n, name in enumerate(used, start=1)
        }
        components = _sccs(order, {n: deps[n] for n in order})

        # Bindings, outermost first, as (ids, values, recursive).
        bindings = []
        for comp in components:
            for name in comp:
                self.local[name] = len(self.local)
            recursive = len(comp) > 1 or comp[0] in deps[comp[0]]
            values = [self.relation(name) for name in comp]
            bindings.append((comp, values, recursive))

        body = self.get(output, lt.GetPlan.PassArrangements())
        for comp, values, recursive in reversed(bindings):
            if recursive:
                body = self.expr(
                    lt.LirRelationNode.LetRec(
                        ids=[lt.LocalId(self.local[n]) for n in comp],
                        values=values,
                        limits=[
                            lt.LetRecLimit(
                                max_iters=self.limit,
                                return_at_limit=self.return_at_limit,
                            )
                            for _ in comp
                        ],
                        body=body,
                    )
                )
            else:
                (name,), (value,) = comp, values
                body = self.expr(
                    lt.LirRelationNode.Let(
                        id=lt.LocalId(self.local[name]), value=value, body=body
                    )
                )

        object_id = lt.GlobalId.Transient(len(self.imports) + 1)
        return lt.LirPlanEnvelope(
            lir_version=LIR_VERSION,
            imports=[
                lt.EnvelopeImport(
                    id=lt.GlobalId.Transient(n),
                    source=lt.ImportSource.Name(sql_name),
                    persist=True,
                    indexes=[],
                )
                for n, sql_name in self.imports.values()
            ],
            objects=[lt.EnvelopeObject(id=object_id, plan=body)],
            output=object_id,
        )


def compile_program(
    rules: list[Rule],
    output: str,
    imports: dict[str, str],
    limit: int = DEFAULT_LIMIT,
    return_at_limit: bool = False,
) -> lt.LirPlanEnvelope:
    """Compiles `rules` to an envelope whose result is the relation `output`.

    `imports` maps the relation names of the program to SQL item names. A recursive
    relation that has not converged after `limit` iterations is an error in the
    output, or, with `return_at_limit`, its contents at that point.
    """
    if limit < 1:
        raise DatalogError("the recursion limit must be positive")
    return _Compiler(rules, imports, limit, return_at_limit).compile(output)


def main(
    argv: list[str], read: Callable[[str], str] = lambda p: Path(p).read_text()
) -> str:
    """Runs the command line, returning the envelope as JSON text."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("program", help="the Datalog program")
    parser.add_argument("--output", required=True, help="the relation to compute")
    parser.add_argument(
        "--import",
        dest="imports",
        action="append",
        default=[],
        metavar="NAME=SQL_NAME",
        help="read relation NAME from the SQL relation SQL_NAME",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"iteration limit of recursive relations (default {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--return-at-limit",
        action="store_true",
        help="at the iteration limit, return the current contents instead of an error,"
        " which is not the fixpoint the program means",
    )
    args = parser.parse_args(argv)
    imports = {}
    for spec in args.imports:
        name, sep, sql_name = spec.partition("=")
        if not sep or not name or not sql_name:
            raise DatalogError(f"--import expects NAME=SQL_NAME, got {spec!r}")
        imports[name] = sql_name
    envelope = compile_program(
        parse(read(args.program)),
        args.output,
        imports,
        args.limit,
        args.return_at_limit,
    )
    return lt.dumps(envelope)


if __name__ == "__main__":
    try:
        print(main(sys.argv[1:]))
    except DatalogError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
