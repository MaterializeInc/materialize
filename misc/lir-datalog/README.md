# Toy Datalog to LIR compiler

A demonstration that the LIR plan format of `CREATE MATERIALIZED VIEW ... FROM PLAN` can be targeted from outside the repository.
The compiler reads a small Datalog program and prints a plan envelope that Materialize validates and installs.
It uses only the Python standard library and imports nothing from Materialize.
Its inputs are the serde registry of the format and the envelopes that `EXPLAIN PHYSICAL PLAN AS LIR JSON` prints.

## Usage

```
python3 misc/lir-datalog/datalog2lir.py misc/lir-datalog/examples/tc.dl \
    --output reachable --import edges=materialize.public.edges
```

The envelope goes to stdout.
Install it with the feature flag `enable_lir_plans` on:

```sql
CREATE MATERIALIZED VIEW reachable (src int8 NOT NULL, dst int8 NOT NULL)
FROM PLAN '<envelope>';
```

`test/sqllogictest/lir_plans_datalog.slt` installs the compiled examples and compares them with the same queries in SQL.

## Language

* A program is a list of rules `head(x, y) :- atom(x, z), atom(z, y).`, with `%` comments.
* Atom arguments are variables or integers, and head arguments are variables.
  A variable shared between atoms is an equality join, and a variable repeated within an atom, or an integer, is an equality filter.
  The anonymous variable `_` is not supported.
* Facts come only from imports, `--import NAME=SQL_NAME`, where `SQL_NAME` is a SQL item name with SQL quoting.
  The envelope imports only the relations the output depends on.
* Every column is `int8 NOT NULL`, and every defined relation is a set.
  Relations without columns, such as `has_loop()`, can be used in rules, but the output needs a column.
* Recursion, including mutual recursion, is allowed.
  A recursive binding that has not reached its fixpoint after `--limit` iterations (default 10000) makes the view report an error, as `ERROR AT RECURSION LIMIT` does in SQL.
  `--return-at-limit` returns the contents at the limit instead, which is not the fixpoint the program means.

## Plan shape

The envelope has one import per imported relation and one object.
The object is a chain of `Let` bindings for non-recursive relations and `LetRec` bindings for recursive components, in dependency order, whose body reads the output relation.
Each relation is the `Union` of its rules, made distinct by a `Reduce`, followed by an `ArrangeBy` that forms the raw collection from the reduction's arrangement.
A rule with several atoms is a linear join in the order the atoms are written.
Each join input is an `ArrangeBy` that forms the arrangement its stage reads, in the canonical layout the validator requires.
This follows the plan the optimizer produces for the same query written with `WITH MUTUALLY RECURSIVE`.

## Files

* `gen_lir_types.py` generates `lir_types.py` from `src/compute-types/tests/snapshots/lir_v1.json`.
  Rerun it, then `bin/fmt`, when the snapshot changes.
* `lir_types.py` has a dataclass for every type of the registry and a codec for the JSON that serde_json writes.
  The compiler builds plans only from these types.
* `datalog2lir.py` is the compiler.
* `demo/` holds a scripted demo of extracting, editing and installing plans, see `demo/README.md`.
* `test_datalog2lir.py` holds the tests, run with `python3 -m unittest misc/lir-datalog/test_datalog2lir.py`.
  They check that `lir_types.py` is fresh and that every envelope in the LIR plan sqllogictest files decodes and re-encodes unchanged.
