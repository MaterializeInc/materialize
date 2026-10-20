# User-provided LIR plans demo

`demo.sh` walks through the success criteria of [the design](../../../doc/developer/design/20261019_user_provided_lir_plans.md) against a running local Materialize.
It prints each statement before running it, with a caption and a pause, and needs no input.
It drops and recreates the tables `transfers`, `flagged` and `edges` in `materialize.public`, and everything that depends on them.

## Running

Start Materialize and wait until it accepts connections on port 6875:

```
bin/environmentd --optimized
```

Then run the demo, which turns on `enable_lir_plans` as `mz_system` on port 6877:

```
misc/lir-datalog/demo/demo.sh
```

`MZ_HOST`, `MZ_PORT` and `MZ_SYSTEM_PORT` select another environment, and `DEMO_PAUSE` sets the pause after each caption in seconds (default 3).
To record the demo:

```
asciinema rec misc/lir-datalog/demo/demo.cast -c misc/lir-datalog/demo/demo.sh
asciinema play misc/lir-datalog/demo/demo.cast
```

## Steps

1. `flagged_transfers.sql` is a materialized view of the transfers from or to a flagged account.
   Its correlated subquery joins on `f.account = t.src OR f.account = t.dst`.
   The optimizer finds no equality to join on, so it decorrelates the subquery into a cross join of the distinct `(src, dst)` pairs with the flagged accounts, filters the product with the OR, and makes the matching pairs distinct.
   Both inputs of the cross join are arranged by the empty key.
2. `EXPLAIN PHYSICAL PLAN AS LIR JSON FOR MATERIALIZED VIEW` extracts the dataflow as a plan envelope.
3. `improve_plan.py` replaces the cross join with the union of one equi-join per equality, on arrangements by `src`, by `dst`, and by `account`.
   A pair that matches both equalities comes out of both joins, and the distinct reduction above the cross join removes the duplicate, so the view's results do not change.
   The script refuses to rewrite a cross join that does not feed a distinct reduction.
   `EXPLAIN PHYSICAL PLAN FOR CREATE MATERIALIZED VIEW ... FROM PLAN` validates the result without installing it.
4. `improve_plan.py --broken` leaves out the arrangement by `dst`, and the validator rejects the plan with an error that names the join and the missing arrangement.
5. The improved plan is installed as `flagged_transfers_fast`.
   `compare.sql` checks that both views return the same rows, and `cost.sql` compares the CPU time and arrangement sizes of their dataflows.
6. The SQL view has no keys, so `CREATE REPLACEMENT MATERIALIZED VIEW ... FROM PLAN` and `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT` swap the improved plan in under it.
   New rows reach both views, which still agree.
7. `datalog2lir.py` compiles the transitive closure in `../examples/tc.dl` to a plan, which is installed and queried, and then updated by an insert that closes a cycle.

## The improvement

The optimizer cannot produce the improved plan from the view's SQL, because it does not split a disjunction of join conditions into a union of joins.
The SQL can be rewritten by hand into `t.src IN (SELECT account FROM flagged) OR t.dst IN (SELECT account FROM flagged)`, which the optimizer plans with equi-joins, but its plan computes each membership test separately, with negations and further joins to tell matches from non-matches, and is larger than the edited plan.

With 50000 transfers and 1000 flagged accounts, on a local `bin/environmentd --optimized` with the default `quickstart` cluster:

| Dataflow | Hydration | CPU time | Arranged records | Arranged size |
|---|---|---|---|---|
| SQL view | 10.0 s | 10.9 s | 204964 | 1826 kB |
| Edited plan | 0.1 s | 0.4 s | 254964 | 2637 kB |

Hydration is how long the first `SELECT count(*)` after `CREATE` waits, and CPU time is the scheduling time of the dataflow's root operator after hydration, from `mz_introspection.mz_scheduling_elapsed`.

The cross join compares every pair with every flagged account, 50 million comparisons, while the equi-joins only visit matches.
The edited plan keeps one more arrangement of the pairs, because each equi-join reads them by a different key, which costs about 25% more arranged records and 45% more arranged bytes.

## Files

* `demo.sh` runs the demo.
* `improve_plan.py` rewrites the plan.
  It reuses `permutation_for_arrangement` from `../datalog2lir.py` for the arrangement layouts the validator requires.
* `setup.sql` creates and fills the tables, and `flagged_transfers.sql` creates the SQL view.
* `compare.sql` and `cost.sql` are the queries of steps 5 and 6.
