# `WITH MUTUALLY RECURSIVE`

## Summary

<!--
// Brief, high-level overview. A few sentences long.
// Be sure to capture the customer impact - framing this as a release note may be useful.
-->

Progressively introduce support for `WITH MUTUALLY RECURSIVE` common table expressions, as demonstrated in https://github.com/MaterializeInc/database-issues/issues/3264. This alternate form of `WITH` requires column names and identifiers for each binding, but allows all bindings to reference each other bindings, and introduces support for recursion and mutual recursion.

## Goals

<!--
// Enumerate the concrete goals that are in scope for the project.
-->

Support for a correct implementation of `WITH MUTUALLY RECURSIVE`.

There are likely several subgoals, corresponding to steps along the way.
These include support in at least each of the following layers.

1. SQL parsing
2. SQL name resolution
3. SQL planning
4. HIR generalization
5. Lowering
6. MIR generalization
7. MIR optimization corrections
8. LIR generalization
9. Rendering

The one intentional restriction is to avoid support for nested mutually recursive fragments.
These are not likely to be renderable in the foreseeable future, as they cannot necessarily be flattened to a single mutually recursive scope.

## Non-Goals

<!--
// Enumerate potential goals that are explicitly out of scope for the project
// ie. what could we do or what do we want to do in the future - but are not doing now
-->

1.  Support for SQL's `WITH RECURSIVE`.

    This is a tortured construct with surprising semantics.
    Instead `WITH MUTUALLY RECURSIVE` be easier to parse, plan, optimize, and render.
    We should learn from this in ways that may inform whether we want to later support `WITH RECURSIVE`.

2.  Optimization for recursive queries.

    There are several optimization patterns that recursive queries benefit from.
    For this design doc, we are only hoping to produce not-incorrect recursive queries.

3.  Nested mutual recursion.

    There should either be at most one `WITH MUTUALLY RECURSIVE`, or appear so after query optimization.
    We are not able to render nested mutually recursive fragements.

## Description

<!--
// Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
// If applicable, be sure to call out any new testing/validation that will be required
-->

Much of this is taken from https://github.com/MaterializeInc/database-issues/issues/3264.

We can extend SQL's `WITH` fragment with a new variant, not strictly more general, which looks like
```sql
WITH MUTUALLY RECURSIVE
    name_1 (col1a type1a, col1b type1b, ... ) AS ( <SELECT STATEMENT 1> ),
    name_2 (col2a type2a, col2b type2b, ... ) AS ( <SELECT STATEMENT 2> ),
    name_3 (col3a type3a, col3b type3b, ... ) AS ( <SELECT STATEMENT 3> ),
    ...
```
This fragment binds multiple identifiers, *must* specify all column names and types, and all bound names are available in each `SELECT` statement.

The value of each bound name in the following `SELECT` block (as for a standard `WITH`) is as if each were initially empty, and then each are updated in sequence using the most recent contents of each binding.
Mechanically, one could evaluate this as:
```rust
let mut bindings = HashMap::new();
for (name, _) in bindings.iter() {
    bindings.insert(name, empty());
}

let mut changed = true;
while changed {
    changed = false;
    for (name, statement) in bindings {
        let new_binding = statement.eval(&bindings);
        if new_binding != binding[name] {
            binding.insert(name, new_binding);
            changed = true;
        }
    }
}
```

As an example, one can write "standard" `WITH RECURSIVE` queries
```sql
WITH MUTUALLY RECURSIVE
    reach (a int, b int) AS (
        SELECT * FROM edges
        UNION
        SELECT edges.a, reach.b FROM edges, reach WHERE edges.b = reach.a
    ),
SELECT * FROM reach;
```
One can also write more complex non-linear, non-monotonic, mutual recursion
```sql
WITH MUTUALLY RECURSIVE
    odd_path (a int, b int) AS (
        SELECT * FROM edges
        UNION
        SELECT edges.a, even_path.b FROM edges, even_path WHERE edges.b = even_path.a
    ),
    even_path (a int, b int) AS (
        SELECT edges.a, odd_path.b FROM edges, odd_path WHERE edges.b = odd_path.a
        UNION
        -- This is optional, but demonstrates non-linear queries and reduces the "depth" of derivations.
        SELECT path1.a, path2.b FROM odd_path path1, odd_path path2 WHERE path1.b = path2.b
    ),
SELECT * FROM reach;
```
The `SELECT` blocks can be anything we support, except we are unlikely to support additional `WITH MUTUALLY RECURSIVE` blocks in them for some time.

The sequence of goals have their own design considerations

1.  SQL parsing

    We must support the `WITH MUTUALLY RECURSIVE` fragment and capture the column names and types.
    We are in a position to allow and capture further constraint information, such as nullability and unique keys.
    This happens by default with `parse_columns`, though we could intentionally avoid accepting that information until we know what to do with it.

2.  SQL name resolution

    SQL name resolution currently introduces CTE bindings after resolving names in the CTE.
    We would need to generalize this to introduce the names before name resolving recursive CTEs.

3.  SQL planning

    SQL planning currently introduces CTE bindings (types mainly) after planning the CTE.
    We would need to generalize this to introduce the types before planning recursive CTEs.

4.  HIR generalization

    HIR has a single-binding `Let` variant that cannot house mutually recursive bindings.
    We could introduce a `LetRec` or `MultiLet` variant to support this, and even migrate other uses to it.

5.  Lowering / decorrelation

    It is not clear that lowering and decorrelation are complicated.
    As existing single-binding `Let` constructs, the `value` and `body` are each prefixed with correlated columns of the outer scope, and this should continue to be the case.

6.  MIR generalization

    MIR also requires a multi-binding `Let`, which could be prototyped as `LetRec` or `MultiLet`.

7.  MIR optimization corrections

    All MIR optimizations are at risk of being incorrect with recursive `Let` bindings.
    Many of them propagate information about let bindings and must either a. start with conservative information (e.g. all columns required) or b. iterate to fixed point before applying their conclusions.

8.  LIR generalization

    LIR may want to *remove* `Let` and move all bindings in to the sequence of dataflow objects to construct.
    This is already roughly the same as having a multiple binding `Let` block around the whole statement.

9.  Rendering

    Rendering will need a few modifications, but they have been prototyped and seen to work multiple times.
    The main change is having each bindings result in a differential dataflow `Variable` which is then used instead of the collection itself.
    Once the definition of each object is complete, it is bound to its variable.

    We have not previously prototyped sequenced recursion, where there are two recursive blocks in sequence.
    We can support this in the dataflow layer, but we would likely need to change our dataflow descriptions to accommodate it.

## Alternatives

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

The only known alternative is SQL's `WITH RECURSIVE` which is sufficiently fraught that we ought not implement it directly.

There are several distinctions it has from the proposed `WITH MUTUALLY RECURSIVE` that it may help to visit.

1.  SQL's `WITH RECURSIVE` does not require column names and types.

    In principle the names and types could be inferred.
    In practice this appears to be a complicated task that would require substantial reconsideration.
    Given that `WITH MUTUALLY RECURSIVE` has no compatibility requirements, it seems reasonable to try out requiring this column information.

2.  SQL's `WITH RECURSIVE` requires each binding be used only once.

    This is confusing and only enforced syntactically.
    One can introduce a CTE renaming a binding and then use that rebinding multiple times.
    In principle and practice, the requirement does not exist.

3.  SQL's `WITH RECURSIVE` requires each select statement have a "base case" (`UNION`, `UNION ALL`, etc with a non-recursive fragment).

    This is not required for query planning, optimization, or execution.
    This may be helpful from a query admission point of view (should we allow you to run your query).
    We could likely introduce this constraint if we discover what it is valuable for.

There is an alternate evaluation order that updates all collections *concurrently*, rather than in sequence.
This would mean that it takes "one step" for each value to propogate to each other binding, rather than be immediately available.
This results in a different semantics, especially in the case of non-monotonic expressions (e.g. aggregation, retraction).
However, it can always be "faked" in the sequential update model, by introducing "mirroring" collections at the end of the block and only using these in other bindings.

The concurrent update pattern also makes life unneccesarily hard for the optimization team.
Ideally, all existing optimizations would apply now without changes, which is the case for sequential evaluation.
However, with concurrent evaluation it would be incorrect to apply certain transformations:
```sql
WITH MUTUALLY RECURSIVE (CONCURRENT)
    A (x int) as ... ,
    B (x int) as SELECT * FROM A,
    C (x int) as SELECT * FROM A EXCEPT ALL SELECT * FROM B
...
```
Ideally one could reason that `C` is empty, which is the case in sequential evaluation but not in concurrent evaluation.
In concurrent evaluation, `C` would contain version of `A` minus the prior iterate of `A`.
Despite working on a system that manages changing data, we've avoided until now the issue that the same name can be used for different versions of data.

## Open questions

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->
