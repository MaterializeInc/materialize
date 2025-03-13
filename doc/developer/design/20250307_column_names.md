# Precise column name tracking

- Associated: https://github.com/MaterializeInc/database-issues/issues/8889

Our `EXPLAIN` output typically uses column numbers. It should as much
as possible use column names.

## The Problem

Our `EXPLAIN WITH (humanized_expressions)` (the default) propagates
names upwards from tables---which means we miss many opportunities for
column names. Consider this simple example:

```sql
CREATE TABLE t(x INT, y TEXT);
EXPLAIN SELECT x + 1 AS succ_x, y FROM t;
```

Notice that the name `y` is kept in the _named_ column reference
`#1{y}`, but `succ_x` is lost in the column reference `#2`.

```
Explained Query:
  Project (#2, #1{y}) // { arity: 2 }
    Map ((#0{x} + 1)) // { arity: 3 }
      ReadStorage materialize.public.t // { arity: 2 }

Source materialize.public.t

Target cluster: quickstart
```

## Success Criteria

For an MIR expression `q`, define:

```
NAMESCORE(q) = |# of named column references|
               ------------------------------
                  |# of column references|
```

i.e., `NAMESCORE(q)` is the percentage of column references which we
can attach a name to.

We should expect to see a serious improvement in `NAMESCORE` across
both our tests and customer queries.

## Out of Scope

We should _not_ expect `NAMESCORE(q) = 1.0` for all queries `q`.

In particular, window functions may significantly complicate our
ability to keep track of names.

We should not reimplement all of the `Analysis` framework for LIR.

## Solution Proposal

We will need to keep track of more column name information. We propose
doing so in a few positions:

1. Keep track of column names all `ScalarExpr` constructors.

2. Keep track of column names on `usize` column references in
   `*RelationExpr`.

In both cases, we will work with `Option<Rc<str>>`, being careful
to generate a single reference for each string, i.e., we will do
lightweight, reference-counted string internment to avoid tons of
extra allocations.

### Column names on `{Hir,Mir}ScalarExpr::Column`

Extend `HirScalarExpr` and `MirScalarExpr` to optionally keep track of
column names, i.e.:

```rust
pub enum HirScalarExpr {
  Column(ColumnRef, Option<Rc<str>>),
  CallUnary {
    func: UnaryFunc,
    expr: Box<HirScalarExpr>,
    name: Option<Rc<str>>>,
  }
  // ...
}

impl HirScalarExpr {
  pub fn name(&self) -> Option<Rc<str>> {
    match self {
      HirScalarExpr::Column(_, name)
      | HirScalrExpr::CallUnary { name, .. }
      | ... => name.clone()
    }
  }
}
```

Keeping track of column names on _every_ expression makes it so we can
remember names for SQL CTEs. Lowering would change to generate scalar
expressions with names for:

  - column references
  - `SELECT expr AS name, ...`
  - `CREATE VIEW v(name, ...) AS ...`
  - `WITH t(name, ...), ...`
  - `WITH MUTUALLY RECURSIVE t(name type, ...), ...)`

### Column names in `{Hir,Mir}RelationExpr::{Project,Reduce,TopK}`

Extend `HirScalarExpr` and `MirScalarExpr` to optionally keep track of
the column name.

```rust
pub enum HirRelationExpr {
    Project {
        input: Box<HirRelationExpr>,
        outputs: Vec<(usize, Option<Rc<str>>)>,
    },
    // ...
}

pub enum MirRelationExpr {
    Project {
        input: Box<MirRelationExpr>,
        outputs: Vec<(usize, Option<Rc<str>>)>,
    },
    // ...
}
```

The `Project` is the most critical, as projections appear at the top
of nearly every `SELECT` query---the missing `succ_x` in the example
above appears in a `Project`, for example.

### The new `EXPLAIN AS TEXT` format requires names for LIR

https://github.com/MaterializeInc/materialize/pull/31643 proposes a
new `EXPLAIN AS TEXT` format based on LIR. We will very much want good
column names in the new format, which means keeping track of names at
the LIR level, too.

We currently compute arity, names, and other metadata using the
`Analysis` bottom-up framework. But `Analysis` is only defined for
MIR. We should build simple, direct versions of these analyses for
LIR---and we can generalize it to a lower-level `Analysis` framework
if we later need it.

## Alternatives

Rather than attaching column names on every `ScalarExpr` node, we
could just record them on `*ScalarExpr::Column`. Similarly, rather
than attaching column names to to `*RelationExpr` nodes, an `EXPLAIN`
plan has sufficient information to reconstruct the names of the
outermost projection, as the original SQL select (with column name
information) should be present:

```rust
ExplainPlanPlan.explainee ==
  Explainee::Statement(
    ExplaineeStatement::Select {
      plan: SelectPlan {
          select: Option<Box<SelectStatement<Aug>>>,
          ..
        },
      ..
      },
  )
```

Neither of these approach doesn't scale naturally to having multiple
views or CTEs, though.

## Open questions

Is it possible to get a good `NAMESCORE` without changing
`{Hir,Mir}RelationExpr`?
