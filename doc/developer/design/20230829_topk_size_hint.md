# Top-K Group Size Query Hints

- Associated: MaterializeInc/database-issues#5578

## The Problem

Min/max reductions and top-k operations are computed in Materialize by creating a stack of
arrangements and performing tournament-tree-style hierarchical processing of updates. By default,
the height of the hierarchies created is sized to accommodate the existence of potentially
very large query groups in the data. This implies that Materialize provides an incremental update
experience from the start, even in the presence of large input query groups, but this comes at the
cost of extra memory usage.

Memory usage can be reduced substantially for queries with min/max or top-k by tuning the
`EXPECTED GROUP SIZE` query hint. The query hint informs Materialize about the maximum size of
the query groups that are expected in the input to the min/max reduction or top-k operation. Based
on this information, Materialize can size down the height of the hierarchy and stack fewer
arrangements.

Currently, the `EXPECTED GROUP SIZE` controls the tuning of both min/max reductions and top-k computations.
However, when the two patterns co-occur in the same query block, it is currently impossible to tune them
independently. This problem is reported in issue MaterializeInc/database-issues#5578. We expand the first
example query provided in the issue to illustrate the problem in more detail:

```sql
SELECT
    sum(max_revenue) AS sum_max_revenue
FROM (
    SELECT l_orderkey,
           max(l_extendedprice * (1 - l_discount)) AS max_revenue
    FROM lineitem
    GROUP BY l_orderkey
    OPTIONS (EXPECTED GROUP SIZE = 7)
    ORDER BY max_revenue DESC
    LIMIT 10
);
```

The `EXPLAIN` output for the above query is:

```
                                   Optimized Plan
-------------------------------------------------------------------------------------
 Explained Query:                                                                   +
   Return                                                                           +
     Union                                                                          +
       Get l0                                                                       +
       Map (null)                                                                   +
         Union                                                                      +
           Negate                                                                   +
             Project ()                                                             +
               Get l0                                                               +
           Constant                                                                 +
             - ()                                                                   +
   With                                                                             +
     cte l0 =                                                                       +
       Reduce aggregates=[sum(#0)]                                                  +
         TopK order_by=[#0 desc nulls_first] limit=10 exp_group_size=7              +
           Project (#1)                                                             +
             Reduce group_by=[#0] aggregates=[max((#1 * (1 - #2)))] exp_group_size=7+
               Project (#0, #5, #6)                                                 +
                 Get materialize.public.lineitem                                    +

(1 row)
```

Above, we see that the `exp_group_size` for *both* the max reduction and the top-k operation
are set to the same value. This may not be desirable as the size of the groups going into the
reduction above correspond to the number of line items per order (maximum 7), but the size of
the single group going into the top-k operation corresponds to the number of orders (1.5 million
times the scale factor).

To fix the above, the user presently has two options:

1. Take the maximum of the hints (above, 1.5 million times the scale factor), but leave substantial
potential for memory savings on the table.
2. Rewrite the SQL to separate the operations into different query blocks. In the example:

```sql
SELECT
    sum(max_revenue) AS sum_max_revenue
FROM (
    SELECT max_revenue
    FROM (
        SELECT l_orderkey,
               max(l_extendedprice * (1 - l_discount)) AS max_revenue
        FROM lineitem
        GROUP BY l_orderkey
        OPTIONS (EXPECTED GROUP SIZE = 7)
    )
    OPTIONS (EXPECTED GROUP SIZE = 1500000)
    ORDER BY max_revenue DESC
    LIMIT 10
);
```
```
                                   Optimized Plan
-------------------------------------------------------------------------------------
 Explained Query:                                                                   +
   Return                                                                           +
     Union                                                                          +
       Get l0                                                                       +
       Map (null)                                                                   +
         Union                                                                      +
           Negate                                                                   +
             Project ()                                                             +
               Get l0                                                               +
           Constant                                                                 +
             - ()                                                                   +
   With                                                                             +
     cte l0 =                                                                       +
       Reduce aggregates=[sum(#0)]                                                  +
         TopK order_by=[#0 desc nulls_first] limit=10 exp_group_size=1500000        +
           Project (#1)                                                             +
             Reduce group_by=[#0] aggregates=[max((#1 * (1 - #2)))] exp_group_size=7+
               Project (#0, #5, #6)                                                 +
                 Get materialize.public.lineitem                                    +

(1 row)
```

To avoid using the workaround above and directly hint the original SQL statement, we would need
to break the ambiguity regarding to which construct the hint applies.

## Success Criteria

* A solution to the problem must allow independent specification of hints to control min/max
reductions and top-k operations even if they co-occur in the same query block.
* Given that the problem is one of usability and a workaround exists, a solution to the problem
should ideally avoid operational complexity and eliminate issues with backwards compatibility.
In other words, queries that currently use the `EXPECTED GROUP SIZE` query hint should not have
to be rewritten to use a different hint. At the same time, a user could themself choose to exploit
higher potential for memory savings with minimal changes to their SQL (i.e., by changing the
hints in the `OPTIONS` clause).

## Out of Scope

* Fixing any understandability issues with the current `EXPECTED GROUP SIZE` hint is considered
out of scope to solve the problem above. In particular, sometimes the `EXPECTED GROUP SIZE` is
useful even if a `GROUP BY` clause is not present in the query, e.g.:

  ```sql
  SELECT max(l_orderkey) FROM lineitem
  OPTIONS (EXPECTED GROUP SIZE = 6000000)
  ```

  ```sql
  SELECT DISTINCT ON(l_orderkey) l_orderkey, l_linenumber
  FROM lineitem
  OPTIONS (EXPECTED GROUP SIZE = 7)
  ORDER BY l_orderkey, l_extendedprice DESC;
  ```

  In the queries above, users may be confused about what `GROUP` is referred to.
  We do not aim to solve this understandability issue with the existing hint as part of this design.

* Presently, if a query hint is provided but not used, no notice nor error is produced. For example:

  ```sql
  SELECT * FROM lineitem
  OPTIONS (EXPECTED GROUP SIZE = 6000000)
  ```

  Above, the query will be executed by simply ignoring the `EXPECTED GROUP SIZE` hint.
  We deem out-of-scope to change this behavior.

* An `EXPECTED GROUP SIZE` hint specification will be provided to reductions that do *not* include
min/max aggregates in MIR, but will be subsequently be attached only to hierarchical reductions that
employ bucketed plans in lowering to LIR and ignored otherwise. For example:

  ```sql
  SELECT l_linenumber, sum(l_extendedprice)
  FROM lineitem
  GROUP BY l_linenumber
  OPTIONS (EXPECTED GROUP SIZE = 860000);
  ```
  ```
                            Optimized Plan
  -------------------------------------------------------------------
   Explained Query:                                                 +
     Reduce group_by=[#0] aggregates=[sum(#1)] exp_group_size=860000+
       Project (#3, #5)                                             +
         Get materialize.public.lineitem                            +

  (1 row)
  ```

  We do not aim to change this current characteristic of the code.


## Solution Proposal

In a single query block, we can add different kinds of SQL constructs that include
min/max aggregates and different flavors of top-k operations. Consider the following
example:

```sql
CREATE TABLE teachers (id INT, name TEXT);
CREATE TABLE sections (id INT, teacher_id INT, course_id INT, schedule TEXT);

CREATE MATERIALIZED VIEW nested_distinct_on_group_by_limit AS
SELECT SUM(id) AS sum_id, SUM(teacher_id) AS sum_teacher_id, SUM(max_course_id) AS sum_max_course_id
FROM (
    SELECT DISTINCT ON(teacher_id) id, teacher_id, MAX(course_id) AS max_course_id
    FROM sections
    GROUP BY id, teacher_id
    OPTIONS (EXPECTED GROUP SIZE = 1000)
    ORDER BY teacher_id, id
    LIMIT 2
);
```
```
                                      Optimized Plan
------------------------------------------------------------------------------------------
 materialize.public.nested_distinct_on_group_by_limit:                                   +
   Return                                                                                +
     Union                                                                               +
       Get l0                                                                            +
       Map (null, null, null)                                                            +
         Union                                                                           +
           Negate                                                                        +
             Project ()                                                                  +
               Get l0                                                                    +
           Constant                                                                      +
             - ()                                                                        +
   With                                                                                  +
     cte l0 =                                                                            +
       Reduce aggregates=[sum(#0), sum(#1), sum(#2)]                                     +
         TopK order_by=[#1 asc nulls_last, #0 asc nulls_last] limit=2 exp_group_size=1000+
           TopK group_by=[#1] order_by=[#0 asc nulls_last] limit=1 exp_group_size=1000   +
             Reduce group_by=[#0, #1] aggregates=[max(#2)] exp_group_size=1000           +
               Project (#0..=#2)                                                         +
                 Get materialize.public.sections                                         +

(1 row)
```

To disambiguate the query hints when necessary, we argue for an approach with the
following characteristics:
1. Maintain backwards compatibility with `EXPECTED GROUP SIZE` by allowing users to use this
hint with the exact same semantics it has today, i.e., if the `EXPECTED GROUP SIZE` is specified,
it is attached to all instances of reductions and top-k operators originating from the query block.
2. Introduce three additional query hints that attach to specific clauses in the query block,
allowing the user to disambiguate the application of the hints to the reduction or to different
instances of top-k operations in the query block. If these new hints are specified together with
the `EXPECTED GROUP SIZE`, the statement will error out.

The error behavior advocated for in 2. above ensures that either the user will employ the new,
more ergonomic hints, or alternatively rely on the backwards compatible `EXPECTED GROUP SIZE`.
It eliminates any concerns regarding interactions between the new hints and the old one.

To operationalize the above, the following new hints are proposed:

1. `AGGREGATE INPUT GROUP SIZE`: This hint attaches to the `Reduce` operator implementing the aggregation
in the query block.
2. `DISTINCT ON INPUT GROUP SIZE`: This hint attaches to the `TopK` operator implementing the
`DISTINCT ON` clause.
3. `LIMIT INPUT GROUP SIZE`: This hint attaches to the `TopK` operator implementing the `LIMIT` clause.

## Minimal Viable Prototype

The following queries illustrate how the proposed query hints can be used in a manner that is
backwards compatible with the current syntax, but allows for breaking the ambiguity when
desired.

```sql
CREATE MATERIALIZED VIEW nested_distinct_on_group_by_limit AS
SELECT SUM(id) AS sum_id, SUM(teacher_id) AS sum_teacher_id, SUM(max_course_id) AS sum_max_course_id
FROM (
    SELECT DISTINCT ON(teacher_id) id, teacher_id, MAX(course_id) AS max_course_id
    FROM sections
    GROUP BY id, teacher_id
    OPTIONS (AGGREGATE INPUT GROUP SIZE = 1000, LIMIT INPUT GROUP SIZE = 50)
    ORDER BY teacher_id, id
    LIMIT 2
);
```
```
Expected Plan:
                                      Optimized Plan
------------------------------------------------------------------------------------------
 materialize.public.nested_distinct_on_group_by_limit:                                   +
   Return                                                                                +
     Union                                                                               +
       Get l0                                                                            +
       Map (null, null, null)                                                            +
         Union                                                                           +
           Negate                                                                        +
             Project ()                                                                  +
               Get l0                                                                    +
           Constant                                                                      +
             - ()                                                                        +
   With                                                                                  +
     cte l0 =                                                                            +
       Reduce aggregates=[sum(#0), sum(#1), sum(#2)]                                     +
         TopK order_by=[#1 asc nulls_last, #0 asc nulls_last] limit=2 exp_group_size=50  +
           TopK group_by=[#1] order_by=[#0 asc nulls_last] limit=1                       +
             Reduce group_by=[#0, #1] aggregates=[max(#2)] exp_group_size=1000           +
               Project (#0..=#2)                                                         +
                 Get materialize.public.sections                                         +

(1 row)
```

```sql
CREATE MATERIALIZED VIEW nested_distinct_on_group_by_limit AS
SELECT SUM(id) AS sum_id, SUM(teacher_id) AS sum_teacher_id, SUM(max_course_id) AS sum_max_course_id
FROM (
    SELECT DISTINCT ON(teacher_id) id, teacher_id, MAX(course_id) AS max_course_id
    FROM sections
    GROUP BY id, teacher_id
    OPTIONS (LIMIT INPUT GROUP SIZE = 50, DISTINCT ON INPUT GROUP SIZE = 60)
    ORDER BY teacher_id, id
    LIMIT 2
);
```
```
Expected Plan:
                                      Optimized Plan
------------------------------------------------------------------------------------------
 materialize.public.nested_distinct_on_group_by_limit:                                   +
   Return                                                                                +
     Union                                                                               +
       Get l0                                                                            +
       Map (null, null, null)                                                            +
         Union                                                                           +
           Negate                                                                        +
             Project ()                                                                  +
               Get l0                                                                    +
           Constant                                                                      +
             - ()                                                                        +
   With                                                                                  +
     cte l0 =                                                                            +
       Reduce aggregates=[sum(#0), sum(#1), sum(#2)]                                     +
         TopK order_by=[#1 asc nulls_last, #0 asc nulls_last] limit=2 exp_group_size=50  +
           TopK group_by=[#1] order_by=[#0 asc nulls_last] limit=1 exp_group_size=60     +
             Reduce group_by=[#0, #1] aggregates=[max(#2)]                               +
               Project (#0..=#2)                                                         +
                 Get materialize.public.sections                                         +

(1 row)
```

```sql
CREATE MATERIALIZED VIEW nested_distinct_on_group_by_limit AS
SELECT SUM(id) AS sum_id, SUM(teacher_id) AS sum_teacher_id, SUM(max_course_id) AS sum_max_course_id
FROM (
    SELECT DISTINCT ON(teacher_id) id, teacher_id, MAX(course_id) AS max_course_id
    FROM sections
    GROUP BY id, teacher_id
    OPTIONS (AGGREGATE INPUT GROUP SIZE = 1000, DISTINCT ON INPUT GROUP SIZE = 60, LIMIT INPUT GROUP SIZE = 50)
    ORDER BY teacher_id, id
    LIMIT 2
);
```
```
Expected Plan:
                                      Optimized Plan
------------------------------------------------------------------------------------------
 materialize.public.nested_distinct_on_group_by_limit:                                   +
   Return                                                                                +
     Union                                                                               +
       Get l0                                                                            +
       Map (null, null, null)                                                            +
         Union                                                                           +
           Negate                                                                        +
             Project ()                                                                  +
               Get l0                                                                    +
           Constant                                                                      +
             - ()                                                                        +
   With                                                                                  +
     cte l0 =                                                                            +
       Reduce aggregates=[sum(#0), sum(#1), sum(#2)]                                     +
         TopK order_by=[#1 asc nulls_last, #0 asc nulls_last] limit=2 exp_group_size=50  +
           TopK group_by=[#1] order_by=[#0 asc nulls_last] limit=1 exp_group_size=60     +
             Reduce group_by=[#0, #1] aggregates=[max(#2)] exp_group_size=1000           +
               Project (#0..=#2)                                                         +
                 Get materialize.public.sections                                         +

(1 row)
```

```sql
CREATE MATERIALIZED VIEW nested_distinct_on_group_by_limit AS
SELECT SUM(id) AS sum_id, SUM(teacher_id) AS sum_teacher_id, SUM(max_course_id) AS sum_max_course_id
FROM (
    SELECT DISTINCT ON(teacher_id) id, teacher_id, MAX(course_id) AS max_course_id
    FROM sections
    GROUP BY id, teacher_id
    OPTIONS (LIMIT INPUT GROUP SIZE = 50, EXPECTED GROUP SIZE = 1000)
    ORDER BY teacher_id, id
    LIMIT 2
);
```
```
Expected Plan:
ERROR: EXPECTED GROUP SIZE cannot be used in combination with LIMIT INPUT GROUP SIZE.
```

```sql
CREATE MATERIALIZED VIEW nested_distinct_on_group_by_limit AS
SELECT SUM(id) AS sum_id, SUM(teacher_id) AS sum_teacher_id, SUM(max_course_id) AS sum_max_course_id
FROM (
    SELECT DISTINCT ON(teacher_id) id, teacher_id, MAX(course_id) AS max_course_id
    FROM sections
    GROUP BY id, teacher_id
    OPTIONS (EXPECTED GROUP SIZE = 1000)
    ORDER BY teacher_id, id
    LIMIT 2
);
```
```
Expected Plan:
                                      Optimized Plan
------------------------------------------------------------------------------------------
 materialize.public.nested_distinct_on_group_by_limit:                                   +
   Return                                                                                +
     Union                                                                               +
       Get l0                                                                            +
       Map (null, null, null)                                                            +
         Union                                                                           +
           Negate                                                                        +
             Project ()                                                                  +
               Get l0                                                                    +
           Constant                                                                      +
             - ()                                                                        +
   With                                                                                  +
     cte l0 =                                                                            +
       Reduce aggregates=[sum(#0), sum(#1), sum(#2)]                                     +
         TopK order_by=[#1 asc nulls_last, #0 asc nulls_last] limit=2 exp_group_size=1000+
           TopK group_by=[#1] order_by=[#0 asc nulls_last] limit=1 exp_group_size=1000   +
             Reduce group_by=[#0, #1] aggregates=[max(#2)] exp_group_size=1000           +
               Project (#0..=#2)                                                         +
                 Get materialize.public.sections                                         +

(1 row)
```

We illustrate a few more queries with the usage of the new hints and variations of top-k patterns:

```sql
CREATE MATERIALIZED VIEW sections_of_top_3_courses_per_teacher AS
SELECT id AS teacher_id, section_id
FROM teachers grp,
     LATERAL (SELECT id AS section_id
              FROM sections
              WHERE teacher_id = grp.id
              OPTIONS (LIMIT INPUT GROUP SIZE = 1000)
              ORDER BY course_id DESC
              LIMIT 3);
```

In the query above, the hint `EXPECTED GROUP SIZE` could have been used interchangeably, since there
is no ambiguity and the hint applies to the `LIMIT` clause.

```sql
CREATE MATERIALIZED VIEW max_sections_of_top_3_courses_per_teacher AS
SELECT id AS teacher_id, max_section_id
FROM teachers grp,
     LATERAL (SELECT course_id, MAX(id) AS max_section_id
              FROM sections
              WHERE teacher_id = grp.id
              GROUP BY course_id
              OPTIONS (AGGREGATE INPUT GROUP SIZE = 1000, LIMIT INPUT GROUP SIZE = 20)
              ORDER BY course_id DESC
              LIMIT 3);
```

The above query specifies both the `AGGREGATE INPUT GROUP SIZE` and the `LIMIT INPUT GROUP SIZE` wherein the
`AGGREGATE INPUT GROUP SIZE` will thus only apply to the min/max reduction while the `LIMIT INPUT GROUP SIZE`
will apply to the top-k operation.

## Other Remarks

This section captures a few more suggestions that came up during discussion of the proposal in this design doc.

1. The three new hints are arguably more ergonomic and should be the preferred way to tune the `exp_group_size`
of various operators. So we should update our documentation to primarily refer to the three new hints. Perhaps
not even mentioning the old `EXPECTED GROUP SIZE` at all would allow us to create a path for syntax deprecation
in the future (even though that process is not in scope for this design).

2. Two follow-up items that are related improvements to usability of the hints include: (a) Producing errors
when the hints do not apply (MaterializeInc/database-issues#6472); (b) Moving the `OPTIONS` clause to a different
position in the `SELECT` syntax and potentially renaming it (MaterializeInc/database-issues#6473).
These two additional improvements are considered out-of-scope for this design, but during implementation
we will evaluate if opportunity arises to address them expediently (leaving them to future work otherwise).

## Alternatives

### Different hinting strategies

Two main categories of approches were discussed to solve the problem:

1. *Hints are about semantic properties:* If the user informs us of an `EXPECTED GROUP COUNT`,
this hint could transcend the configuration of top-k operators. It could be used for
cardinality estimation, for example. However, it's not very clear what we mean if we use this
hint in the context of a `LATERAL` join. Probably a clearer hint there would be something
describing the expected number of join pairs per row of the outer relation in the join.

2. *Hints are about configuring low-level operators:* In this line of thinking, we accept that
hints should apply to particular low-level operators and users need to know what operator they are
targeting with a particular hint. Instead of `EXPECTED GROUP COUNT`, we could directly specify
something like `EXPECTED TOPK GROUP SIZE`.

The proposal in this design document is a hybrid of the two categories above in that we attach
semantics to specific SQL clauses, which are then attached to particular HIR and MIR operators.
We find this approach more workable that hints about low-level operators, since the same SQL
query block can include different syntactic constructs that refer to different instances of the
same type of low-level operator.

The suggestion in MaterializeInc/database-issues#5578 to add an `EXPECTED GROUP COUNT` hint is a
proposal that follows a semantic hinting philosophy, but is higher-level in than the proposal
in this design in that it does not refer to a specific SQL clause. Given the many variations
that SQL syntax includes, we found it tricky to define an extensive hint set about semantic
properties that would match the many syntactic variations, especially for top-k. This is why
this proposal focuses on attaching hints to the SQL syntax that encodes the reduction and
top-k variants.

### Non-backward compatible changes

We considered changing the current behavior of `EXPECTED GROUP SIZE`. For example, we could make the
`EXPECTED GROUP SIZE` apply only to the reduction in a single query block and force users to
specify the other proposed hints for top-k constructs. Additionally, `AGGREGATE INPUT GROUP SIZE` could
be introdced as a synonym for `EXPECTED GROUP SIZE`, which would in turn be deprecated.

However, such a change would introduce operational complexity. Migration procedures would need to be
devised and implemented where we rewrite production queries to introduce the additional
hints instead of only the `EXPECTED GROUP SIZE` in single block queries with multiple constructs.
Additional migration procedures would need to rewrite indexed / materialized view definitions
in the catalog to change `EXPECTED GROUP SIZE` to `AGGREGATE INPUT GROUP SIZE`. Finally, a syntax
deprecation process would need to be followed.

Given that the issue is one of better UX on the specific cases where hints need to be provided
and only in the cases where there is ambiguity, the trade-off between operational complexity
and risk with the gain from the change leans on the direction of maintaining backwards
compatibility, as advocated by the proposal in this design document.

## Open questions
