<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Overview](/docs/self-managed/v25.2/transform-data/)

</div>

# Optimization

## Indexes

Indexes in Materialize maintain the complete up-to-date query results in
memory (and not just the index keys and the pointers to data rows).
Unlike some other databases, Materialize can use an index to serve query
results even if the query does not specify a `WHERE` condition on the
index keys. Serving queries from an index is fast since the results are
already up-to-date and in memory.

Materialize can use
[indexes](/docs/self-managed/v25.2/concepts/indexes/) to further
optimize query performance in Materialize. Improvements can be
significant, reducing some query times down to single-digit
milliseconds.

Building an efficient index depends on the clauses used in your queries
as well as your expected access patterns. Use the following as a guide:

- [WHERE point lookups](#where-point-lookups)
- [JOIN](#join)
- [DEFAULT](#default-index)

### `WHERE` point lookups

Unlike some other databases, Materialize can use an index to serve query
results even if the query does not specify a `WHERE` condition on the
index keys. For some queries, Materialize can perform [**point
lookups**](/docs/self-managed/v25.2/concepts/indexes/#point-lookups) on
the index (as opposed to an index scan) if the query’s `WHERE` clause:

- Specifies equality (`=` or `IN`) condition on **all** the indexed
  fields. The equality conditions must specify the **exact** index key
  expression (including type).

- Only uses `AND` (conjunction) to combine conditions for **different**
  fields.

Depending on your query pattern, you may want to build indexes to
support point lookups.

#### Create an index to support point lookups

To [create an index](/docs/self-managed/v25.2/sql/create-index/) to
support [**point
lookups**](/docs/self-managed/v25.2/concepts/indexes/#point-lookups):

<div class="highlight">

``` chroma
CREATE INDEX ON obj_name (<keys>);
```

</div>

- Specify **only** the keys that are constrained in the query’s `WHERE`
  clause. If your index contains keys not specified in the query’s
  `WHERE` clause, then Materialize performs a full index scan.

- Specify all (or a subset of) keys that are constrained in the query
  pattern’s `WHERE` clause. If the index specifies all the keys,
  Materialize performs a point lookup only. If the index specifies a
  subset of keys, then Materialize performs a point lookup on the index
  keys and then filters these results using the conditions on the
  non-indexed fields.

- Specify index keys that **exactly match** the column expressions in
  the `WHERE` clause. For example, if the query specifies
  `WHERE quantity * price = 100`, the index key should be
  `quantity * price` and not `price * quantity`.

- If the `WHERE` uses `OR` clauses and:

  - The `OR` arguments constrain all the same fields (e.g.,
    `WHERE (quantity = 5 AND price = 1.25) OR (quantity = 10 AND price = 1.25)`),
    create an index for the constrained fields (e.g., `quantity` and
    `price`).

  - The `OR` arguments constrain some of the same fields (e.g.,
    `WHERE (quantity = 5 AND price = 1.25) OR (quantity = 10 AND item = 'brownie)`),
    create an index for the intersection of the constrained fields
    (e.g., `quantity`). Materialize performs a point lookup on the
    indexed key and then filters the results using the conditions on the
    non-indexed fields.

  - The `OR` arguments constrain completely disjoint sets of fields
    (e.g., `WHERE quantity = 5 OR item = 'brownie'`), try to rewrite
    your query using a `UNION` (or `UNION ALL`), where each argument of
    the `UNION` has one of the original `OR` arguments.

    For example, the query can be rewritten as:

    <div class="highlight">

    ``` chroma
    SELECT * FROM orders_view WHERE quantity = 5
    UNION
    SELECT * FROM orders_view WHERE item = 'brownie';
    ```

    </div>

    Depending on your usage pattern, you may want point-lookup indexes
    on both `quantity` and `item` (i.e., create two indexes, one on
    `quantity` and one on `item`). However, since each index will hold a
    copy of the data, consider the tradeoff between speed and memory
    usage. If the memory impact of having both indexes is too high, you
    might want to take a more global look at all of your queries to
    determine which index to build.

#### Examples

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>WHERE clause of your query patterns</th>
<th>Index for point lookups</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>WHERE x = 42</code></td>
<td><code>CREATE INDEX ON obj_name (x);</code></td>
</tr>
<tr>
<td><code>WHERE x IN (1, 2, 3)</code></td>
<td><code>CREATE INDEX ON obj_name (x);</code></td>
</tr>
<tr>
<td><code>WHERE x = 1 OR x = 2</code></td>
<td><code>CREATE INDEX ON obj_name (x);</code></td>
</tr>
<tr>
<td><code>WHERE (x, y) IN ((1, 'a'), (7, 'b'), (8, 'c'))</code></td>
<td><code>CREATE INDEX ON obj_name (x, y);</code> or<br />
<code>CREATE INDEX ON obj_name (y, x);</code></td>
</tr>
<tr>
<td><code>WHERE x = 1 AND y = 'abc'</code></td>
<td><code>CREATE INDEX ON obj_name (x, y);</code> or<br />
<code>CREATE INDEX ON obj_name (y, x);</code></td>
</tr>
<tr>
<td><code>WHERE (x = 5 AND y = 'a') OR (x = 7 AND y = ''</code>)</td>
<td><code>CREATE INDEX ON obj_name (x, y);</code> or<br />
<code>CREATE INDEX ON obj_name (y, x);</code></td>
</tr>
<tr>
<td><code>WHERE y * x = 64</code></td>
<td><code>CREATE INDEX ON obj_name (y * x);</code></td>
</tr>
<tr>
<td><code>WHERE upper(y) = 'HELLO'</code></td>
<td><code>CREATE INDEX ON obj_name (upper(y));</code></td>
</tr>
</tbody>
</table>

You can verify that Materialize is accessing the input by an index
lookup using [`EXPLAIN`](/docs/self-managed/v25.2/sql/explain-plan/).

<div class="highlight">

``` chroma
CREATE INDEX ON foo (x, y);
EXPLAIN SELECT * FROM foo WHERE x = 42 AND y = 50;
```

</div>

In the [`EXPLAIN`](/docs/self-managed/v25.2/sql/explain-plan/) output,
check for `lookup_value` after the index name to confirm that
Materialize will use a point lookup; i.e., that Materialize will only
read the matching records from the index instead of scanning the entire
index:

```
 Explained Query (fast path):
   Project (#0{x}, #1{y})
     ReadIndex on=materialize.public.foo foo_x_y_idx=[lookup value=(42, 50)]

 Used Indexes:
   - materialize.public.foo_x_y_idx (lookup)
```

### `JOIN`

In general, you can [improve the performance of your
joins](https://materialize.com/blog/maintaining-joins-using-few-resources)
by creating indexes on the columns occurring in join keys. (When a
relation is joined with different relations on different keys, then
separate indexes should be created for these keys.) This comes at the
cost of additional memory usage. Materialize’s in-memory
[arrangements](/docs/self-managed/v25.2/overview/arrangements) (the
internal data structure of indexes) allow the system to share indexes
across queries: **for multiple queries, an index is a fixed upfront cost
with memory savings for each new query that uses it.**

Let’s create a few tables to work through examples.

<div class="highlight">

``` chroma
CREATE TABLE teachers (id INT, name TEXT);
CREATE TABLE sections (id INT, teacher_id INT, course_id INT, schedule TEXT);
CREATE TABLE courses (id INT, name TEXT);
```

</div>

#### Multiple Queries Join On the Same Collection

Let’s consider two queries that join on a common collection. The idea is
to create an index that can be shared across the two queries to save
memory.

Here is a query where we join a collection `teachers` to a collection
`sections` to see the name of the teacher, schedule, and course ID for a
specific section of a course.

<div class="highlight">

``` chroma
SELECT
    t.name,
    s.schedule,
    s.course_id
FROM teachers t
INNER JOIN sections s ON t.id = s.teacher_id;
```

</div>

Here is another query that also joins on `teachers.id`. This one counts
the number of sections each teacher teaches.

<div class="highlight">

``` chroma
SELECT
    t.id,
    t.name,
    count(*)
FROM teachers t
INNER JOIN sections s ON t.id = s.teacher_id
GROUP BY t.id, t.name;
```

</div>

We can eliminate redundant memory usage for these two queries by
creating an index on the common column being joined, `teachers.id`.

<div class="highlight">

``` chroma
CREATE INDEX pk_teachers ON teachers (id);
```

</div>

#### Joins with Filters

If your query filters one or more of the join inputs by a literal
equality (e.g., `WHERE t.name = 'Escalante'`), place one of those input
collections first in the `FROM` clause. In particular, this can speed up
[ad hoc `SELECT`
queries](/docs/self-managed/v25.2/sql/select/#ad-hoc-queries) by
accessing collections using index lookups rather than full scans.

Note that when the same input is being used in a join as well as being
constrained by equalities to literals, *either* the join *or* the
literal equalities can be sped up by an index (possibly the same index,
but usually different indexes). Which of these will perform better
depends on the characteristics of your data. For example, the following
query can make use of *either* of the following two indexes, but not
both at the same time:

- on `teachers(name)` to perform the `t.name = 'Escalante'` point lookup
  before the join,
- on `teachers(id)` to speed up the join and then perform the
  `WHERE t.name = 'Escalante'`.

<div class="highlight">

``` chroma
SELECT
    t.name,
    s.schedule,
    s.course_id
FROM teachers t
INNER JOIN sections s ON t.id = s.teacher_id
WHERE t.name = 'Escalante';
```

</div>

In this case, the index on `teachers(name)` might work better, as the
`WHERE t.name = 'Escalante'` can filter out a very large percentage of
the `teachers` table before the table is fed to the join. You can see an
example `EXPLAIN` command output for the above query
[here](#use-explain-to-verify-index-usage).

#### Optimize Multi-Way Joins with Delta Joins

Materialize has access to a join execution strategy we call **delta
joins**, which aggressively re-uses indexes and maintains no
intermediate results in memory. Materialize considers this plan only if
all the necessary indexes already exist, in which case the additional
memory cost of the join is zero. This is typically possible when you
index all the join keys (including primary keys and foreign keys that
are involved in the join). Delta joins are relevant only for joins of
more than 2 inputs.

Let us extend the previous example by also querying for the name of the
course rather than just the course ID, needing a 3-input join.

<div class="highlight">

``` chroma
CREATE VIEW course_schedule AS
  SELECT
      t.name AS teacher_name,
      s.schedule,
      c.name AS course_name
  FROM teachers t
  INNER JOIN sections s ON t.id = s.teacher_id
  INNER JOIN courses c ON c.id = s.course_id;
```

</div>

In this case, we create indexes on the join keys to optimize the query:

<div class="highlight">

``` chroma
CREATE INDEX pk_teachers ON teachers (id);
CREATE INDEX sections_fk_teachers ON sections (teacher_id);
CREATE INDEX pk_courses ON courses (id);
CREATE INDEX sections_fk_courses ON sections (course_id);
```

</div>

<div class="highlight">

``` chroma
EXPLAIN SELECT * FROM course_schedule;
```

</div>

```
Optimized Plan
Explained Query:
  Project (#1, #5, #7)
    Filter (#0) IS NOT NULL AND (#4) IS NOT NULL
      Join on=(#0 = #3 AND #4 = #6) type=delta                 <---------- Delta join
        ArrangeBy keys=[[#0]]
          ReadIndex on=teachers pk_teachers=[delta join 1st input (full scan)]
        ArrangeBy keys=[[#1], [#2]]
          ReadIndex on=sections sections_fk_teachers=[delta join lookup] sections_fk_courses=[delta join lookup]
        ArrangeBy keys=[[#0]]
          ReadIndex on=courses pk_courses=[delta join lookup]

Used Indexes:
  - materialize.public.pk_teachers (delta join 1st input (full scan))
  - materialize.public.sections_fk_teachers (delta join lookup)
  - materialize.public.pk_courses (delta join lookup)
  - materialize.public.sections_fk_courses (delta join lookup)
```

For [ad hoc `SELECT`
queries](/docs/self-managed/v25.2/sql/select/#ad-hoc-queries) with a
delta join, place the smallest input (taking into account predicates
that filter from it) first in the `FROM` clause. (This is only relevant
for joins with more than two inputs, because two-input joins are always
Differential joins.)

It is important to note that often more than one index is needed on a
single input of a multi-way join. In the above example, `sections` needs
an index on the `teacher_id` column and another index on the `course_id`
column. Generally, when a relation is joined with different relations on
different keys, then separate indexes should be created for each of
these keys.

#### Further Optimize with Late Materialization

Materialize can further optimize memory usage when joining collections
with primary and foreign key constraints using a pattern known as **late
materialization**.

To understand late materialization, you need to know about primary and
foreign keys. In our example, the `teachers.id` column uniquely
identifies all teachers. When a column or set of columns uniquely
identifies each record, it is called a **primary key**. We also have
`sections.teacher_id`, which is not the primary key of `sections`, but
it *does* correspond to the primary key of `teachers`. Whenever we have
a column that is a primary key of another collection, it is called a
[**foreign key**](https://en.wikipedia.org/wiki/Foreign_key).

In many relational databases, indexes don’t replicate the entire
collection of data. Rather, they maintain just a mapping from the
indexed columns back to a primary key. These few columns can take
substantially less space than the whole collection, and may also change
less as various unrelated attributes are updated. This is called **late
materialization**, and it is possible to achieve in Materialize as well.
Here are the steps to implementing late materialization along with
examples.

1.  Create indexes on the primary key column(s) for your input
    collections.

    <div class="highlight">

    ``` chroma
    CREATE INDEX pk_teachers ON teachers (id);
    CREATE INDEX pk_sections ON sections (id);
    CREATE INDEX pk_courses ON courses (id);
    ```

    </div>

2.  For each foreign key in the join, create a “narrow” view with just
    two columns: foreign key and primary key. Then create two indexes:
    one for the foreign key and one for the primary key. In our example,
    the two foreign keys are `sections.teacher_id` and
    `sections.course_id`, so we do the following:

    <div class="highlight">

    ``` chroma
    -- Create a "narrow" view containing primary key sections.id
    -- and foreign key sections.teacher_id
    CREATE VIEW sections_narrow_teachers AS SELECT id, teacher_id FROM sections;
    -- Create indexes on those columns
    CREATE INDEX sections_narrow_teachers_0 ON sections_narrow_teachers (id);
    CREATE INDEX sections_narrow_teachers_1 ON sections_narrow_teachers (teacher_id);
    ```

    </div>

    <div class="highlight">

    ``` chroma
    -- Create a "narrow" view containing primary key sections.id
    -- and foreign key sections.course_id
    CREATE VIEW sections_narrow_courses AS SELECT id, course_id FROM sections;
    -- Create indexes on those columns
    CREATE INDEX sections_narrow_courses_0 ON sections_narrow_courses (id);
    CREATE INDEX sections_narrow_courses_1 ON sections_narrow_courses (course_id);
    ```

    </div>

    <div class="note">

    **NOTE:** In this case, because both foreign keys are in `sections`,
    we could have gotten away with one narrow collection
    `sections_narrow_teachers_and_courses` with indexes on `id`,
    `teacher_id`, and `course_id`. In general, we won’t be so lucky to
    have all the foreign keys in the same collection, so we’ve shown the
    more general pattern of creating a narrow view and two indexes for
    each foreign key.

    </div>

3.  Rewrite your query to use your narrow collections in the join
    conditions. Example:

    <div class="highlight">

    ``` chroma
    SELECT
      t.name AS teacher_name,
      s.schedule,
      c.name AS course_name
    FROM sections_narrow_teachers s_t
    INNER JOIN sections s ON s_t.id = s.id
    INNER JOIN teachers t ON s_t.teacher_id = t.id
    INNER JOIN sections_narrow_courses s_c ON s_c.id = s.id
    INNER JOIN courses c ON s_c.course_id = c.id;
    ```

    </div>

### Default index

Create a default index when there is no particular `WHERE` or `JOIN`
clause that would fit the above cases. This can still speed up your
query by reading the input from memory.

| Clause                      | Index                               |
|-----------------------------|-------------------------------------|
| `SELECT x, y FROM obj_name` | `CREATE DEFAULT INDEX ON obj_name;` |

### Use `EXPLAIN` to verify index usage

Use `EXPLAIN` to verify that indexes are used as you expect. For
example:

<div class="highlight">

``` chroma
CREATE TABLE teachers (id INT, name TEXT);
CREATE TABLE sections (id INT, teacher_id INT, course_id INT, schedule TEXT);
CREATE TABLE courses (id INT, name TEXT);

CREATE INDEX pk_teachers ON teachers (id);
CREATE INDEX teachers_name ON teachers (name);
CREATE INDEX sections_fk_teachers ON sections (teacher_id);
CREATE INDEX pk_courses ON courses (id);
CREATE INDEX sections_fk_courses ON sections (course_id);

EXPLAIN
  SELECT
      t.name AS teacher_name,
      s.schedule,
      c.name AS course_name
  FROM teachers t
  INNER JOIN sections s ON t.id = s.teacher_id
  INNER JOIN courses c ON c.id = s.course_id
  WHERE t.name = 'Escalante';
```

</div>

```
                                                  Optimized Plan
------------------------------------------------------------------------------------------------------------------
 Explained Query:                                                                                                +
   Project (#1, #6, #8)                                                                                          +
     Filter (#0) IS NOT NULL AND (#5) IS NOT NULL                                                                +
       Join on=(#0 = #4 AND #5 = #7) type=delta                                                                  +
         ArrangeBy keys=[[#0]]                                                                                   +
           ReadIndex on=materialize.public.teachers teachers_name=[lookup value=("Escalante")]                   +
         ArrangeBy keys=[[#1], [#2]]                                                                             +
           ReadIndex on=sections sections_fk_teachers=[delta join lookup] sections_fk_courses=[delta join lookup]+
         ArrangeBy keys=[[#0]]                                                                                   +
           ReadIndex on=courses pk_courses=[delta join lookup]                                                   +
                                                                                                                 +
 Used Indexes:                                                                                                   +
   - materialize.public.teachers_name (lookup)                                                                   +
   - materialize.public.sections_fk_teachers (delta join lookup)                                                 +
   - materialize.public.pk_courses (delta join lookup)                                                           +
   - materialize.public.sections_fk_courses (delta join lookup)                                                  +
```

You can see in the above `EXPLAIN` printout that the system will use
`teachers_name` for a point lookup, and use three other indexes for the
execution of the delta join. Note that the `pk_teachers` index is not
used, as explained [above](#joins-with-filters).

The following are the possible index usage types:

- `*** full scan ***`: Materialize will read the entire index.
- `lookup`: Materialize will look up only specific keys in the index.
- `differential join`: Materialize will use the index to perform a
  *differential join*. For a differential join between two relations,
  the amount of memory required is proportional to the sum of the sizes
  of each of the input relations that are **not** indexed. In other
  words, if an input is already indexed, then the size of that input
  won’t affect the memory usage of a differential join between two
  relations. For a join between more than two relations, we recommend
  aiming for a delta join instead of a differential join, as explained
  [above](#optimize-multi-way-joins-with-delta-joins). A differential
  join between more than two relations will perform a series of binary
  differential joins on top of each other, and each of these binary
  joins (except the first one) will use memory proportional to the size
  of the intermediate data that is fed into the join.
- `delta join 1st input (full scan)`: Materialize will use the index for
  the first input of a [delta
  join](#optimize-multi-way-joins-with-delta-joins). Note that the first
  input of a delta join is always fully scanned. However, executing the
  join won’t require additional memory if the input is indexed.
- `delta join lookup`: Materialize will use the index for a non-first
  input of a [delta join](#optimize-multi-way-joins-with-delta-joins).
  This means that, in an ad hoc query, the join will perform only
  lookups into the index.
- `fast path limit`: When a [fast
  path](/docs/self-managed/v25.2/sql/explain-plan/#fast-path-queries)
  query has a `LIMIT` clause but no `ORDER BY` clause, then Materialize
  will read from the index only as many records as required to satisfy
  the `LIMIT` (plus `OFFSET`) clause.

### Limitations

Indexes in Materialize do not order their keys using the data type’s
natural ordering and instead orders by its internal representation of
the key (the tuple of key length and value).

As such, indexes in Materialize currently do not provide optimizations
for:

- Range queries; that is queries using `>`, `>=`, `<`, `<=`, `BETWEEN`
  clauses (e.g., `WHERE quantity > 10`, `price >= 10 AND price <= 50`,
  and `WHERE quantity BETWEEN 10 AND 20`).

- `GROUP BY`, `ORDER BY` and `LIMIT` clauses.

## Query hints

Materialize has at present three important [query
hints](/docs/self-managed/v25.2/sql/select/#query-hints):
`AGGREGATE INPUT GROUP SIZE`, `DISTINCT ON INPUT GROUP SIZE`, and
`LIMIT INPUT GROUP SIZE`. These hints apply to indexed or materialized
views that need to incrementally maintain
[`MIN`](/docs/self-managed/v25.2/sql/functions/#min),
[`MAX`](/docs/self-managed/v25.2/sql/functions/#max), or [Top
K](/docs/self-managed/v25.2/transform-data/patterns/top-k) queries, as
specified by SQL aggregations, `DISTINCT ON`, or `LIMIT` clauses.
Maintaining these queries while delivering low latency result updates is
demanding in terms of main memory. This is because Materialize builds a
hierarchy of aggregations so that data can be physically partitioned
into small groups. By having only small groups at each level of the
hierarchy, we can make sure that recomputing aggregations is not slowed
down by skew in the sizes of the original query groups.

The number of levels needed in the hierarchical scheme is by default set
assuming that there may be large query groups in the input data. By
specifying the query hints, it is possible to refine this assumption,
allowing Materialize to build a hierarchy with fewer levels and lower
memory consumption without sacrificing update latency.

Consider the previous example with the collection `sections`.
Maintenance of the maximum `course_id` per `teacher` can be achieved
with a materialized view:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW max_course_id_per_teacher AS
SELECT teacher_id, MAX(course_id)
FROM sections
GROUP BY teacher_id;
```

</div>

If the largest number of `course_id` values that are allocated to a
single `teacher_id` is known, then this number can be provided as the
`AGGREGATE INPUT GROUP SIZE`. For the query above, it is possible to get
an estimate for this number by:

<div class="highlight">

``` chroma
SELECT MAX(course_count)
FROM (
  SELECT teacher_id, COUNT(*) course_count
  FROM sections
  GROUP BY teacher_id
);
```

</div>

However, the estimate is based only on data that is already present in
the system. So taking into account how much this largest number could
expand is critical to avoid issues with update latency after tuning the
query hint.

For our example, let’s suppose that we determined the largest number of
courses per teacher to be `1000`. Then, the original definition of
`max_course_id_per_teacher` can be revised to include the
`AGGREGATE INPUT GROUP SIZE` query hint as follows:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW max_course_id_per_teacher AS
SELECT teacher_id, MAX(course_id)
FROM sections
GROUP BY teacher_id
OPTIONS (AGGREGATE INPUT GROUP SIZE = 1000)
```

</div>

The other two hints can be provided in [Top
K](/docs/self-managed/v25.2/transform-data/patterns/top-k) query
patterns specified by `DISTINCT ON` or `LIMIT`. As examples, consider
that we wish not to compute the maximum `course_id`, but rather the `id`
of the section of this top course. This computation can be incrementally
maintained by the following materialized view:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW section_of_top_course_per_teacher AS
SELECT DISTINCT ON(teacher_id) teacher_id, id AS section_id
FROM sections
OPTIONS (DISTINCT ON INPUT GROUP SIZE = 1000)
ORDER BY teacher_id ASC, course_id DESC;
```

</div>

In the above examples, we see that the query hints are always positioned
in an `OPTIONS` clause after a `GROUP BY` clause, but before an
`ORDER BY`, as captured by the [`SELECT`
syntax](/docs/self-managed/v25.2/sql/select/#syntax). However, in the
case of Top K using a `LATERAL` subquery and `LIMIT`, it is important to
note that the hint is specified in the subquery. For instance, the
following materialized view illustrates how to incrementally maintain
the top-3 section `id`s ranked by `course_id` for each teacher:

<div class="highlight">

``` chroma
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

</div>

For indexed and materialized views that have already been created
without specifying query hints, Materialize includes an introspection
view,
[`mz_introspection.mz_expected_group_size_advice`](/docs/self-managed/v25.2/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice),
that can be used to query, for a given cluster, all incrementally
maintained
[dataflows](/docs/self-managed/v25.2/get-started/arrangements/#dataflows)
where tuning of the above query hints could be beneficial. The
introspection view also provides an advice value based on an estimate of
how many levels could be cut from the hierarchy. The following query
illustrates how to access this introspection view:

<div class="highlight">

``` chroma
SELECT dataflow_name, region_name, levels, to_cut, hint
FROM mz_introspection.mz_expected_group_size_advice
ORDER BY dataflow_name, region_name;
```

</div>

The column `hint` provides the estimated value to be provided to the
`AGGREGATE INPUT GROUP SIZE` in the case of a `MIN` or `MAX` aggregation
or to the `DISTINCT ON INPUT GROUP SIZE` or `LIMIT INPUT GROUP SIZE` in
the case of a Top K pattern.

## Learn more

Check out the blog post [Delta Joins and Late
Materialization](https://materialize.com/blog/delta-joins/) to go deeper
on join optimization in Materialize.

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/transform-data/optimization.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
