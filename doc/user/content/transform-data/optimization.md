---
title: "Optimization"
description: "Recommendations for query optimization in Materialize."
menu:
  main:
    name: "Query optimization"
    parent: transform-data
    weight: 30
aliases:
  - /ops/speed-up/
  - /ops/optimization/
---

## Indexes

Like in any standard relational database, you can use [indexes](/get-started/key-concepts/#indexes) to optimize query performance in Materialize. Improvements can be significant, reducing some query times down to single-digit milliseconds.

Building an efficient index depends on the clauses used in your queries, as well as your expected access patterns. Use the following as a guide:

* [WHERE](#where-point-lookups)
* [JOIN](#join)
* [DEFAULT](#default-index)

`GROUP BY`, `ORDER BY` and `LIMIT` clauses currently do not benefit from an index.


### `WHERE` point lookups
Speed up a query involving a `WHERE` clause with equality comparisons to literals (e.g., `42`, or `'foo'`):

| Clause                                            | Index                                    |
|---------------------------------------------------|------------------------------------------|
| `WHERE x = 42`                                    | `CREATE INDEX ON obj_name (x);`        |
| `WHERE x IN (1, 2, 3)`                            | `CREATE INDEX ON obj_name (x);`        |
| `WHERE (x, y) IN ((1, 'a'), (7, 'b'), (8, 'c'))`  | `CREATE INDEX ON obj_name (x, y);`     |
| `WHERE x = 1 AND y = 'abc'`                       | `CREATE INDEX ON obj_name (x, y);`     |
| `WHERE (x = 5 AND y = 'a') OR (x = 7 AND y = ''`) | `CREATE INDEX ON obj_name (x, y);`     |
| `WHERE 2 * x = 64`                                | `CREATE INDEX ON obj_name (2 * x);`    |
| `WHERE upper(y) = 'HELLO'`                        | `CREATE INDEX ON obj_name (upper(y));` |

You can verify that Materialize is accessing the input by an index lookup using `EXPLAIN`. Check for `lookup_value` after the index name to confirm that an index lookup is happening, i.e., that Materialize is only reading the matching records from the index instead of scanning the entire index:
```sql
EXPLAIN SELECT * FROM foo WHERE x = 42 AND y = 'hello';
```
```
                               Optimized Plan
-----------------------------------------------------------------------------
 Explained Query (fast path):                                               +
   Project (#0, #1)                                                         +
     ReadExistingIndex materialize.public.foo_x_y lookup_value=(42, "hello")+
                                                                            +
 Used Indexes:                                                              +
   - materialize.public.foo_x_y (lookup)                                    +
```

#### Matching multi-column indexes to multi-column `WHERE` clauses

In general, your index key should exactly match the columns that are constrained in the `WHERE` clause. In more detail:
{{< warning >}} If the `WHERE` clause constrains fewer fields than your index key includes, then the index will not be used for a lookup, but will be fully scanned. For example, an index on `(x, y)` cannot be used to execute `WHERE x = 7` as a point lookup. {{< /warning >}}
- If the `WHERE` clause constrains more fields than your index key includes, then the index might still provide some speedup, but it won't necessarily be optimal: In this case, the index lookup is performed using only those constraints that are included in the index key, and the rest of the constraints will be used to subsequently filter the result of the index lookup.
- If `OR` is used and not all arguments constrain the same fields, create an index for the intersection of the constrained fields. For example, if you have `WHERE (x = 51 AND y = 'bbb') OR (x = 76 AND z = 9)`, create an index just on `x`.
- If `OR` is used and its arguments constrain completely disjoint sets of fields (e.g. `WHERE x = 5 OR y = 'aaa'`), try to rewrite your query using a `UNION` (or `UNION ALL`), where each argument of the `UNION` has one of the original `OR` arguments.

### `JOIN`

In general, you can [improve the performance of your joins](https://materialize.com/blog/maintaining-joins-using-few-resources)  by creating indexes on the columns occurring in join keys. This comes at the cost of additional memory usage. Materialize's in-memory [arrangements](/overview/arrangements) (the internal data structure of indexes) allow the system to share indexes across queries: **for multiple queries, an index is a fixed upfront cost with memory savings for each new query that uses it.**

Let's create a few tables to work through examples.

```sql
CREATE TABLE teachers (id INT, name TEXT);
CREATE TABLE sections (id INT, teacher_id INT, course_id INT, schedule TEXT);
CREATE TABLE courses (id INT, name TEXT);
```

#### Multiple Queries Join On the Same Collection

Let's consider two queries that join on a common collection. The idea is to create an index that can be shared across the two queries to save memory.

Here is a query where we join a collection `teachers` to a collection `sections` to see the name of the teacher, schedule, and course ID for a specific section of a course.

```sql
SELECT
    t.name,
    s.schedule,
    s.course_id
FROM teachers t
INNER JOIN sections s ON t.id = s.teacher_id;
```

Here is another query that also joins on `teachers.id`. This one counts the number of sections each teacher teaches.

```sql
SELECT
    t.id,
    t.name,
    count(*)
FROM teachers t
INNER JOIN sections s ON t.id = s.teacher_id
GROUP BY t.id, t.name;
```

We can eliminate redundant memory usage for these two queries by creating an index on the common column being joined, `teachers.id`.

```sql
CREATE INDEX pk_teachers ON teachers (id);
```

#### Joins with Filters

If your query filters one or more of the join inputs by a literal equality (e.g., `WHERE t.name = 'Escalante'`), place one of those input collections first in the `FROM` clause. In particular, this can speed up [ad hoc `SELECT` queries](/sql/select/#ad-hoc-queries) by accessing collections using index lookups rather than full scans.

Note that when the same input is being used in a join as well as being constrained by equalities to literals, _either_ the join _or_ the literal equalities can be sped up by an index (possibly the same index, but usually different indexes). Which of these will perform better depends on the characteristics of your data. For example, the following query can make use of _either_ of the following two indexes, but not both at the same time:
- on `teachers(name)` to perform the `t.name = 'Escalante'` point lookup before the join,
- on `teachers(id)` to speed up the join and then perform the `WHERE t.name = 'Escalante'`.

```sql
SELECT
    t.name,
    s.schedule,
    s.course_id
FROM teachers t
INNER JOIN sections s ON t.id = s.teacher_id
WHERE t.name = 'Escalante';
```

In this case, the index on `teachers(name)` might work better, as the `WHERE t.name = 'Escalante'` can filter out a very large percentage of the `teachers` table before the table is fed to the join. You can see an example `EXPLAIN` command output for the above query [here](#use-explain-to-verify-index-usage).

#### Optimize Multi-Way Joins with Delta Joins

Materialize has access to a join execution strategy we call `DeltaQuery`, a.k.a. **delta joins**, that aggressively re-uses indexes and maintains no intermediate results. Materialize considers this plan only if all the necessary indexes already exist, in which case the additional memory cost of the join is zero. This is typically possible when you index all the join keys.

From the previous example, add the name of the course rather than just the course ID.

```sql
CREATE VIEW course_schedule AS
  SELECT
      t.name AS teacher_name,
      s.schedule,
      c.name AS course_name
  FROM teachers t
  INNER JOIN sections s ON t.id = s.teacher_id
  INNER JOIN courses c ON c.id = s.course_id;
```

In this case, we create indexes on the join keys to optimize the query:

```sql
CREATE INDEX pk_teachers ON teachers (id);
CREATE INDEX sections_fk_teachers ON sections (teacher_id);
CREATE INDEX pk_courses ON courses (id);
CREATE INDEX sections_fk_courses ON sections (course_id);
```

```sql
EXPLAIN SELECT * FROM course_schedule;
```

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

For [ad hoc `SELECT` queries](/sql/select/#ad-hoc-queries) with a delta join, place the smallest input (taking into account predicates that filter from it) first in the `FROM` clause.

#### Further Optimize with Late Materialization

Materialize can further optimize memory usage when joining collections with primary and foreign key constraints using a pattern known as **late materialization**.

To understand late materialization, you need to know about primary and foreign keys. In our example, the `teachers.id` column uniquely identifies all teachers. When a column or set of columns uniquely identifies each record, it is called a **primary key**. We also have `sections.teacher_id`, which is not the primary key of `sections`, but it *does* correspond to the primary key of `teachers`. Whenever we have a column that is a primary key of another collection, it is called a [**foreign key**](https://en.wikipedia.org/wiki/Foreign_key).

In many relational databases, indexes don't replicate the entire collection of data. Rather, they maintain just a mapping from the indexed columns back to a primary key. These few columns can take substantially less space than the whole collection, and may also change less as various unrelated attributes are updated. This is called **late materialization**, and it is possible to achieve in Materialize as well. Here are the steps to implementing late materialization along with examples.

1. Create indexes on the primary key column(s) for your input collections.
    ```sql
    CREATE INDEX pk_teachers ON teachers (id);
    CREATE INDEX pk_sections ON sections (id);
    CREATE INDEX pk_courses ON courses (id);
    ```


2. For each foreign key in the join, create a "narrow" view with just two columns: foreign key and primary key. Then create two indexes: one for the foreign key and one for the primary key. In our example, the two foreign keys are `sections.teacher_id` and `sections.course_id`, so we do the following:
    ```sql
    -- Create a "narrow" view containing primary key sections.id
    -- and foreign key sections.teacher_id
    CREATE VIEW sections_narrow_teachers AS SELECT id, teacher_id FROM sections;
    -- Create indexes on those columns
    CREATE INDEX sections_narrow_teachers_0 ON sections_narrow_teachers (id);
    CREATE INDEX sections_narrow_teachers_1 ON sections_narrow_teachers (teacher_id);
    ```
    ```sql
    -- Create a "narrow" view containing primary key sections.id
    -- and foreign key sections.course_id
    CREATE VIEW sections_narrow_courses AS SELECT id, course_id FROM sections;
    -- Create indexes on those columns
    CREATE INDEX sections_narrow_courses_0 ON sections_narrow_courses (id);
    CREATE INDEX sections_narrow_courses_1 ON sections_narrow_courses (course_id);
    ```
    {{< note >}}
  In this case, because both foreign keys are in `sections`, we could have gotten away with one narrow collection `sections_narrow_teachers_and_courses` with indexes on `id`, `teacher_id`, and `course_id`. In general, we won't be so lucky to have all the foreign keys in the same collection, so we've shown the more general pattern of creating a narrow view and two indexes for each foreign key.
    {{</ note >}}

3. Rewrite your query to use your narrow collections in the join conditions. Example:

    ```sql
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

### Default index

Create a default index when there is no particular `WHERE` or `JOIN` clause that would fit the above cases. This can still speed up your query by reading the input from memory.

Clause                                               | Index                               |
-----------------------------------------------------|-------------------------------------|
`SELECT x, y FROM obj_name`                          | `CREATE DEFAULT INDEX ON obj_name;` |

### Use `EXPLAIN` to verify index usage

Use `EXPLAIN` to verify that indexes are used as you expect. For example:

```SQL
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

You can see in the above `EXPLAIN` printout that the system will use `teachers_name` for a point lookup, and use three other indexes for the execution of the delta join. Note that the `pk_teachers` index is not used, as explained [above](#joins-with-filters).

The following are the possible index usage types:
- `*** full scan ***`: Materialize will read the entire index.
- `lookup`: Materialize will look up only specific keys in the index.
- `differential join`: Materialize will use the index to perform a _differential join_. For a differential join between two relations, the amount of memory required is proportional to the sum of the sizes of each of the input relations that are **not** indexed. In other words, if an input is already indexed, then the size of that input won't affect the memory usage of a differential join between two relations. For a join between more than two relations, we recommend aiming for a delta join instead of a differential join, as explained [above](#optimize-multi-way-joins-with-delta-joins). A differential join between more than two relations will perform a series of binary differential joins on top of each other, and each of these binary joins (except the first one) will use memory proportional to the size of the intermediate data that is fed into the join.
- `delta join 1st input (full scan)`: Materialize will use the index for the first input of a [delta join](#optimize-multi-way-joins-with-delta-joins). Note that the first input of a delta join is always fully scanned. However, executing the join won't require additional memory if the input is indexed.
- `delta join lookup`: Materialize will use the index for a non-first input of a [delta join](#optimize-multi-way-joins-with-delta-joins). This means that, in an ad hoc query, the join will perform only lookups into the index.
- `fast path limit`: When a [fast path](/sql/explain-plan/#fast-path-queries) query has a `LIMIT` clause but no `ORDER BY` clause, then Materialize will read from the index only as many records as required to satisfy the `LIMIT` (plus `OFFSET`) clause.

## Query hints

Materialize has at present three important [query hints]: `AGGREGATE INPUT GROUP SIZE`, `DISTINCT ON INPUT GROUP SIZE`, and `LIMIT INPUT GROUP SIZE`. These hints apply to indexed or materialized views that need to incrementally maintain [`MIN`], [`MAX`], or [Top K] queries, as specified by SQL aggregations, `DISTINCT ON`, or `LIMIT` clauses. Maintaining these queries while delivering low latency result updates is demanding in terms of main memory. This is because Materialize builds a hierarchy of aggregations so that data can be physically partitioned into small groups. By having only small groups at each level of the hierarchy, we can make sure that recomputing aggregations is not slowed down by skew in the sizes of the original query groups.

The number of levels needed in the hierarchical scheme is by default set assuming that there may be large query groups in the input data. By specifying the query hints, it is possible to refine this assumption, allowing Materialize to build a hierarchy with fewer levels and lower memory consumption without sacrificing update latency.

Consider the previous example with the collection `sections`. Maintenance of the maximum `course_id` per `teacher` can be achieved with a materialized view:

```sql
CREATE MATERIALIZED VIEW max_course_id_per_teacher AS
SELECT teacher_id, MAX(course_id)
FROM sections
GROUP BY teacher_id;
```

If the largest number of `course_id` values that are allocated to a single `teacher_id` is known, then this number can be provided as the `AGGREGATE INPUT GROUP SIZE`. For the query above, it is possible to get an estimate for this number by:

```sql
SELECT MAX(course_count)
FROM (
  SELECT teacher_id, COUNT(*) course_count
  FROM sections
  GROUP BY teacher_id
);
```

However, the estimate is based only on data that is already present in the system. So taking into account how much this largest number could expand is critical to avoid issues with update latency after tuning the query hint.

For our example, let's suppose that we determined the largest number of courses per teacher to be `1000`. Then, the original definition of `max_course_id_per_teacher` can be revised to include the `AGGREGATE INPUT GROUP SIZE` query hint as follows:

```sql
CREATE MATERIALIZED VIEW max_course_id_per_teacher AS
SELECT teacher_id, MAX(course_id)
FROM sections
GROUP BY teacher_id
OPTIONS (AGGREGATE INPUT GROUP SIZE = 1000)
```

The other two hints can be provided in [Top K] query patterns specified by `DISTINCT ON` or `LIMIT`. As examples, consider that we wish not to compute the maximum `course_id`, but rather the `id` of the section of this top course. This computation can be incrementally maintained by the following materialized view:

```sql
CREATE MATERIALIZED VIEW section_of_top_course_per_teacher AS
SELECT DISTINCT ON(teacher_id) teacher_id, id AS section_id
FROM sections
OPTIONS (DISTINCT ON INPUT GROUP SIZE = 1000)
ORDER BY teacher_id ASC, course_id DESC;
```

In the above examples, we see that the query hints are always positioned in an `OPTIONS` clause after a `GROUP BY` clause, but before an `ORDER BY`, as captured by the [`SELECT` syntax]. However, in the case of Top K using a `LATERAL` subquery and `LIMIT`, it is important to note that the hint is specified in the subquery. For instance, the following materialized view illustrates how to incrementally maintain the top-3 section `id`s ranked by `course_id` for each teacher:

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

For indexed and materialized views that have already been created without specifying query hints, Materialize includes an introspection view, [`mz_internal.mz_expected_group_size_advice`], that can be used to query, for a given cluster, all incrementally maintained [dataflows] where tuning of the above query hints could be beneficial. The introspection view also provides an advice value based on an estimate of how many levels could be cut from the hierarchy. The following query illustrates how to access this introspection view:

```sql
SELECT dataflow_name, region_name, levels, to_cut, hint
FROM mz_internal.mz_expected_group_size_advice
ORDER BY dataflow_name, region_name;
```

The column `hint` provides the estimated value to be provided to the `AGGREGATE INPUT GROUP SIZE` in the case of a `MIN` or `MAX` aggregation or to the `DISTINCT ON INPUT GROUP SIZE` or `LIMIT INPUT GROUP SIZE` in the case of a Top K pattern.

## Learn more

Check out the blog post [Delta Joins and Late Materialization](https://materialize.com/blog/delta-joins/) to go deeper on join optimization in Materialize.

[query hints]: /sql/select/#query-hints
[arrangements]: /get-started/arrangements/#arrangements
[`MIN`]: /sql/functions/#min
[`MAX`]: /sql/functions/#max
[Top K]: /transform-data/patterns/top-k
[`mz_internal.mz_expected_group_size_advice`]: /sql/system-catalog/mz_internal/#mz_expected_group_size_advice
[dataflows]: /get-started/arrangements/#dataflows
[`SELECT` syntax]: /sql/select/#syntax
