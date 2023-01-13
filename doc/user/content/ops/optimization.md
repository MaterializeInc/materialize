---
title: "Optimization"
description: "Recommendations for query optimization in Materialize."
menu:
  main:
    parent: ops
    weight: 70
aliases:
  - /ops/speed-up/
---

## Indexes

Like in any standard relational database, you can use [indexes](/overview/key-concepts/#indexes) to optimize query performance in Materialize. Improvements can be significant, reducing some query times down to single-digit milliseconds.

Building an efficient index depends on the clauses used in your queries, as well as your expected access patterns. Use the following as a guide:

* [WHERE](#where)
* [JOIN](#join)
* [DEFAULT](#default)

`GROUP BY`, `ORDER BY` and `LIMIT` clauses currently do not benefit from an index.


### `WHERE`
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

You can verify that Materialize is accessing the input by an index lookup using `EXPLAIN`. Check for `lookup_value` after the index name to confirm that an index lookup is happening, i.e., that Materialize is only reading the matching elements of the index instead of scanning the entire index:
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
   - materialize.public.foo_x_y                                             +
```

#### Matching multi-column indexes to multi-column `WHERE` clauses

In general, your index key should exactly match the columns that are constrained in the `WHERE` clause. In more detail:
- If the `WHERE` clause constrains fewer fields than your index key includes, then **the index will not be used**. For example, an index on `(x, y)` cannot be used to speed up `WHERE x = 7`.
- If the `WHERE` clause constrains more fields than your index key includes, then the index might still provide some speedup, but it won't necessarily be optimal: In this case, the index lookup is performed using only those constraints that are included in the index key, and the rest of the constraints will be used to subsequently filter the result of the index lookup.
- If `OR` is used and not all arguments constrain the same fields, create an index for the intersection of the constrained fields. For example, if you have `WHERE (x = 51 AND y = 'bbb') OR (x = 76 AND z = 9)`, create an index just on `x`.
- If `OR` is used and its arguments constrain completely disjoint sets of fields (e.g. `WHERE x = 5 OR y = 'aaa'`), try to rewrite your query using a `UNION` (or `UNION ALL`), where each argument of the `UNION` has one of the original `OR` arguments.

### `JOIN`

In general, you can improve the performance of your joins by creating indexes on the columns being joined. This comes at the cost of additional memory usage. Fortunately, Materialize can reuse these indexes for different queries, which means **for multiple queries, indexes are a fixed upfront cost with memory savings for each new query.**

Let's create a few tables to work through examples.

```sql
CREATE TABLE teachers (id INT, name TEXT);
CREATE TABLE sections (id INT, teacher_id INT, course_id INT, schedule TEXT);
CREATE TABLE courses (id INT, name TEXT);
```

#### Joining Two Collections

Here is an example where we join a collection `teachers` to a collection `sections` to see the name of the teacher, schedule, and course ID for a specific section of a course.

```sql
SELECT
    t.name,
    s.schedule,
    s.course_id
FROM teachers t
INNER JOIN sections s ON (t.id = s.teacher_id);
```

We can optimize this query by creating an index for each column being joined.

```sql
CREATE INDEX teachers_id_index ON teachers (id);
CREATE INDEX sections_teacher_id_index ON sections (teacher_id);
```



#### Optimize Multi-Way Joins with Delta Joins

Materialize has access to a join execution strategy we call `DeltaQuery`, a.k.a. **delta joins**, that aggressively re-uses indexes and maintains **_zero_** intermediate results. Materialize considers this plan only if all the necessary indexes already exist, in which case the **_additional_** memory cost of the join is **zero**. This is typically possible when you **index all the join keys**.

From the previous example, suppose we want to add the name of the course rather than just the course ID. We will create a view `course_schedule` to reflect this:

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
EXPLAIN VIEW course_schedule;
```

```
                  Optimized Plan                  
--------------------------------------------------
 Explained Query:                                +
   Project (#1, #5, #7)                          +
     Filter (#0) IS NOT NULL                     +
       Join on=(#0 = #3 AND #4 = #6) type=delta  + <--- using delta join
         ArrangeBy keys=[[#0]]                   +
           Get materialize.public.teachers       +
         ArrangeBy keys=[[#1], [#2]]             +
           Get materialize.public.sections       +
         ArrangeBy keys=[[#0]]                   +
           Filter (#0) IS NOT NULL               +
             Get materialize.public.courses      +
                                                 +
 Source materialize.public.courses               +
   filter=((#0) IS NOT NULL)                     +
                                                 +
 Used Indexes:                                   +
   - materialize.public.pk_teachers              +
   - materialize.public.sections_fk_teachers     +
   - materialize.public.sections_fk_courses      +
   - materialize.public.pk_courses               +
```

If your query filters one or more of the join inputs by a literal equality (e.g., `WHERE t.name = 'Escalante'`), place one of those input collections first in the `FROM` clause. In particular, this can speed up [ad hoc `SELECT` queries](/sql/select/#ad-hoc-queries) by accessing collections using index lookups rather than full scans.

#### Further Optimize with Late Materialization

Materialize can further optimize memory usage when joining collections with primary and foreign key constraints using a pattern known as **late materialization**.

To understand late materialization, you need to know about primary and secondary indexes. In our example, the `teachers.id` column uniquely identifies all teachers. When a column or set of columns uniquely identifies each record, it is called a **primary key**, and an index on the primary key is called a **primary index**. We also have `sections.teacher_id`, which is not the primary key of `sections`, but it *does* correspond to the primary key of `teachers`. Whenever we have a column that is a primary key of another collection, it is called a [foreign key](https://en.wikipedia.org/wiki/Foreign_key). When we create an index on a foreign key, it's called a **secondary index**.

In many relational databases, indexes don't replicate the entire collection of data. Rather, they maintain just a mapping from the indexed columns back to a primary key. These few columns can take substantially less space than the whole collection, and may also change less as various unrelated attributes are updated. This is called **late materialization**, and it is possible to achieve in Materialize as well. Here are the steps to implementing late materialization along with examples.

1. Create primary indexes for your input collections.
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
    -- Create primary and secondary indexes
    CREATE INDEX sections_narrow_teachers_0 ON sections_narrow_teachers (id);
    CREATE INDEX sections_narrow_teachers_1 ON sections_narrow_teachers (teacher_id);
    ```
    ```sql
    -- Create a "narrow" view containing primary key sections.id
    -- and foreign key sections.course_id
    CREATE VIEW sections_narrow_courses AS SELECT id, course_id FROM sections;
    -- Create primary and secondary indexes
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

### Default

Create a default index when there is no particular `WHERE` or `JOIN` clause that would fit the above cases. This can still speed up your query by reading the input from memory.

Clause                                               | Index                               |
-----------------------------------------------------|-------------------------------------|
`SELECT x, y FROM obj_name`                          | `CREATE DEFAULT INDEX ON obj_name;` |
