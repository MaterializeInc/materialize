---
title: "JOIN"
description: "`JOIN` lets you combine two or more table expressions."
menu:
  main:
    parent: 'sql'
---

`JOIN` lets you combine two more more table expressions into a single table
expression.

## Conceptual Framework

Much like an RDBMS, Materialize can join together any two table expressions (in
our case, either [sources](../create-sources) or [views](../create-views)) into
a single table expression.

Materialize's support for `JOIN` is radically simplified from most streaming
platforms, and generally provides unrestricted access to `JOIN`s.

## Syntax

### join_expr

{{< diagram "join-expr.html" >}}

### join_type

{{< diagram "join-type.html" >}}

Field | Use
------|-----
_select&lowbar;pred_ | The predicating [`SELECT`](../select) clauses you want to use, e.g. `SELECT col_ref FROM table_ref...`. The _table&lowbar;ref_ from the _select&lowbar;pred_ is the left-hand table.
**NATURAL** | Join table expressions on all columns with the same names in both tables. This is similar to the `USING` clause naming all identically named columns in both tables.
_join&lowbar;type_ | The type of `JOIN` you want to use _(`INNER` is implied default)_.
_table&lowbar;ref_ | The table expression you want to join, i.e. the right-hand table.
**USING (** _col&lowbar;ref..._ **)** | If the join condition does not require table-level qualification (i.e. joining tables on columns with the same name), the columns to join the tables on. For example, `USING (customer_id)`.
**ON** _expression_ | The condition on which to join the tables. For example `ON purchase.customer_id = customer.id`.
_select&lowbar;pred_ | The remaining [`SELECT`](../select) clauses you want to use, e.g. `...WHERE expr GROUP BY col_ref HAVING expr`.

**Note**: It's possible to join together table expressions as inner joins without using this clause whatsoever, e.g. `SELECT cols... FROM t1, t2 WHERE t1.x = t2.x GROUP BY cols...`

## Details

Unlike most other streaming platforms, `JOIN`s in Materialize have very few, if
any, restrictions. For example, Materialize:

- Does not require time windows when joining streams.
- Does not require any kind of partitioning.

Instead, `JOIN`s work over the available history of both streams, which
ultimately provides an experience more similar to an RDBMS than other streaming
platforms.

## Examples

For these examples, we'll use a small data set:

**Employees**

```nofmt
 id |  name
----+--------
  1 | Frank
  2 | Arjun
  3 | Nikhil
  4 | Cuong
```

**Managers**

```nofmt
 id | name  | manages
----+-------+---------
  1 | Arjun |       4
  2 | Cuong |       3
  3 | Frank |
```

In this table:

- `Arjun` and `Frank` do not have managers.
- `Frank` is a manager but has no reports.

### Inner Join

Inner joins return all tuples from both tables where the join condition is
valid.

![inner join diagram](/images/join-inner.png)

```sql
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
INNER JOIN managers
  ON employees.id = managers.manages;
```
```nofmt
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
```

### Left Outer Join

Left outer joins (also known as left joins) return all tuples from the
left-hand-side table, and all tuples from the right-hand-side table that match
the join condition. Tuples on from the left-hand table that are not joined with
a tuple from the right-hand table contain `NULL` wherever the right-hand table
is referenced.

![left outer join diagram](/images/join-left-outer.png)

```sql
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
LEFT OUTER JOIN managers
  ON employees.id = managers.manages;
```
```nofmt
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
 Arjun    |
 Frank    |
 ```

### Right Outer Join

Right outer joins (also known as right joins) are simply the right-hand-side
equivalent of left outer joins.

Right outer joins (return all tuples from the right-hand-side table, and all
tuples from the left-hand-side table that match the join condition. Tuples on
from the right-hand table that are not joined with a tuple from the left-hand
table contain `NULL` wherever the left-hand table is referenced.

![right outer join diagram](/images/join-right-outer.png)

```sql
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
RIGHT OUTER JOIN managers
  ON employees.id = managers.manages;
```
```nofmt
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
          | Frank
 ```

### Full Outer Join

Full outer joins perform both a left outer join and a right outer join. They
return all tuples from both tables, and join them together where the join
conditions are met.

Tuples that are not joined with the other table contain `NULL` wherever the
other table is referenced.

![full outer join diagram](/images/join-full-outer.png)

```sql
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
FULL OUTER JOIN managers
  ON employees.id = managers.manages;
```
```nofmt
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
          | Frank
 Arjun    |
 Frank    |
```

### Cross Join

Cross joins return the [Cartesian
product](https://en.wikipedia.org/wiki/Cartesian_product) of the two tables,
i.e. all combinations of tuples from the left-hand table combined with tuples
from the right-hand table.

![cross join diagram](/images/join-cross.png)

Our example dataset doesn't have a meaningful cross-join query, but the above
diagram shows how cross joins form the Cartesian product.

## Related pages

- [`SELECT`](../select)
- [`CREATE VIEW`](../create-view)
