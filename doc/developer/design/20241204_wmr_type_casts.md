# WMR CTE Type Casts

## The Problem

The code on `main` prevents users from declaring a WMR CTE with a `numeric`
column if the corresponding value returned from the query uses a different
scale, even though SQL trivially supports the operation.

More generally, WMR's reliance on `ScalarType`'s `PartialEq` implementation
to determine type compatibility is more underpowered than it needs to be.

## Success Criteria

The following SQL logic test passes:

```
statement ok
CREATE TABLE y (a BIGINT);

statement ok
INSERT INTO y VALUES (1);

query T
WITH MUTUALLY RECURSIVE
    bar(x NUMERIC) as (SELECT sum(a) FROM y)
SELECT x FROM bar
----
1

query T
WITH MUTUALLY RECURSIVE
    bar(x NUMERIC) as (SELECT sum(a) FROM y)
SELECT pg_typeof(x) FROM bar
----
numeric

query T
WITH MUTUALLY RECURSIVE
    bar(x NUMERIC) as (SELECT sum(a) + 1.23456 FROM y)
SELECT x FROM bar
----
2.23456

query T
WITH MUTUALLY RECURSIVE
    bar(x NUMERIC(38,2)) as (SELECT sum(a) + 1.23456 FROM y)
SELECT x FROM bar
----
2.23
```

## Out of Scope

Treating untyped string literals as "untyped string literals" rater than `TEXT`
values. This could be done at a later point in time.

## Solution Proposal

We can cast the derived `RelationExpr`'s output types to match those that the
user proposed in their statement using `CastContext::Assignment`.

## Minimal Viable Prototype

See the changed files bundled in this PR.

## Alternatives

### What kind of cast?

In the most simplistic terms, we have options for how this coercion should
occur:

-   No coercion
-   Implicit casts
-   Assignment casts

#### A quick note on casts

The difference between cast contexts (implicit, assignment, explicit) is how
destructive the cast may be. Implicit casts are not permitted to introduce any
"destructive" behavior whatsoever, assignment casts allow truncation of
different varieties, and explicit casts allow the most radical transformations
(e.g. moving between type categories).

#### Assignment casts (preferred)

Because WMR statements require users to supply an explicit schema for the CTEs,
we should honor that schema with assignment casts.

This means that if a user specifies that a column in a WMR CTE is of a type that
also contains a typmod (e.g. `numeric(38,2)`), we should impose this typmod on
all values returned from the CTE.

At first I thought this made the CTE too much like a relation, but I validated
that transient relations do carry typmods into their output with the following
example in PG.

```sql
CREATE FUNCTION inspect_type(IN TEXT, IN TEXT)
	RETURNS TEXT
	LANGUAGE SQL
	IMMUTABLE
	RETURNS NULL ON NULL INPUT
	AS $$SELECT
	format_type(id, typmod)
FROM
	(
		SELECT
			atttypid AS id, atttypmod AS typmod
		FROM
			pg_attribute AS att
			JOIN pg_class AS class ON att.attrelid = class.oid
		WHERE
			class.relname = $1 AND att.attname = $2
	)
		AS r;$$;

CREATE TABLE x (a) AS SELECT 1.234::numeric(38,2)`;

SELECT inexpect_type('x', 'a');
----
numeric(38,2)
```

#### Implicit casts

Implicit casts are non-destructive and permissive. For example, implicitly
casting `numeric(38,10)` to `numeric(38,2)` (such as adding two values of these
types) will produce `numeric(38,10)`––there is no way of performing the
truncation expressed by the typmod to the higher-scaled value. Because of this,
I don't think the implicit context is appropriate to use when casting the
derived relation to the proposed relation.

If this were the desired behavior, I would recommend we disallow typmods on the
column definitions for WMR CTEs.

However, this then introduces wrinkles in dealing with custom types––we disallow
explicit typmods, but what about custom types?

#### No coercion

Another option here would be to perform no coercion but allow more types. For
example, we could disallow typmods in WMR CTE definitions, but allow any type
that passes the `ScalarType::base_eq` check.

For example, if you declared the return type as `numeric`, it would let you
return any `numeric` type irrespective of its scale, e.g. `numeric(38,0)`,
`numeric(38,2)`.

This might be the "easiest" to implement, but introduces a type of
casting/coercion behavior not used anywhere else. This seems unergonomic to
introduce and without clear benefit.
