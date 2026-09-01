---
title: "CREATE FOREIGN KEY"
description: "`CREATE FOREIGN KEY` records that columns of one relation correspond to columns of another."
menu:
  main:
    parent: 'commands'
---

{{< private-preview />}}

`CREATE FOREIGN KEY` records that a set of columns in one relation corresponds
to a set of columns in another. Materialize stores the relationship as catalog
metadata and does not check it. Nothing about how your queries are planned or
what they return changes.

The point of declaring one is to make a join path discoverable. Tools that read
the catalog, including the Materialize MCP server, can then tell which relations
join to which and on what columns, instead of guessing from column names.

## Syntax

{{% include-syntax file="examples/create_foreign_key" example="syntax" %}}

## Details

### Not enforced

Materialize never validates a foreign key. A referencing value with no matching
referenced value is not an error, and no statement is rejected because of one.
The `NOT ENFORCED` clause is required so that the declaration says as much
wherever it appears.

This means a foreign key describes intent, not a guarantee. Read it as "these
columns are meant to line up," and treat it as a statement about how to join the
two relations rather than a promise about the data in them.

Because nothing is enforced, the referenced columns do not need to be unique,
and Materialize does not require an index or a key on either side.

### Column types

The two column lists are matched by position, so they must be the same length.
Each pair must have comparable types, using the same rule Materialize applies
when two values meet in an expression: the types must share a type category and
have a common type both convert to.

In practice this accepts the pairings you can actually join on and rejects the
ones you cannot. Pairing an [`integer`](/sql/types/integer) column with a
`bigint` column works, as does pairing two [`numeric`](/sql/types/numeric)
columns of different precision. Pairing [`text`](/sql/types/text) with
`integer` returns an error.

### Lifecycle

A foreign key depends on both relations it names. Dropping either one drops the
foreign key with it, and does not require `CASCADE`.

You can also drop a foreign key on its own with
[`DROP FOREIGN KEY`](/sql/drop-foreign-key), which leaves both relations
untouched.

## Examples

### Declare a relationship between two tables

```mzsql
CREATE TABLE customers (id uuid, name text, region text);
CREATE TABLE orders (id uuid, customer_id uuid, region text, total numeric);

CREATE FOREIGN KEY orders_customer_fkey
ON orders (customer_id)
REFERENCES customers (id)
NOT ENFORCED;
```

A tool reading the catalog can now join `orders` to `customers` without
inferring the relationship from the column names.

### Let Materialize name the foreign key

If you omit the name, Materialize derives one from the referencing relation and
its columns.

```mzsql
CREATE FOREIGN KEY ON orders (customer_id)
REFERENCES customers (id)
NOT ENFORCED;
```

```mzsql
SELECT name FROM (SHOW FOREIGN KEYS ON orders);
```

```nofmt
          name
--------------------------
 orders_customer_id_fkey
```

### Declare a composite relationship

When rows correspond on more than one column, list the columns in matching
order.

```mzsql
CREATE FOREIGN KEY ON orders (customer_id, region)
REFERENCES customers (id, region)
NOT ENFORCED;
```

The columns pair up by position: `orders.customer_id` with `customers.id`, and
`orders.region` with `customers.region`.

### Declare a relationship over a view

A foreign key can name any relation, not just a table. This is often what you
want, since the relationships that matter to a consumer are usually between the
views you publish rather than the tables underneath them.

```mzsql
CREATE MATERIALIZED VIEW order_totals AS
    SELECT customer_id, sum(total) AS total
    FROM orders
    GROUP BY customer_id;

CREATE FOREIGN KEY ON order_totals (customer_id)
REFERENCES customers (id)
NOT ENFORCED;
```

### Describe what the relationship means

Attach a comment to explain the relationship to whoever, or whatever, reads it
next.

```mzsql
COMMENT ON FOREIGN KEY orders_customer_fkey IS
    'Each order is placed by exactly one customer.';
```

The comment is returned alongside the relationship by
[`SHOW FOREIGN KEYS`](/sql/show-foreign-keys) and by the MCP server.

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-foreign-key" %}}

Ownership of both relations is required because a foreign key is an assertion
about what someone else's data means, published where other people and tools
will act on it.

## Related pages

- [`DROP FOREIGN KEY`](/sql/drop-foreign-key)
- [`SHOW FOREIGN KEYS`](/sql/show-foreign-keys)
- [`SHOW CREATE FOREIGN KEY`](/sql/show-create-foreign-key)
- [`COMMENT ON`](/sql/comment-on)
