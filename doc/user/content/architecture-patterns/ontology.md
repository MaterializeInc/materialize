---
title: "Use an ontology table"
description: "Create an ontology table that helps agents write correct joins."
menu:
  main:
    parent: architecture-patterns
    weight: 10
---

The ontology table is a curated catalog of join relationships between tables in
your database. Each row describes a single join: the columns in one table that
reference columns in another.

Through the Materialize [MCP server](/integrations/mcp-server/)'s `query` tool,
an agent can query the ontology table before writing multi-table SQL.

{{< note >}}
This pattern relies on the MCP server's `query` tool, which is enabled by
default starting in v26.27 for the agent MCP server and v26.30 for the developer
MCP server.
{{</ note >}}

```sql
CREATE TABLE ontology (
    table_name         text   NOT NULL,
    columns            text[] NOT NULL,
    referenced_table   text   NOT NULL,
    referenced_columns text[] NOT NULL
);

COMMENT ON TABLE ontology IS
'Defines the join relationships between tables in the database. Each row
describes a single join: the columns in table_name that reference
referenced_columns in referenced_table. ALWAYS query this table before
writing any multi-table query. Use it to confirm exact join keys rather
than guessing column names. Filter by table_name OR referenced_table to
find all relationships involving a given table.';

COMMENT ON COLUMN ontology.table_name IS
'The dependent table, the one that holds the foreign key.';
COMMENT ON COLUMN ontology.columns IS
'The FK columns in table_name, in order. Pair positionally with referenced_columns.';
COMMENT ON COLUMN ontology.referenced_table IS
'The parent table, the one being pointed to.';
COMMENT ON COLUMN ontology.referenced_columns IS
'The PK or unique columns in referenced_table, in order matching columns.';

CREATE DEFAULT INDEX ON ontology;
```

## Agent system prompt

Add the following to the agent's system prompt to enforce the intended behavior:

```text
Before writing or executing any joins, query the ontology table for the involved table names. Use the returned join keys verbatim.
```

## Example: e-commerce schema

Given the following tables and join-relevant columns:

| Table | Key columns |
| --- | --- |
| `customers` | `id`, `email` |
| `addresses` | `id`, `customer_id` |
| `orders` | `id`, `customer_id`, `shipping_address_id` |
| `order_items` | `id`, `order_id`, `product_id` |
| `products` | `id`, `category_id` |
| `categories` | `id` |
| `support_tickets` | `id`, `customer_email` *(implicit join, no FK)* |

The ontology table is populated as:

```sql
INSERT INTO ontology (table_name, columns, referenced_table, referenced_columns) VALUES
('addresses',       ARRAY['customer_id'],         'customers',  ARRAY['id']),
('orders',          ARRAY['customer_id'],         'customers',  ARRAY['id']),
('orders',          ARRAY['shipping_address_id'], 'addresses',  ARRAY['id']),
('order_items',     ARRAY['order_id'],            'orders',     ARRAY['id']),
('order_items',     ARRAY['product_id'],          'products',   ARRAY['id']),
('products',        ARRAY['category_id'],         'categories', ARRAY['id']),
('support_tickets', ARRAY['customer_email'],      'customers',  ARRAY['email']);
```

Tables with multiple relationships, like `orders`, contribute one row per
relationship. Implicit joins, such as `support_tickets` → `customers`, are
documented exactly like the declared foreign-key relationships.
