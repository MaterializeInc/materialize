### Constraints

Constraints declare data quality rules on objects. Three kinds exist:

- **PRIMARY KEY** — no duplicate values in the specified columns
- **UNIQUE CONSTRAINT** — no duplicate values (same semantics as PK)
- **FOREIGN KEY** — every value in child columns must exist in referenced parent columns

Two enforcement modes:

- **Enforced** (default) — compiled into a companion materialized view that
  continuously monitors for violations. Empty result set = no violations.
  Requires `IN CLUSTER`.
- **NOT ENFORCED** — metadata-only for documentation/catalog purposes. Never
  executed. `IN CLUSTER` is forbidden.

#### Syntax

```sql
CREATE { PRIMARY KEY | UNIQUE CONSTRAINT | FOREIGN KEY }
  [NOT ENFORCED]
  [<name>]
  [IN CLUSTER <cluster>]
  ON <object> (<columns>)
  [REFERENCES <object> (<columns>)]  -- FK only
;
```

#### Naming

Explicit: `CREATE PRIMARY KEY my_pk IN CLUSTER c ON ...`

Auto-generated default: `<object>_<col1>_<col2>_<kind>` where kind is `pk`,
`unique`, or `fk`. Examples:

- `users_id_pk`
- `emails_email_unique`
- `orders_customer_id_fk`

#### Object type rules

| Parent type       | Enforced | NOT ENFORCED |
|-------------------|----------|--------------|
| Materialized View | Yes      | Yes          |
| View              | No       | Yes          |
| Table             | No       | No           |

#### IN CLUSTER rules

| Enforcement  | IN CLUSTER |
|--------------|------------|
| Enforced     | Required   |
| NOT ENFORCED | Forbidden  |

#### FK reference target rules

| Target type              | Enforced FK | Not-enforced FK |
|--------------------------|-------------|-----------------|
| Table / Table from Source| Yes         | Yes             |
| Materialized View        | Yes         | Yes             |
| View                     | No          | Yes             |

#### How enforced constraints work

Lowered into companion materialized views:

- **PK / UNIQUE**: `SELECT <cols>, count(*) FROM <obj> GROUP BY <cols> HAVING count(*) > 1`
- **FK**: `SELECT <fk_cols> FROM <child> EXCEPT SELECT <pk_cols> FROM <parent>`

These MVs participate in the full deployment pipeline (dependency tracking,
blue/green, change detection) but are hidden from explorer docs.

#### Examples

Enforced PK on a materialized view:
```sql
CREATE MATERIALIZED VIEW users IN CLUSTER compute AS SELECT * FROM ingest.users_raw;

CREATE PRIMARY KEY users_pk IN CLUSTER compute ON users (id);
```

Enforced FK on a materialized view:
```sql
CREATE MATERIALIZED VIEW orders IN CLUSTER compute AS SELECT * FROM ingest.orders_raw;

CREATE FOREIGN KEY orders_fk IN CLUSTER compute ON orders (user_id) REFERENCES mydb.public.users (id);
```

Enforced UNIQUE on a materialized view:
```sql
CREATE MATERIALIZED VIEW emails IN CLUSTER compute AS SELECT * FROM ingest.emails_raw;

CREATE UNIQUE CONSTRAINT emails_unique IN CLUSTER compute ON emails (email);
```

NOT ENFORCED PK on a view:
```sql
CREATE VIEW items AS SELECT 1 AS item_id, 'test'::text AS name;

CREATE PRIMARY KEY NOT ENFORCED items_pk ON items (item_id);
```

#### Common errors

- Constraints on tables → constraint not allowed on table
- Enforced on a view → enforced constraint not allowed
- Enforced missing IN CLUSTER → missing required IN CLUSTER
- NOT ENFORCED with IN CLUSTER → must not specify IN CLUSTER
- ON name doesn't match file's main object → reference mismatch
- Column doesn't exist on parent → column not found
- FK reference column doesn't exist → ref column not found
- FK target is wrong type → invalid target type
