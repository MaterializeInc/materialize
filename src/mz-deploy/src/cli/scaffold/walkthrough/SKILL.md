---
name: walkthrough
description: >
  Interactive tutorial that guides a user through building a data mesh with
  mz-deploy across 10 modules and ~68 steps, ending with `explore` to visualize
  the result. Framed as a Middle-earth quest where the user is a Lorekeeper
  forging the Great Ontology.
user_invocable: true
---

## OVERVIEW FOR CLAUDE CODE

You are conducting an interactive educational session to teach a user how to build a real data mesh with mz-deploy — the declarative CLI for Materialize blue-green deployments. The tutorial is framed as a quest through Middle-earth: the user is a **Lorekeeper** tasked with forging the Great Ontology (a unified source of truth) that the kingdoms (applications) can depend on.

**Hybrid format**: You (Claude) write all SQL files directly. The user runs `mz-deploy` commands in a second terminal. This keeps the session flowing — you inscribe the scrolls, they invoke the magic. At four key moments, the user writes SQL themselves to build muscle memory.

**Tone**: Light fantasy flavor in `Say:` blocks. Technical content stays clear and direct. Don't overdo the theme — a sentence or two of flavor per step, then get practical.

This session covers 10 modules plus setup. Estimate 90–120 minutes total. Always wait for user confirmation before proceeding to the next module. Follow the STEP / WAIT FOR / CHECKPOINT structure precisely.

---

## PRE-SESSION SETUP: "The Gates of the Realm"

STEP 1: Quest introduction.

Say: "You stand at the gates of the Materialize Realm, Lorekeeper. Your quest: to forge the **Great Ontology**, a unified source of truth that the kingdoms of your data mesh can depend on.

This is a hands-on tutorial. I'll write most of the SQL files, but at key moments, you'll take the quill yourself. You'll run the mz-deploy commands in a **second terminal** to bring them to life.

Do you have a second terminal window ready alongside this conversation?"

WAIT FOR: User confirmation they have a second terminal ready.

STEP 2: Verify mz-deploy is installed.

Say: "First, let's make sure `mz-deploy` is at your command. In your second terminal, run:

```
mz-deploy --help
```

Do you see the help output?"

WAIT FOR: User confirmation that mz-deploy is working.

IF ISSUES: Help troubleshoot installation before continuing.

STEP 3: Spin up Docker infrastructure.

ACTION (Claude runs the setup script — no manual steps needed):
1. Run `.agents/skills/walkthrough/references/setup.sh` via Bash.
2. If the script exits non-zero, show the error output and help troubleshoot.

Say: "I've started Materialize and Postgres via Docker. Both are healthy.

Now, in your **second terminal**, tell mz-deploy to find `profiles.toml` in the current project directory rather than the default `~/.mz/`:

```
export MZ_DEPLOY_PROFILES_DIR=.
```

Then verify the realm is reachable:

```
mz-deploy debug
```

You should see a successful connection. What do you see?"

WAIT FOR: User confirmation that `mz-deploy debug` succeeds.

IF ISSUES: Help troubleshoot Docker/connection issues. Check that `profiles.toml` exists in the project root and that `MZ_DEPLOY_PROFILES_DIR=.` is set in the terminal. The env var tells mz-deploy to look for `profiles.toml` in the current directory. If Docker services aren't running, check `docker compose ps` and `docker compose logs`.

CHECKPOINT: Say: "The gates are open and the realm awaits. Ready to begin Module 1?"

WAIT FOR: User confirmation to proceed.

---

## MODULE 1: "The Map of the Realm" — Project Orientation

STEP 4: Introduce mz-deploy.

Say: "**Module 1: The Map of the Realm**

**mz-deploy** is a declarative CLI for managing Materialize deployments. You define SQL objects in files, and mz-deploy deploys them blue-green, staging new versions alongside production and atomically swapping them in.

Let's survey the project structure. Here's what we have:"

Read the project directory and show the user the layout:
```
.
├── clusters/          — Cluster definitions (compute resources)
├── models/            — SQL model files (the heart of the project)
│   └── materialize/
│       └── public/    — Schema directory (empty for now)
├── roles/             — Role definitions
├── network-policies/  — Network policies
├── profiles.toml      — Connection profiles
└── project.toml       — Project settings
```

Explain: "**File paths are fully qualified names.** A file at `models/materialize/public/customers.sql` defines the object `materialize.public.customers`. The directory structure *is* the namespace."

STEP 5: Explain project.toml.

Read `project.toml` and show it to the user.

Say: "Two settings matter here:
- **`profile`** — which connection profile to use (from `profiles.toml`)
- **`mz_version`** — `\"cloud\"` means target the latest Materialize cloud release. You can pin a specific version like `\"v26.0.0\"` if needed."

STEP 6: Explain profiles.toml.

Say: "Connection profiles live in `profiles.toml`. A profile tells mz-deploy how to reach your Materialize instance: host, port, credentials. Our default profile points to the local Docker instance.

mz-deploy resolves the profiles directory in this order: the `--profiles-dir` CLI flag, then the `MZ_DEPLOY_PROFILES_DIR` environment variable, then `~/.mz/`. We set the env var in the previous step so mz-deploy finds our local file."

STEP 7: Explain key conventions.

Say: "Three conventions to remember as we journey forward:

1. **One object per file** — each `.sql` file defines exactly one primary object (view, table, source, etc.), and the filename must match the object name.

2. **Schema separation** — storage objects (tables, sources) and computation objects (views, materialized views) **cannot share a schema**. We'll use `raw` for storage and `public`/`internal` for computation.

3. **Two deployment paths** — `apply` manages durable infrastructure (clusters, sources, tables) that persists across deployments. `stage` + `promote` deploys computation (views, MVs) via blue-green swaps.

```
apply (durable infrastructure):     stage + promote (blue-green):
  clusters                            views
  secrets                             materialized views
  connections                         indexes
  sources
  tables
  roles
  network policies
```

These will make more sense as we build. Any questions before we move on?"

CHECKPOINT: Say: "The map is drawn. Ready to open the Eastern Gate in Module 2?"

WAIT FOR: User confirmation to proceed.

---

## MODULE 2: "Opening the Eastern Gate" — Infrastructure (apply)

STEP 8: Introduce infrastructure concepts.

Say: "**Module 2: Opening the Eastern Gate**

Infrastructure objects — clusters, secrets, connections, sources, and tables — are **durable state**. They persist across deployments, and only `mz-deploy apply` creates or modifies them. They are *not* part of the blue-green `stage`/`promote` cycle.

We need to:
1. Create clusters for ingestion and computation
2. Connect to Postgres (our source of raw materials)
3. Ingest the customers, orders, and products tables via CDC

I'll write the SQL. You'll run `apply`. First, the engines of the realm."

STEP 9: Create clusters for ingestion and computation.

Create the file `clusters/ingest.sql` with:
```sql
CREATE CLUSTER ingest SIZE = '50cc';
```

Create the file `clusters/compute.sql` with:
```sql
CREATE CLUSTER compute SIZE = '50cc';
```

Say: "Two clusters — `ingest` for source ingestion, `compute` for views and materialized views. Separate clusters let you scale ingestion and computation independently."

STEP 10: Create the Postgres CDC infrastructure.

Create the following files. Explain each briefly as you create it.

Create `models/materialize/raw/pgpass.sql`:
```sql
CREATE SECRET pgpass AS env_var('PGPASSWORD');
```

Say: "`env_var()` pulls the password from the environment at deploy time. For production, you can use `aws_secret('secret-name')` to pull from AWS Secrets Manager (requires `aws_profile` in your profile config). Never hardcode credentials in SQL."

Create `models/materialize/raw/pg_connection.sql`:
```sql
CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'postgres',
    PORT 5432,
    DATABASE 'shop',
    USER 'postgres',
    PASSWORD = SECRET pgpass
);
```

Say: "A named connection to our Postgres instance. The host `postgres` is the Docker service name."

Create `models/materialize/raw/pg_source.sql`:
```sql
CREATE SOURCE pg_source
    IN CLUSTER ingest
    FROM POSTGRES CONNECTION pg_connection
    (PUBLICATION 'shop_publication');
```

Say: "This sets up Postgres CDC (Change Data Capture). Materialize will continuously replicate changes from the `shop_publication` publication.

```
Postgres (shop DB)
  └─ publication: shop_publication
       └─ pg_connection ──► pg_source (cluster: ingest)
                                ├─► raw.customers
                                ├─► raw.orders
                                └─► raw.products
```"

Create `models/materialize/raw/customers.sql`:
```sql
CREATE TABLE customers FROM SOURCE pg_source (REFERENCE shop.public.customers);
```

Create `models/materialize/raw/orders.sql`:
```sql
CREATE TABLE orders FROM SOURCE pg_source (REFERENCE shop.public.orders);
```

Create `models/materialize/raw/products.sql`:
```sql
CREATE TABLE products FROM SOURCE pg_source (REFERENCE shop.public.products);
```

Say: "Three tables from the source — customers, orders, and products. Each `CREATE TABLE ... FROM SOURCE` binds a specific upstream Postgres table to a Materialize table."

STEP 11: Preview with dry-run.

Say: "Before we run it, let's preview what `apply` will do. In your second terminal:

```
mz-deploy apply --dry-run
```

This shows what `apply` will create, without executing anything. What do you see?"

WAIT FOR: User reports dry-run output.

STEP 12: Execute apply.

Say: "Looks good — now let's open the gate for real:

```
mz-deploy apply
```

This creates the cluster, secret, connection, source, and tables in Materialize. What does the output show?"

WAIT FOR: User confirms apply succeeded.

STEP 13: Explain types.lock.

Say: "Notice something new: a `types.lock` file. When `apply` creates tables from external sources, mz-deploy caches their schemas (column names and types) in `types.lock`. This lets `compile` and `test` resolve external table schemas **locally** without a live Materialize connection.

You can regenerate it anytime with `mz-deploy lock`."

CHECKPOINT: Say: "The Eastern Gate is open. Data flows from Postgres into the realm. Ready to forge your first artifacts in Module 3?"

WAIT FOR: User confirmation to proceed.

---

## MODULE 3: "Forging the First Artifacts" — Creating Views

STEP 14: Introduce view creation.

Say: "**Module 3: Forging the First Artifacts**

We're building the **ontology** — canonical business entities that represent the truth of your domain. We'll start with simple views in `public`, then refactor to the stable API pattern in Module 7.

Remember: one object per file, filename matches object name."

STEP 15: Create customers view.

Create `models/materialize/public/customers.sql`:
```sql
CREATE VIEW customers AS
SELECT
    id,
    name,
    email,
    created_at
FROM raw.customers
WHERE active = true;
```

Say: "Our first artifact — the `customers` view. It selects from `raw.customers` (the CDC table we created in Module 2) and filters to only active customers. The file lives at `models/materialize/public/customers.sql`, so its fully qualified name is `materialize.public.customers`."

STEP 16: Create orders view.

Create `models/materialize/public/orders.sql`:
```sql
CREATE VIEW orders AS
SELECT
    o.id,
    o.customer_id,
    c.name AS customer_name,
    o.product_id,
    o.quantity,
    o.status,
    o.created_at
FROM raw.orders o
JOIN customers c ON o.customer_id = c.id;

CREATE INDEX orders_customer_idx IN CLUSTER compute ON orders (customer_id);

COMMENT ON VIEW orders IS 'Orders enriched with customer name, filtered to active customers via join';
```

Say: "The `orders` view joins raw orders with our `customers` view — so it inherits the active-customer filter automatically. Notice two supporting statements:
- `CREATE INDEX` — speeds up lookups by `customer_id`
- `COMMENT ON` — documents the object

These are **supporting statements** — allowed in the same file as the primary object. `GRANT` is another common one (e.g., `GRANT SELECT ON orders TO some_role`).

mz-deploy resolves dependencies automatically — it knows `orders` depends on `customers` and will deploy them in the right order."

STEP 17: Your turn to forge.

Say: "You've seen two views created. Now it's your turn. Create the file `models/materialize/public/products.sql` with a view that:

- Selects `id`, `name`, `description`, `price`, and `inventory` from `raw.products`
- Filters to products where `inventory > 0`

Remember: filename must match object name, one object per file.

Create the file in your second terminal (or your editor), then run:

```
mz-deploy compile
```

Does it pass?"

WAIT FOR: User confirms compile passes.

IF ISSUES: Show the correct SQL and help debug. The expected solution is:
```sql
CREATE VIEW products AS
SELECT
    id,
    name,
    description,
    price,
    inventory
FROM raw.products
WHERE inventory > 0;
```
Common mistakes: wrong filename, using `SELECT *`, forgetting the WHERE clause, or not matching column names exactly.

STEP 18: Verify understanding.

Say: "Well forged! We now have three views that transform raw CDC data into clean, meaningful entities. In the next module, we'll validate them with compilation. Any questions about the file structure or conventions?"

CHECKPOINT: Say: "Three artifacts forged — two by scribe, one by your own hand. Ready for the Trial of Validation in Module 4?"

WAIT FOR: User confirmation to proceed.

---

## MODULE 4: "The Trial of Validation" — Compilation

STEP 19: Introduce compilation.

Say: "**Module 4: The Trial of Validation**

`mz-deploy compile` validates your SQL locally:
- Parses every `.sql` file
- Resolves dependencies and determines the right deployment order
- Type-checks against `types.lock` (using a local Materialize container via Docker)

Nothing touches your running Materialize instance. This is pure local validation."

STEP 20: Run compile.

Say: "In your second terminal:

```
mz-deploy compile
```

This should succeed — our SQL is clean. What do you see?"

WAIT FOR: User confirms compile succeeds.

STEP 21: Explore verbose output.

Say: "Now try verbose mode to see the dependency graph and deployment order:

```
mz-deploy compile -v
```

What does the output show?"

WAIT FOR: User reports verbose output.

STEP 22: Demonstrate compile catching errors.

Create a deliberately broken file `models/materialize/public/broken.sql`:
```sql
CREATE VIEW broken AS
SELECT nonexistent_column
FROM raw.customers;
```

Say: "I've planted a flaw — a view that references a column that doesn't exist. Run compile again:

```
mz-deploy compile
```

See how the error is caught locally, before anything touches production?"

WAIT FOR: User confirms they see the compilation error.

Delete the broken file.

Say: "Error caught, artifact destroyed. The trial works."

CHECKPOINT: Say: "The Trial of Validation is complete. Ready for the Crucible of Proof in Module 5?"

WAIT FOR: User confirmation to proceed.

---

## MODULE 5: "The Crucible of Proof" — Unit Testing

STEP 23: Introduce unit test syntax.

Say: "**Module 5: The Crucible of Proof** — Compilation proves your SQL is valid. Tests prove it's *correct*.

mz-deploy has a built-in test syntax: `EXECUTE UNIT TEST`. This is **unique to mz-deploy** — you won't have seen it elsewhere. Let me teach you the full syntax before we write any tests.

Here's the structure:

```sql
EXECUTE UNIT TEST <test_name>
FOR <database>.<schema>.<view>
[AT TIME '<timestamp>']
MOCK <fqn>(<col> <type>, ...) AS (
  SELECT * FROM VALUES (<val>, ...), ...
),
...
EXPECTED(<col> <type>, ...) AS (
  SELECT * FROM VALUES (<val>, ...), ...
);
```

Let me break down each clause:"

Explain each part clearly:
- **`EXECUTE UNIT TEST <name>`** — declares a named test. The name appears in test output so you can identify which test passed or failed.
- **`FOR <database.schema.view>`** — the fully qualified view being tested.
- **`MOCK`** — one per dependency of the target view. Column names and types must match the real schema. You're providing fake data that the view will process.
- **`EXPECTED`** — the expected output columns and rows. Columns must match the view's output schema.
- **`AT TIME`** (optional) — sets the value of `mz_now()` for views with temporal filters.

STEP 24: Write the first test.

Edit `models/materialize/public/customers.sql` to append the test after the view definition:

```sql
CREATE VIEW customers AS
SELECT
    id,
    name,
    email,
    created_at
FROM raw.customers
WHERE active = true;

EXECUTE UNIT TEST test_filters_inactive
FOR materialize.public.customers
MOCK materialize.raw.customers(id bigint, name text, email text, created_at timestamptz, active boolean) AS (
    SELECT * FROM (VALUES
        (1, 'alice', 'alice@example.com', '2024-01-01T00:00:00Z'::timestamptz, true),
        (2, 'bob', 'bob@example.com', '2024-01-01T00:00:00Z'::timestamptz, false))
),
EXPECTED(id bigint, name text, email text, created_at timestamptz) AS (
    SELECT * FROM (VALUES
        (1, 'alice', 'alice@example.com', '2024-01-01T00:00:00Z'::timestamptz))
);
```

Say: "Walk through what's happening:
- The **MOCK** provides two customers: alice (active=true) and bob (active=false)
- The MOCK columns match `raw.customers`'s real schema (id, name, email, created_at, active)
- The **EXPECTED** output has only alice — because the view filters `WHERE active = true`
- The EXPECTED columns match the view's output (id, name, email, created_at — no `active` column since the view doesn't select it)"

STEP 26: Run the test.

Say: "Time to enter the crucible. In your second terminal:

```
mz-deploy test
```

What does the output show?"

WAIT FOR: User confirms the test passes.

STEP 27: Write a test with dependency mocking.

Edit `models/materialize/public/orders.sql` to append a test:

```sql
CREATE VIEW orders AS
SELECT
    o.id,
    o.customer_id,
    c.name AS customer_name,
    o.product_id,
    o.quantity,
    o.status,
    o.created_at
FROM raw.orders o
JOIN customers c ON o.customer_id = c.id;

CREATE INDEX orders_customer_idx IN CLUSTER compute ON orders (customer_id);

COMMENT ON VIEW orders IS 'Orders enriched with customer name, filtered to active customers via join';

EXECUTE UNIT TEST test_enriches_with_customer_name
FOR materialize.public.orders
MOCK materialize.public.customers(id bigint, name text, email text, created_at timestamptz) AS (
    SELECT * FROM (VALUES
        (1, 'alice', 'alice@example.com', '2024-01-01T00:00:00Z'::timestamptz))
),
MOCK materialize.raw.orders(id bigint, customer_id bigint, product_id bigint, quantity int, status text, created_at timestamptz) AS (
    SELECT * FROM (VALUES
        (100, 1, 10, 2, 'pending', '2024-01-15T00:00:00Z'::timestamptz))
),
EXPECTED(id bigint, customer_id bigint, customer_name text, product_id bigint, quantity int, status text, created_at timestamptz) AS (
    SELECT * FROM (VALUES
        (100, 1, 'alice', 10, 2, 'pending', '2024-01-15T00:00:00Z'::timestamptz))
);
```

Say: "**Key teaching moment**: notice what we mock here. The `orders` view depends on two things:
- `raw.orders` — the CDC table (mock it directly)
- `customers` — the **view**, not `raw.customers`

We mock `materialize.public.customers` (the view's output schema — without the `active` column), not `materialize.raw.customers`. Always mock the **direct dependencies** of the view being tested, not their upstream sources.

```
                         Testing: orders
                               ↓
raw.customers ──► customers (view) ──► orders (view) ◄── raw.orders
     ✗                  ✓ mock this                        ✓ mock this

Mock direct dependencies only. Don't reach through to raw.customers.
```

Run the tests again:

```
mz-deploy test
```"

WAIT FOR: User confirms both tests pass.

STEP 28: Your turn in the crucible.

Say: "You've seen how to write tests with MOCK and EXPECTED. Now write a unit test for `materialize.public.products`.

Hints:
- The view depends on `raw.products` — mock it with columns: `id bigint`, `name text`, `description text`, `price numeric`, `inventory int`
- Provide at least one in-stock product (inventory > 0) and one out-of-stock product (inventory = 0)
- The EXPECTED output should only contain the in-stock product

Add the test to `models/materialize/public/products.sql` after the view definition, then run:

```
mz-deploy test
```

Does your test pass?"

WAIT FOR: User confirms test passes.

IF ISSUES: Show the correct test syntax and help debug. The expected solution is:
```sql
EXECUTE UNIT TEST test_filters_out_of_stock
FOR materialize.public.products
MOCK materialize.raw.products(id bigint, name text, description text, price numeric, inventory int) AS (
    SELECT * FROM (VALUES
        (1, 'Widget', 'A useful widget', 9.99, 10),
        (2, 'Gadget', 'A cool gadget', 19.99, 0))
),
EXPECTED(id bigint, name text, description text, price numeric, inventory int) AS (
    SELECT * FROM (VALUES
        (1, 'Widget', 'A useful widget', 9.99, 10))
);
```
Common mistakes: forgetting the FOR clause, wrong column types, missing commas between MOCK/EXPECTED.

STEP 29: Demonstrate test failure.

Edit the EXPECTED in `customers.sql` test to deliberately include bob (who should be filtered out). Change the EXPECTED to:

```sql
EXPECTED(id bigint, name text, email text, created_at timestamptz) AS (
    SELECT * FROM (VALUES
        (1, 'alice', 'alice@example.com', '2024-01-01T00:00:00Z'::timestamptz),
        (2, 'bob', 'bob@example.com', '2024-01-01T00:00:00Z'::timestamptz))
);
```

Say: "I've sabotaged the test. It now expects bob, but the view filters him out. Run:

```
mz-deploy test
```

Watch for the **MISSING** label — that means a row was expected but not produced."

WAIT FOR: User observes the failure and MISSING label.

Fix the test back to the correct version (only alice in EXPECTED).

Say: "Test restored. The crucible reveals all falsehoods."

STEP 30: Show test filtering.

Say: "You can run tests for a specific view or even a specific named test:

```
mz-deploy test 'materialize.public.customers'
mz-deploy test 'materialize.public.customers#test_filters_inactive'
```

Try one of these to see filtered output."

WAIT FOR: User confirms test filtering works.

STEP 31: Mention type normalization.

Say: "One more thing before we leave the crucible: in `MOCK` and `EXPECTED`, common type aliases are interchangeable. `int`/`int4`/`integer`, `text`/`varchar`/`string`, `bigint`/`int8` — mz-deploy normalizes them. Don't worry about exact type names as long as they're equivalent."

STEP 32: Introduce JUnit XML output.

Say: "For CI pipelines, you can output test results as JUnit XML:

```
mz-deploy test --junit-xml test-results.xml
```

This produces a standard JUnit report that CI tools (GitHub Actions, Jenkins, etc.) can parse to show test results inline. Try it now if you like — you'll see a `test-results.xml` file appear in the project directory."

CHECKPOINT: Say: "The Crucible of Proof is complete. Your artifacts are validated and tested."

WAIT FOR: User confirmation.

---

## INTERMISSION: "The Halfway Rest"

Say: "You've completed the first half of the quest, Lorekeeper. You now have a complete local development workflow: write SQL, compile, test. That alone is enough to start building.

The second half covers production patterns: the stable API, deployment, cross-domain applications, and CI/CD. These are more advanced topics that build on everything you've learned.

**This is a natural stopping point.** If you want to take a break and return later, your project is in a good state. Just make sure Docker is still running and `MZ_DEPLOY_PROFILES_DIR=.` is set when you come back.

Ready to continue to the production patterns?"

WAIT FOR: User confirmation to proceed.

---

## MODULE 6: "The Great Forging" — Deployment Lifecycle

STEP 33: Introduce the deployment lifecycle.

Say: "**Module 6: The Great Forging** — Everything we've built exists only as files on disk.

The full lifecycle: `compile` -> `test` -> `apply` -> `stage` -> `wait` -> `promote`

```
          local                          remote
  ┌─────────────────┐          ┌──────────────────────┐
  │ compile → test  │          │  apply (infra)       │
  └────────┬────────┘          │  stage (create dpl)  │
           │                   │  wait  (hydrate)     │
           └──────────────────►│  promote (swap)      │
                               └──────────────────────┘
```

We've already done `compile`, `test`, and `apply`. Now we complete the cycle with `stage`, `wait`, and `promote`."

STEP 34: Preview with stage dry-run.

Say: "First, let's see what the deployment plan looks like:

```
mz-deploy stage --dry-run
```

This shows which schemas and objects the deployment creates. What do you see?"

WAIT FOR: User reports dry-run output.

STEP 35: Stage the deployment.

Say: "Now, the real forging begins:

```
mz-deploy stage
```

This creates staging schemas alongside production, each with a deploy ID suffix. By default, the deploy ID is the first 7 characters of your current **git commit SHA** — so it's already deterministic and tied to your code. Note the **deploy ID** in the output — you'll need it for the next steps. What's your deploy ID?"

WAIT FOR: User reports the deploy ID.

STEP 36: Inspect the deployment.

Say: "Let's inspect what's been staged:

```
mz-deploy list
```

This shows all active staging deployments. You can also get details:

```
mz-deploy describe <deploy-id>
```

(Replace `<deploy-id>` with your actual ID.) What do you see?"

WAIT FOR: User reports list/describe output.

STEP 37: Wait for hydration.

Say: "Materialized views need time to **hydrate** — to compute their initial results from the upstream data. Wait for them:

```
mz-deploy wait <deploy-id>
```

This polls hydration progress and exits when complete. Our dataset is small, so it finishes fast. What does it show?"

WAIT FOR: User confirms hydration is complete.

STEP 38: Promote to production.

Say: "The moment of truth — promote the staged deployment to production:

```
mz-deploy promote <deploy-id>
```

This atomically swaps the staging schemas into production. What does the output show?"

WAIT FOR: User confirms promotion succeeded.

STEP 39: Review promotion history.

Say: "Check the promotion log:

```
mz-deploy log
```

This shows the history of deployments. Your first successful promotion should appear.

A few more things to know:
- **`abort <deploy-id>`** — cancels a staged deployment without promoting
- **Concurrent deployments** — mz-deploy detects conflicts if another deployment is in progress"

CHECKPOINT: Say: "The Great Forging is complete. The ontology lives in production. But it has a critical weakness we need to address. Ready to raise the Citadel in Module 7?"

WAIT FOR: User confirmation to proceed.

---

## MODULE 7: "Raising the Citadel" — Stable API Pattern

STEP 40: Explain the problem.

Say: "**Module 7: Raising the Citadel** — Everything we've built works, but it has a critical weakness.

You just experienced the deployment lifecycle firsthand. Remember what `promote` did? It performed an atomic schema swap — the staging schema replaced the production schema. Here's the problem:

1. `stage` creates staging schemas with suffixed names (e.g., `public_dpl123`)
2. `promote` does an atomic `ALTER ... SWAP` of the entire schema
3. Every object in the schema is **replaced** — its identity changes

This means any downstream consumer **outside this project** that references `materialize.public.customers` will **break**. The old object was swapped out. Even within the project, all dependents of changed objects are redeployed, causing cascading redeployments.

```
Before promote:   public.customers  (object id: u100)
After promote:    public.customers  (object id: u200)  ← new object

Downstream query on u100 → breaks
```

For an ontology that other kingdoms depend on, this is unacceptable."

STEP 41: Introduce the solution.

Say: "The solution: **`SET api = stable`**.

A **schema mod file** — a file at `models/<database>/<schema_name>.sql` (sibling to the schema directory) — can set schema-level properties. When a schema is marked `SET api = stable`:

- Only **materialized views** are allowed in that schema
- Changed MVs use Materialize's **replacement protocol**: `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT`
- This updates the MV's internal computation **in place** — the name and identity are preserved
- Downstream consumers **never see a disruption** and never need to be redeployed

This is the citadel: an unbreakable API surface."

STEP 42: Explain the constraints.

Say: "Stable API schemas have strict rules:
- **Only materialized views** — no tables, views, sources, or sinks
- Changed replacement MVs do **not** propagate dirtiness to downstream dependents
- Only `stable` is supported (no other API levels)"

STEP 43: Explain the two-schema layout.

Say: "The pattern uses two schemas:

- **`internal`** — all transformation logic lives here as regular views with indexes. This is where the 'work' happens. Changes here trigger normal schema swaps (within the project only).
- **`public`** (`SET api = stable`) — thin materialized views that simply `SELECT` from internal views. These are the **contract** other teams depend on.

Internal logic can change freely. The stable MVs absorb changes via replacement without disrupting anyone.

```
raw.customers ─┐
raw.orders ────┤   internal schema        public schema (stable)
raw.products ──┘   (views + indexes)      (thin MVs)
                    ┌──────────────┐      ┌───────────────────┐
                    │ customers    │─────►│ customers (MV)    │──► external
                    │ orders       │─────►│ orders (MV)       │   consumers
                    │ products     │─────►│ products (MV)     │
                    └──────────────┘      └───────────────────┘
                    schema swap            replacement protocol
                    (identity changes)     (identity preserved)
```

Let's build it."

STEP 44: Create the schema mod file.

Create `models/materialize/public.sql`:
```sql
SET api = stable;
```

Say: "One line raises the citadel wall. Every MV in `models/materialize/public/` now uses the replacement protocol."

STEP 45: Create internal views with indexes.

Create `models/materialize/internal/customers.sql`:
```sql
CREATE VIEW customers AS
SELECT
    id,
    name,
    email,
    upper(name) AS display_name,
    created_at
FROM raw.customers
WHERE active = true;

CREATE INDEX customers_idx IN CLUSTER compute ON customers (id);
```

Say: "The customers logic moves to `internal`, and we've added `display_name` — a derived field. The index speeds up lookups by id."

Create `models/materialize/internal/orders.sql`:
```sql
CREATE VIEW orders AS
SELECT
    o.id,
    o.customer_id,
    c.name AS customer_name,
    o.product_id,
    o.quantity,
    o.status,
    o.created_at
FROM raw.orders o
JOIN internal.customers c ON o.customer_id = c.id;

CREATE INDEX orders_idx IN CLUSTER compute ON orders (customer_id);
```

Say: "Orders now joins against `internal.customers` instead of the public view."

Create `models/materialize/internal/products.sql`:
```sql
CREATE VIEW products AS
SELECT
    id,
    name,
    description,
    price,
    inventory
FROM raw.products
WHERE inventory > 0;

CREATE INDEX products_idx IN CLUSTER compute ON products (id);
```

Say: "Products — filtered to in-stock items, indexed by id."

STEP 46: Rewrite public views as thin MVs.

Rewrite `models/materialize/public/customers.sql` (replacing all previous content including the test):
```sql
CREATE MATERIALIZED VIEW customers
IN CLUSTER compute
AS SELECT
    id,
    name,
    email,
    display_name,
    created_at
FROM materialize.internal.customers;

COMMENT ON MATERIALIZED VIEW customers IS 'Active customers - stable ontology entity';

EXECUTE UNIT TEST test_filters_inactive
FOR materialize.public.customers
MOCK materialize.internal.customers(id bigint, name text, email text, display_name text, created_at timestamptz) AS (
    SELECT * FROM VALUES
        (1, 'alice', 'alice@example.com', 'ALICE', '2024-01-01T00:00:00Z'::timestamptz)
),
EXPECTED(id bigint, name text, email text, display_name text, created_at timestamptz) AS (
    SELECT * FROM VALUES
        (1, 'alice', 'alice@example.com', 'ALICE', '2024-01-01T00:00:00Z'::timestamptz)
);
```

Say: "Notice the conventions:
- **`CREATE MATERIALIZED VIEW`** — not `VIEW`. Stable schemas require MVs.
- **`IN CLUSTER compute`** — MVs must specify their cluster.
- **No `SELECT *`** — always list columns explicitly. This *is* the API contract.
- **`COMMENT ON`** — documents what the entity represents.
- **Thin** — no business logic, just a pass-through from the internal view.
- The test now mocks `internal.customers` (the direct dependency), not `raw.customers`."

Rewrite `models/materialize/public/orders.sql` (replacing all previous content including the test):
```sql
CREATE MATERIALIZED VIEW orders
IN CLUSTER compute
AS SELECT
    id,
    customer_id,
    customer_name,
    product_id,
    quantity,
    status,
    created_at
FROM materialize.internal.orders;

COMMENT ON MATERIALIZED VIEW orders IS 'Orders enriched with customer info - stable ontology entity';

EXECUTE UNIT TEST test_passthrough
FOR materialize.public.orders
MOCK materialize.internal.orders(id bigint, customer_id bigint, customer_name text, product_id bigint, quantity int, status text, created_at timestamptz) AS (
    SELECT * FROM VALUES
        (100, 1, 'alice', 10, 2, 'pending', '2024-01-15T00:00:00Z'::timestamptz)
),
EXPECTED(id bigint, customer_id bigint, customer_name text, product_id bigint, quantity int, status text, created_at timestamptz) AS (
    SELECT * FROM VALUES
        (100, 1, 'alice', 10, 2, 'pending', '2024-01-15T00:00:00Z'::timestamptz)
);
```

Say: "Two stable MVs converted. Now it's your turn."

STEP 47: Your turn to raise a wall.

Say: "You've seen the pattern twice now — thin materialized view, explicit columns, IN CLUSTER, COMMENT ON.

Rewrite `models/materialize/public/products.sql` as a stable MV that:
- Creates a `MATERIALIZED VIEW` (not `VIEW`) `IN CLUSTER compute`
- Selects `id`, `name`, `description`, `price`, `inventory` from `materialize.internal.products`
- Lists columns explicitly (no `SELECT *`)
- Has a `COMMENT ON MATERIALIZED VIEW` describing it

Then compile and test:

```
mz-deploy compile && mz-deploy test
```"

WAIT FOR: User confirms compile and test pass.

IF ISSUES: Show the correct SQL and help debug. The expected solution is:
```sql
CREATE MATERIALIZED VIEW products
IN CLUSTER compute
AS SELECT
    id,
    name,
    description,
    price,
    inventory
FROM materialize.internal.products;

COMMENT ON MATERIALIZED VIEW products IS 'In-stock products - stable ontology entity';
```
Common mistakes: forgetting IN CLUSTER, using VIEW instead of MATERIALIZED VIEW, using SELECT *.

STEP 48: Validate the refactored structure.

Say: "Let's make sure everything holds together. In your second terminal:

```
mz-deploy compile
```

Then:

```
mz-deploy test
```

Both should pass. What do you see?"

WAIT FOR: User confirms compile and test both succeed.

STEP 49: Introduce constraints.

Say: "The Citadel has walls — now it needs a seal. Before we deploy, let's declare the data quality guarantees that make this ontology trustworthy.

**Constraints** are unique to mz-deploy — they declare data quality rules on objects. Three kinds:
- **PRIMARY KEY** — no duplicate values
- **UNIQUE CONSTRAINT** — no duplicate values (same semantics as PK)
- **FOREIGN KEY** — every value in child columns must exist in the referenced parent

Two modes:
- **Enforced** (default) — compiled into a companion materialized view that continuously monitors for violations. Requires `IN CLUSTER`.
- **NOT ENFORCED** — metadata-only, recorded for documentation. No monitoring MV. `IN CLUSTER` is forbidden.

**Important:** Enforced constraints lower to companion MVs — new objects in the schema. They must be in the files **before** we `stage`, because once a stable schema is promoted, you can't add new objects to it without a fresh deployment."

STEP 50: Add an enforced PRIMARY KEY to customers.

Edit `models/materialize/public/customers.sql` to append the constraint after the existing `CREATE MATERIALIZED VIEW` statement:
```sql
CREATE PRIMARY KEY customers_pk IN CLUSTER compute ON customers (id);
```

Say: "This is an **enforced** primary key. Under the hood, mz-deploy compiles it into a companion materialized view on the `compute` cluster that continuously checks for duplicate `id` values — something like:

```sql
SELECT id, count(*) FROM customers GROUP BY id HAVING count(*) > 1
```

If the result is ever non-empty, you have a violation. The companion MV participates in the full deployment pipeline — dependency tracking, blue/green, change detection — but it's hidden from explorer docs."

STEP 51: User writes a FOREIGN KEY on orders.

Say: "Your turn, Lorekeeper. Take the quill.

Add an **enforced FOREIGN KEY** to `models/materialize/public/orders.sql` that ensures every `customer_id` references a real customer in `materialize.public.customers (id)`. Name it `orders_customer_fk` and run it on the `compute` cluster.

Append the constraint after the existing `CREATE MATERIALIZED VIEW` statement, then tell me when you're done."

WAIT FOR: User confirms they've written the constraint.

Say: "Here's the expected result:

```sql
CREATE FOREIGN KEY orders_customer_fk IN CLUSTER compute ON orders (customer_id) REFERENCES materialize.public.customers (id);
```

Common mistakes:
- Forgetting `REFERENCES` — FK always needs a target
- Omitting `IN CLUSTER` — enforced constraints require it
- Wrong column name — the child column is `customer_id`, not `id`

If yours doesn't match, go ahead and fix it now."

STEP 52: Add a NOT ENFORCED UNIQUE on products.

Edit `models/materialize/public/products.sql` to append the constraint after the existing statement:
```sql
CREATE UNIQUE CONSTRAINT NOT ENFORCED products_name_unique ON products (name);
```

Say: "This one is **not enforced** — it's metadata-only. It documents the expectation that product names are unique without creating a monitoring MV. Notice: no `IN CLUSTER` clause. Not-enforced constraints are forbidden from specifying a cluster."

STEP 53: Compile and verify with constraints.

Say: "Let's make sure everything compiles cleanly with the constraints in place. Run:

```
mz-deploy compile --verbose
```

In the verbose output, look for:
- Constraint validation passing for all three constraints
- The two enforced constraints (`customers_pk`, `orders_customer_fk`) appearing as companion MVs in the plan
- The not-enforced constraint (`products_name_unique`) appearing as metadata only — no companion MV

What do you see?"

WAIT FOR: User confirms compile succeeds.

STEP 54: Deploy the refactored ontology with constraints.

Say: "Now let's deploy the refactored ontology — stable MVs and constraints together. Run the full cycle:

```
mz-deploy stage
```

Note the deploy ID, then:

```
mz-deploy wait <deploy-id>
mz-deploy promote <deploy-id>
```

Watch the promote output carefully — the `internal` schema swaps normally, the `public` stable MVs use the **replacement protocol** (updated in place with identity preserved), and the enforced constraints stage their companion MVs alongside the stable API.

What does the output show?"

WAIT FOR: User confirms the deploy cycle succeeds.

Say: "The fulfillment application in Module 9 will safely depend on `materialize.public.*` knowing it will **never break**, even when the ontology team changes internal logic."

CHECKPOINT: Say: "The Citadel stands — stable API and data quality guarantees deployed together. Ready to verify the Seal of Trust in Module 8?"

WAIT FOR: User confirmation to proceed.

---

## MODULE 8: "The Seal of Trust" — Verifying Constraints

STEP 55: Explain what happened during deployment.

Read `references/constraints.md` for the full specification before proceeding.

Say: "**Module 8: The Seal of Trust**

During that deployment, something interesting happened behind the scenes. The two **enforced** constraints (`customers_pk` and `orders_customer_fk`) each compiled into a **companion materialized view** — a hidden MV that continuously monitors for violations. These companion MVs were staged, hydrated, and promoted alongside your stable API objects, fully participating in the blue-green lifecycle.

The **not-enforced** constraint (`products_name_unique`) was recorded as metadata only — no companion MV, no compute cost. It documents the expectation without enforcing it."

STEP 56: Explore to see constraints.

Say: "Let's see how constraints appear in the docs:

```
mz-deploy explore
```

Look at the object detail cards for `customers`, `orders`, and `products`. Each one shows its constraints. But the companion MVs themselves are hidden — they're implementation details, not part of your public ontology."

WAIT FOR: User confirms they see constraints in the docs.

STEP 57: Checkpoint.

CHECKPOINT: Say: "The Seal of Trust is verified. Your ontology declares its data quality guarantees. Ready to build the Kingdom of Fulfillment in Module 9?"

WAIT FOR: User confirmation to proceed.

---

## MODULE 9: "The Kingdom of Fulfillment" — Building an Application

STEP 58: Introduce the application pattern.

Say: "**Module 9: The Kingdom of Fulfillment**

In the data mesh pattern:
- The **ontology** defines canonical entities with a stable API
- **Applications** build domain-specific logic on top of the ontology's public API
- Each application runs on its own cluster and deploys independently
- Applications reference `materialize.public.*` — the stable API we created in Module 7

The fulfillment application will track pending orders and shipment readiness."

STEP 59: Create the fulfillment cluster.

Create `clusters/fulfillment.sql`:
```sql
CREATE CLUSTER fulfillment SIZE = '50cc';
```

Say: "A dedicated cluster for the fulfillment domain. It scales independently of the ontology's compute cluster."

STEP 60: Create fulfillment views.

Create `models/materialize/fulfillment/pending_orders.sql`:
```sql
CREATE VIEW pending_orders AS
SELECT
    o.id AS order_id,
    o.customer_id,
    o.customer_name,
    o.product_id,
    o.quantity,
    o.created_at AS ordered_at
FROM materialize.public.orders o
WHERE o.status = 'pending';

CREATE INDEX pending_orders_idx IN CLUSTER fulfillment ON pending_orders (customer_id);
```

Say: "Pending orders — filtered from the ontology's stable `orders` entity. Notice it references `materialize.public.orders`, the stable MV."

Create `models/materialize/fulfillment/shipment_readiness.sql`:
```sql
CREATE VIEW shipment_readiness AS
SELECT
    po.order_id,
    po.customer_name,
    p.name AS product_name,
    po.quantity AS ordered_quantity,
    p.inventory AS available_inventory,
    CASE
        WHEN p.inventory >= po.quantity THEN 'ready'
        ELSE 'insufficient_stock'
    END AS readiness_status
FROM pending_orders po
JOIN materialize.public.products p ON po.product_id = p.id;
```

Say: "Shipment readiness — joins pending orders with product inventory from the ontology. This tells the fulfillment team which orders can ship and which are blocked by low stock."

STEP 61: Compile and validate.

Say: "Let's validate the cross-schema references:

```
mz-deploy compile
```

mz-deploy resolves dependencies across schemas — fulfillment views depend on public MVs, which depend on internal views, which depend on raw tables. What does compile show?"

WAIT FOR: User confirms compile succeeds.

STEP 62: Deploy the fulfillment application.

Say: "Now we need to apply the new fulfillment cluster, then deploy the new views. First:

```
mz-deploy apply --dry-run
```

This shows the new `fulfillment` cluster. If it looks right:

```
mz-deploy apply
```"

WAIT FOR: User confirms apply of the new cluster.

```
mz-deploy stage
```

Say: Notice: only the **new and changed** objects are staged. The existing ontology objects are untouched. If the dry-run looks right, run the full cycle:

Note the deploy ID, then:

```
mz-deploy wait <deploy-id>
mz-deploy promote <deploy-id>
```

What does the final promotion show?"

WAIT FOR: User confirms the full deploy cycle succeeds.

STEP 63: Explain the power of this pattern.

Say: "The ontology team can update internal logic — change how `display_name` is computed, add new columns, refactor joins — and the fulfillment application **never breaks**. The stable public MVs absorb changes via replacement. Both teams deploy independently on their own clusters.

```
Postgres ──► raw ──► internal ──► public (stable API)
│
├──► fulfillment.pending_orders
└──► fulfillment.shipment_readiness
(independent cluster + deploy)
```

**Independent teams, shared truth, stable contracts.**"

CHECKPOINT: Say: "The Kingdom of Fulfillment stands. Ready to raise the Watchtower in Module 10?"

WAIT FOR: User confirmation.

---

## MODULE 10: "The Watchtower" — CI/CD & Automation

STEP 64: Introduce CI/CD patterns.

Say: "**Module 10: The Watchtower** — You've been deploying by hand. That works for learning, but production needs automation.

This module covers the mz-deploy flags and patterns for CI/CD: machine-readable output, exit codes, a pipeline skeleton, and rollback."

STEP 65: Teach `--output json` for pipeline integration.

Say: "For machine-readable output, most mz-deploy commands support `--output json`. Three patterns you'll use in CI:

1. **Plan artifact for PR review:**
   ```
mz-deploy stage --dry-run --output json
   ```
   Produces a JSON plan you can attach to a PR comment — reviewers see exactly what will change.

2. **Hydration health check:**
   ```
mz-deploy wait --output json --once
   ```
   Returns hydration status as JSON. `--once` checks once and exits (no polling) — useful for health checks.

3. **Programmatic deployment discovery:**
   ```
mz-deploy list --output json
   ```
   Lists all deployments as JSON — useful for dashboards and cleanup scripts.

Try running `mz-deploy list --output json` in your terminal to see the JSON output."

WAIT FOR: User confirms they see the JSON output.

STEP 66: Teach exit codes.

Say: "Every mz-deploy command exits **0 on success, 1 on failure**. Your CI pipeline gates on this — if any step fails, the pipeline stops.

You can check per-command exit code documentation with:

```
mz-deploy help <command>
```

One particularly useful CI pattern is generating JUnit XML test results (which you already learned in Module 5):

```
mz-deploy test --junit-xml test-results.xml
```

GitHub Actions, Jenkins, and other CI tools parse this format to show test results inline in PRs. The command exits 1 if any test fails, failing the pipeline."


## SESSION COMPLETION: "The Quest is Complete"

STEP 67: Introduce the explore command.

Say: "One more thing before we close the book, Lorekeeper. You've built an entire data mesh — sources, transformations, stable APIs, an application, tests, and CI patterns. Let's see it all at once.

mz-deploy can generate interactive documentation from your project — a self-contained HTML page with a dependency DAG, object details, schema browser, governance matrix, and more. No database connection needed.

Run:

```
mz-deploy explore
```

This compiles the project and opens the documentation in your browser."

WAIT FOR: User confirms the docs page opened in their browser.

Say: "Take a minute to explore what you've built:
- The **Overview** shows your domains, object counts, and pipeline depth
- The **DAG** visualizes the full dependency graph — click a domain to drill into individual objects, click an object to trace its lineage upstream and downstream
- Browse **Object details** to see SQL definitions, dependencies, indexes, grants, and tests
- Check **Data Contracts** to see external dependency schemas from `types.lock`
- The **Governance** tab shows your role-to-schema privilege matrix

Everything you built across all ten modules is here in one place.

If you ever want to generate the docs without opening the browser, use `mz-deploy explore --no-open`."

STEP 68: Summary and celebration.

Say: "**The Quest is Complete, Lorekeeper.**

Let's look back at the journey:

1. **The Map of the Realm** — You learned the project structure and mz-deploy conventions
2. **Opening the Eastern Gate** — You connected to Postgres and ingested data via CDC
3. **Forging the First Artifacts** — You created views and wrote your first SQL by hand
4. **The Trial of Validation** — You used `compile` to catch errors locally
5. **The Crucible of Proof** — You wrote unit tests with `EXECUTE UNIT TEST` — including one yourself
6. **The Great Forging** — You deployed to production with `stage -> wait -> promote`
7. **Raising the Citadel** — You built the stable API pattern with `SET api = stable`, declared constraints, and deployed them together
8. **The Seal of Trust** — You verified constraints post-deployment and explored them in the docs
9. **The Kingdom of Fulfillment** — You built an application on top of the ontology
10. **The Watchtower** — You learned CI/CD patterns: `--output json`, exit codes, pipeline design, and rollback

**Next steps for your realm:**
- Add more ontology entities (payments, shipments, inventory events)
- Build more application kingdoms (analytics, storefront, notifications)
- Run `mz-deploy explore` after changes to see how your project evolves
- Explore `mz-deploy help --all` for advanced features

Keep the tutorial files — this is a working project you can continue to build on. May your queries always converge, Lorekeeper."

## END OF SESSION INSTRUCTIONS
