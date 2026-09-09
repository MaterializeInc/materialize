---
name: mz-demo-data
description: >
  Trigger: "demo data", "synthetic data", "live operational data", "generate
  fake data", "create a demo", "load generator alternative", or wants
  realistic continuously-updating data in Materialize for a demo. Also "I
  need streaming data to show off X" or "build a demo schema for <domain>".
  Use this to stand up auctions/bids, ecommerce, banking, IoT, or
  clickstream demos — or to design a new domain in the same style.
---

# Live operational demo data, generated entirely in SQL

This skill builds continuously-updating, realistic synthetic data inside a
running Materialize instance — no Kafka, no external load generator, no
seed scripts. Everything is plain views over `mz_now()`.

The technique comes from [this blog post][blog]. Use the catalog at the
bottom for an existing domain; use the rubric to design new ones.

[blog]: https://github.com/frankmcsherry/blog/blob/master/posts/2024-05-19.md

## Quickstart

```sh
psql -p 6875 -h localhost -U materialize -f assets/scaffold.sql
psql -p 6875 -h localhost -U materialize -f assets/common/people.sql
psql -p 6875 -h localhost -U materialize -f assets/domains/auctions.sql
```

Re-running `scaffold.sql` or `people.sql` is safe — both short-circuit if
already loaded. Re-running a domain file errors on the second `CREATE
VIEW`; run `assets/teardown.sql` first to rebuild.

Then in a psql session:

```sql
COPY (SUBSCRIBE (SELECT COUNT(*) FROM auctions) WITH (progress = true)) TO STDOUT;
```

You should see a heartbeat tick once per second. The count grows for the first
24 hours, then stabilizes at retention/tick (default 86,400).

To change the window or tick rate, set them before `\i`:

```sql
\set retention '6 hours'
\set tick '1 second'
\i assets/scaffold.sql
```

To tear everything down: `psql -f assets/teardown.sql`.

## First contact: walking a fresh user from zero to demo

**Read this section first if you're an agent and a user shows up wanting a
demo.** The skill is meant to feel guided, not like a SQL reference. Drive
the user through these turns:

### Turn 1 — establish the connection

Ask once if it isn't already in context:

> What's the connection? A `psql` command, a `MATERIALIZE_URL`, or
> host/port/user/db?

Accept any of: a connection string, env var, or the four flags. Don't
proceed until you can run `psql -c 'SELECT mz_version();'` and get a row
back. If they say "localhost defaults", use
`psql -p 6875 -h localhost -U materialize -d materialize`.

### Turn 2 — pick a domain

If the user named one ("show me a banking demo"), check the catalog at the
bottom of this file. If it matches, jump to Turn 5 with that file.

If they have a domain in mind that isn't in the catalog ("I work in
logistics", "we do telecom billing", "give me healthcare"), continue to
Turn 3 — we'll build a new domain together. **Do not** force-fit their
domain onto one in the catalog; that's worse than a fresh one.

If they don't have a domain in mind, suggest 2–3 from the catalog with a
one-line hook each and let them pick.

### Turn 3 — propose the model in plain language

**Not SQL yet.** Show the user the entities and relationships in English,
plus the invariant you'd bake in. Format:

> Here's what I'd build:
> - **\<TopEntity\>** (one per moment) — fields: a, b, c
> - **\<ChildEntity\>** (3–5 per top-level) — fields: x, y
> - **\<Lookup\>** (static, N rows) — purpose
>
> **Invariant baked in:** \<one sentence — what must always hold\>
>
> **Joins to people?** \<yes/no, and why\>
>
> Sound right, or different entities?

Wait for their reaction. **Don't write SQL until they sign off.** They will
usually correct one of: an entity name, a missing field, the invariant, or
the cardinality. Cheap to iterate here, expensive later.

### Turn 4 — show the byte budget

Once entities are confirmed, show the byte allocation explicitly:

> Per top-level row, 16 random bytes:
> ```
> [0..2]  id              24-bit
> [3]     person_id       mod 256, FK to people
> [4]     kind            mod N
> [5]     n_children      1..K
> [6]     time offset     for due_at
> [7..]   free
> ```
> Children re-hash `(parent.random || child_index)` for their own bytes.

This is the last cheap-to-change step. If the user wants higher cardinality
in some field, swap bytes here, not later.

### Turn 5 — write and load

Copy `assets/domains/_template.sql` to `assets/domains/<their>.sql` and
fill it in following the byte budget. Then:

```sh
# scaffold is idempotent — safe to run even if already loaded.
psql ... -f assets/scaffold.sql
psql ... -f assets/common/people.sql    # only if domain joins people
psql ... -f assets/domains/<their>.sql
```

If a previous load of THIS domain is present, scaffold will skip silently
but the domain file will fail on `CREATE VIEW ... already exists`. In that
case, run `assets/teardown.sql` first to wipe and reload.

### Turn 6 — prove it works, hand over the wheel

Show the user three queries:

1. **Heartbeat** — confirms data is flowing:
   ```sql
   COPY (SUBSCRIBE (SELECT COUNT(*) FROM <top_table>) WITH (progress = true)) TO STDOUT;
   ```
2. **Invariant** — confirms correctness; should return the expected fixed
   value (usually 0).
3. **One "cool" query** — the demo payoff. For aggregations,
   `SELECT ... GROUP BY ...`; for cross-domain, a join through `people`.

Then offer: "Want to layer another domain on top? You'll see joins through
the shared `people` table stay live too."

### Things to *not* do during first contact

- Don't dump the catalog up front. The user came with a domain or wants
  guidance — give them one path, not a menu.
- Don't ship SQL without the plain-language proposal first.
- Don't skip the byte budget. It's where misalignments hide (e.g., two
  fields sharing byte 5 by accident).
- Don't claim "it's done" until the heartbeat ticks and the invariant
  query returns its expected value against THEIR Materialize instance.
- Don't add columns the user didn't ask for. The template's free bytes
  exist; leave them free unless the user names a use.

## The two-layer model

Every demo built with this skill has exactly two layers:

1. **Scaffold** (stable). The `moments` view — a sliding window of timestamps
   — and the `random` view — MD5(moment) producing 16 deterministic bytes per
   moment. **Do not edit per-domain.** Always loaded first; the same scaffold
   serves every domain. Lives in `assets/scaffold.sql`.

2. **Domain** (creative). Views that turn `random` bytes into entities,
   relationships, and aggregates. One file per domain in `assets/domains/`.
   Domains can compose: load multiple and they cross-join naturally via the
   shared `people` table.

Knowing where the line is matters. If you find yourself wanting to edit the
scaffold from a domain file, you almost certainly want to add another view
on top instead.

## The four design rules for a domain

Every domain in the catalog follows these four rules. Apply them when
designing a new one.

### 1. PKs are derived deterministically from `moment`

A row's primary key is a function of its moment's random bytes:

```sql
get_byte(random, 0) + get_byte(random, 1) * 256 + get_byte(random, 2) * 65536  AS id
```

**Why:** the same moment always yields the same id, so re-derivation is stable
even though the underlying view is a sliding window. PKs naturally vanish from
the system when their moment falls out of retention.

### 2. FKs come from re-derivation, not declared constraints

A child row is generated by re-hashing the parent's random bytes:

```sql
WITH expanded AS (
    SELECT id AS parent_id, ...
           digest(random::text || generate_series(1, n_children)::text, 'md5') AS random
    FROM parent_core
)
SELECT ... FROM expanded;
```

**Why:** when the parent's moment falls out of retention, every child derived
from it vanishes simultaneously. Referential integrity for free, no
`FOREIGN KEY` declarations needed. This is the key insight that makes the
whole approach work.

### 3. Distributions are byte-mask choices

Cardinality is controlled by which bytes you read:

| Pattern                                | Cardinality      |
|----------------------------------------|------------------|
| `get_byte(random, 0)`                  | 256              |
| `get_byte(random, 0) + get_byte(random, 1) * 256` | 65,536 |
| `mod(get_byte(random, 0), 5)`          | 5                |
| `get_byte(random, 0) < 80`             | ~31% boolean     |

**Why:** explicit and predictable. A 256-account banking demo uses one byte
mod 64 for from-account; a 16M-id auction marketplace uses three bytes. Pick
the cardinality you need.

### 4. Evolution is `moment + interval`

Time-relative fields are derived from the moment plus a random offset:

```sql
moment + (get_byte(random, 6)::text || ' minutes')::interval AS end_time
```

**Why:** the field is monotone in the moment, so downstream views can filter
on it sensibly. Combined with rule #1, this gives you a temporal lifecycle
(start, end, expiry) tied to the entity's identity.

## Invariant-by-construction patterns

The strongest demos rely on invariants that **cannot be violated** because of
how the data is constructed. Two patterns:

**Sum-to-zero (double entry).** Each event emits two child rows with opposite
signs (`banking.sql`). `SUM(ledger_entries.amount) = 0` holds at every
consistent timestamp, regardless of concurrent transaction volume. This is
the headline Materialize correctness demo and it's almost impossible to fake
with off-the-shelf data.

**Count matches declared fanout.** A parent row declares `n_items`, and uses
`generate_series(1, n_items)` to emit children. `COUNT(children) per parent =
parent.n_items` is invariant (`ecommerce.sql`).

When designing a new domain, look for an invariant of this form. A demo
without one is weaker — it shows speed but not correctness.

## Invariants vs. id collisions

The blog's construction derives primary keys from 24 bits of MD5 entropy
(`get_byte(random, 0..2)` packed into an int). At default settings — 86,400
moments in retention — the birthday paradox guarantees roughly 230–500
id collisions in steady state. **This is intentional and expected.** The
blog accepts it; this skill keeps it for fidelity to the blog and to keep
parent ids in a tractable range.

Collisions affect which invariant shapes you can honestly claim:

**Survives collisions (claim freely):**
- Aggregate equalities. `COUNT(children) = SUM(n_children over all parents)`.
- Sum-to-zero. `SUM(ledger_entries.amount) = 0`.
- Subset / FK by re-derivation. `every child.parent_id has matching parent` —
  re-derivation guarantees this even when multiple parents share an id (the
  child joins to *some* row with that id).
- Distribution-shape claims. `~10% of page_views are /checkout`.

**Does NOT survive collisions (don't claim, or restate as aggregate):**
- Per-parent fanout equality. `for every parent, COUNT(children with this id) = parent.n_children`.
  Two parents sharing an id will sum their children, breaking the equality.
- Per-id uniqueness. `every id appears exactly once`.
- Per-row temporal monotonicity that depends on id grouping.

When the user asks "what's the invariant," default to the aggregate form
unless the construction makes per-id claims structurally true (e.g.
double-entry, where each transaction emits exactly two entries no matter
what id arithmetic does).

If a demo *requires* unique ids (e.g. a customer-facing dashboard that
treats id as a key), the fix is to either widen the id space (use 4+
bytes), use `EXTRACT(EPOCH FROM moment)::bigint` as the id (collision-free
by construction), or accept ~0.5% noise as a feature ("real data is
messy"). All three are reasonable; the catalog uses the blog's 24-bit
form for fidelity.

## Adapting to a new domain

When walking a user through this, follow the **First contact** protocol
above — propose-then-confirm, don't write SQL upfront. The mechanical
checklist is below.

To add `assets/domains/<yours>.sql`, work through these steps in order:

1. **List the entities.** Top-level (one per moment), child rows (per parent
   via fanout), shared lookups (static).
2. **Pick the byte budget for each top-level entity.** Random gives you 16
   bytes per moment. Allocate bytes to fields by cardinality:
   id (3 bytes), FK to people (1 byte mod 256), category (1 byte mod N),
   timing offsets (1 byte), free for amount/value (2–3 bytes).
3. **Decide which existing shared tables you reference.** Today: `people`. If
   you reference people, your domain joins the cross-domain "same person
   appears in multiple data products" story automatically.
4. **Identify the invariant.** What sum, count, or equality must hold by
   construction? Bake it in via fanout or two-legged emission, not as a
   `CHECK` constraint.
5. **Copy the template.** `cp assets/domains/_template.sql assets/domains/<yours>.sql`
   and fill in the TODOs. The template already encodes the standard
   `_core` view → public MV → child MV → aggregate VIEW pattern and an
   idempotency guard. Keep that structure.
6. **Add two validation queries** at the bottom: a `SUBSCRIBE` heartbeat
   and an invariant query that should always return 0 (or a fixed value).

## Validation

For every domain, two queries:

```sql
-- Heartbeat: confirms the domain is live and producing rows.
COPY (SUBSCRIBE (SELECT COUNT(*) FROM <table>) WITH (progress = true)) TO STDOUT;

-- Invariant: should return a fixed value (usually 0).
SELECT ... ;
```

If the heartbeat ticks but the invariant drifts, the byte budget is
misaligned — likely two entities sharing the same byte for different
purposes. Re-allocate.

## The catalog

| File | Domain | Highlights | Joins `people`? |
|---|---|---|---|
| `domains/auctions.sql`   | Auctions & bids                | Marketplace; lifecycle (end_time); winning-bid demo            | No |
| `domains/ecommerce.sql`  | Orders, line items, totals     | Multi-row child fanout; `order_totals` aggregate-as-view       | Yes |
| `domains/banking.sql`    | Accounts, double-entry txns    | **`SUM(balances) = 0`** invariant. Strongest correctness demo. | Yes |
| `domains/iot.sql`        | Devices, readings, alerts      | High cardinality; threshold alerts; per-site rollup            | No |
| `domains/clickstream.sql`| Sessions, page views, funnel   | Funnel analytics; conversion rate by channel                   | Yes |
| `domains/zoo.sql`        | Zoo visits, ratings, shipments | Four invariants at once, richest domain. Front-of-house ratings correlate with back-of-house skim by construction. | Yes |

Loading more than one ecommerce/banking/clickstream domain at once gives you
the cross-product demo: "show me Person 042's orders, her transactions, and
her browsing session — all live, all consistent."

## File map

```
misc/demo-data/
├── SKILL.md                  ← you are here
├── README.md                 short human intro
└── assets/
    ├── scaffold.sql          moments + random; do not edit per-domain
    ├── teardown.sql          drops everything
    ├── common/
    │   └── people.sql        256-identity pool, shared across domains
    └── domains/
        ├── _template.sql         ← copy this to start a new domain
        ├── auctions.sql
        ├── ecommerce.sql
        ├── banking.sql
        ├── iot.sql
        ├── clickstream.sql
        └── zoo.sql
```
