# DDL Transaction Conflict Retry

- Associated: (TBD, link issue)

## The Problem

Multi-statement DDL transactions abort with `DDLTransactionRace` ("another
session modified the catalog") whenever *any* catalog transaction commits while
they are open, whether or not the concurrent change has anything to do with the
transaction's contents.

The guard is `Catalog::transient_revision()`, an in-memory counter bumped once
per committed content transaction (`src/adapter/src/catalog/transact.rs`,
`transient_revision += 1` on commit). A DDL transaction records the revision
when it starts and errors on strict inequality at four sites:

1. **Statement arrival** (`handle_execute_inner`,
   `src/adapter/src/coord/command_handler.rs`): every blessed statement,
   including the second and later statements of an open transaction, merges an
   empty `TransactionOps::DDL` carrying the *current* revision into the
   session's transaction. The `add_ops` DDL/DDL arm errors on revision
   inequality, so a background commit between statements aborts the
   transaction here, before planning even starts.
2. **Op accumulation** (`catalog_transact_with_ddl_transaction`,
   `src/adapter/src/coord/ddl.rs`): revision mismatch returns
   `DDLTransactionRace` before the incremental dry run of the new ops.
3. **`TransactionStatus::add_ops`** (`src/adapter/src/session.rs`): the
   accumulated ops' revision must equal the incoming ops' revision. This is
   the same check as site 1 mechanically (site 1 *is* an `add_ops` call), but
   it also fires for the write-back at the end of site 2.
4. **`COMMIT`** (`sequence_end_transaction_inner`,
   `src/adapter/src/coord/sequencer/inner.rs`): revision mismatch returns
   `DDLTransactionRace` before the accumulated ops are replayed through
   `catalog_transact`.

This is a real user pain because the revision moves for reasons entirely
outside the user's control:

- **Background system transactions.** The cluster controller commits catalog
  transactions on its own reconciliation tick
  (`src/adapter/src/coord/cluster_controller.rs`, `commit_with_checks`), and
  the system parameter sync commits configuration updates. A replica
  scheduling decision or a parameter flip aborts every open DDL transaction.
- **Unrelated DDL from other sessions.** Single-statement DDL does not wait
  for open DDL transactions (DDL transactions do not hold the
  `serialized_ddl` lock, see `must_serialize_ddl` in
  `src/adapter/src/coord/command_handler.rs`), so any other session's
  `CREATE TABLE` kills an open, logically disjoint rename transaction.
- **Temporary items.** `CREATE TEMPORARY TABLE` from any session goes through
  `Catalog::transact` and bumps the revision.

DDL transactions are also naturally long-running. The statements that may run
inside one today (`AlterObjectRename`, `AlterObjectSwap`, `CreateSource`,
`CreateTableFromSource`, the blessed list in `handle_execute_inner`) include
the `CREATE SOURCE` flow, whose purification performs slow network calls
between statements. The longer the transaction is open, the more likely a
spurious abort.

Note the failure is keyed on the in-memory revision, not the durable catalog
upper. Pure upper advancement (for example the group committer's empty
progress writes) does not bump the revision and does not abort DDL
transactions. The spurious aborts come from unrelated *content* commits.

### Current state of the machinery

`TransactionOps::DDL` (`src/adapter/src/session.rs`) accumulates:

- `ops: Vec<catalog::Op>`: the fully planned catalog operations, in order.
- `state: CatalogState`: in-memory state reflecting the accumulated ops,
  produced by dry runs.
- `snapshot: Option<Snapshot>`: durable transaction snapshot after the last
  dry run, so subsequent dry runs are incremental.
- `revision: u64`: the transient revision the accumulated state is valid
  against.
- `side_effects`: closures to run after a successful commit.

Each statement runs `transact_incremental_dry_run` for its new ops against the
accumulated state. `COMMIT` replays the raw accumulated ops through the normal
`catalog_transact` path against the *current* catalog. That replay re-validates
everything the catalog knows how to validate: missing dependencies, name
collisions, and ownership violations all surface as errors from op
application. The revision equality check is a conservative blanket on top of
that replay.

## Success Criteria

- A DDL transaction is aborted only by a *real* conflict: a concurrent commit
  that touched an object the transaction reads, writes, or whose name it
  claims.
- Background system activity (cluster controller reconciliation, system
  parameter sync, other sessions' temporary items) never aborts a DDL
  transaction that does not reference the affected objects.
- Unrelated concurrent DDL never aborts a DDL transaction, at any of the four
  guard sites, including between statements.
- Real conflicts keep failing with SQLSTATE `serialization_failure` (40001)
  and an error message whose prefix is stable (existing tooling matches on
  it, see Error mapping).
- No weakening of catalog integrity: everything the revision check prevented
  is either prevented by the conflict test or by the commit replay itself.
- The conflict test is a reusable primitive: the future multi-`environmentd`
  catalog (a `CatalogOutOfSync` result at durable commit) needs exactly
  "given ops validated at state S1, decide whether they still apply at S2".

## Out of Scope

- Durable-level conflicts between multiple `environmentd` processes. In a
  single-process deployment all catalog transactions are serialized on the
  coordinator loop, so the durable commit can never observe a content
  conflict. This design keeps that assumption and only generalizes the
  in-memory guard. The conflict test it introduces is the piece the
  multi-writer path will later reuse, after foreign diff application through
  the catalog implications framework exists.
- Making DDL transactions fully serializable with respect to catalog *reads*.
  DDL transactions are structurally write-only against the catalog: `add_ops`
  rejects mixing `TransactionOps::DDL` with peeks or writes
  (`DDLOnlyTransaction` / related arms in `add_ops`), so there is no tracked
  read set to protect. Planning-time reads (name resolution, feature flags)
  are protected only insofar as they are reflected in the ops' footprint, see
  Limitations.
- Widening the set of statements allowed inside DDL transactions.

## Design Overview

Replace the strict revision equality at the guard sites with an **optimistic
revalidation**:

1. Keep a bounded, coordinator-owned **catalog update log**: for each
   committed catalog transaction, the revision it produced and a
   pre-classified summary of the state updates it applied.
2. When a guard site observes `transient_revision() != txn_revision`, compute
   the transaction's **footprint** from its accumulated ops and intersect it
   with the update log entries in `(txn_revision, current_revision]`.
3. **Disjoint**: no real conflict. Refresh the transaction, re-running a full
   dry run of the accumulated ops against the current catalog, store the new
   `state`, `snapshot`, and `revision`, and proceed as if no race happened.
   The user never sees an error.
4. **Intersecting** (or the log does not reach back far enough, or anything
   is unclassifiable): return `DDLTransactionRace`, exactly today's behavior.

### Serialization assumption, and the backstop

The revalidation and the dry run or replay that follows it run within a single
coordinator message handler. This design **assumes that every catalog content
commit executes on the coordinator loop**, so no commit can interleave between
the conflict test and the subsequent commit, even across the handler's
internal await points. That holds today: all production commits flow through
`catalog_transact_inner`, and off-loop work (the group committer) only ever
advances the durable upper with empty progress, which changes no content and
bumps no revision. This assumption is load-bearing and must be recorded in
`doc/developer/guide-adapter.md` (see Documentation) so off-loop work cannot
silently invalidate it.

This is exactly the shape that guide-adapter.md's "A compare-and-append must
be enforced by the transaction, not by a check before it" warns about: a check
separated from the write it guards, defended by loop serialization. We
therefore add a cheap mechanical backstop: the revision observed at
conflict-test time is carried into the commit call as an
`expected_revision`, and `Catalog::transact` fails with `DDLTransactionRace`
if the catalog's revision does not match when the commit actually runs. Under
the stated assumption the backstop never fires. If someone later breaks the
assumption, conflicts turn into clean serialization failures instead of silent
corruption.

### Why the naive alternative is unsound

"Drop the revision check and let the `COMMIT` replay decide" almost works but
misses conflicts that the replay applies *successfully yet wrongly*. Ops carry
artifacts bound at planning time that the replay does not re-derive.
A reachable example: the transaction planned
`CREATE TABLE t FROM SOURCE src ...`, storing in the op both resolved ids and
the `create_sql` text, which embeds the *name* of `src`. Concurrently `src` is
renamed and an unrelated source takes the name `src`. The replay's durable
insert succeeds by id, but the stored `create_sql` now names the wrong object.
The failure is immediate, not deferred: applying the committed update
re-deserializes `create_sql` against the current catalog
(`apply_item_update`), which either panics on unresolvable SQL ("invalid
persisted SQL") or silently binds the item to the wrong object. The footprint
test catches this class: the rename's update summary touches `src`'s id, which
is in the transaction's read set.

### The footprint

The footprint of a transaction is derived from its accumulated
`Vec<catalog::Op>` at conflict-test time (no new session state is required,
the ops are already stored). It has three components:

- `reads`: ids of objects whose existence or current definition the ops
  assume. This includes referenced items (via `CatalogItem::uses()` and
  `references()`), the schema and database an item lives in, the cluster an
  item is placed on, owner and grantee roles, and network policies.
- `writes`: ids the ops create, mutate, or drop.
- `names`: namespace keys the ops claim, as (scope, name) pairs. Scopes are
  keyed by stable ids: item names by `SchemaId`, schema names by
  `DatabaseId` (or the ambient scope), and the flat database, cluster, role,
  and network policy namespaces. Replica names are keyed by `ClusterId`.

**Name normalization.** Ops are inconsistent about how they carry names:
`Op::CreateItem` has a `QualifiedItemName` with id-based qualifiers, while
`Op::RenameItem` has a string-based `FullItemName`. Footprint extraction
normalizes all names into the id-scoped key space above, resolving
string-based scopes against the transaction's accumulated `state` (a
consistent catalog snapshot). If resolution fails, extraction degrades to a
global footprint, which reports a conflict. Extraction never parses SQL and
never consults the live catalog, so it stays cheap and runs only on revision
mismatch.

Extraction is total over `Op` with a conservative default: any op variant
without a precise rule yields a **global** footprint that conflicts with
everything, which degrades to today's behavior rather than under-reporting.

Footprint rules per op variant. "id" refers to the op's target id.

| Op | reads | writes | names |
|---|---|---|---|
| `CreateItem` | `item.uses()` + `item.references()`, schema + database of `name`, cluster (if placed), `owner_id` | id (item + global ids) | (schema, item name) |
| `UpdateItem` | as `CreateItem` for `to_item` | id | (schema, item name) if renaming |
| `RenameItem` | id, schema of `current_full_name` | id | (schema, `to_name`) |
| `AlterAddColumn` | id | id, `new_global_id` | none |
| `AlterRetainHistory`, `AlterSourceTimestampInterval` | id | id | none |
| `AlterMaterializedViewApplyReplacement` | id, `replacement_id` | id, `replacement_id` | none |
| `UpdateSourceReferences` | `source_id` | `source_id` | none |
| `DropObjects` | dropped ids | dropped ids | none |
| `CreateDatabase` | `owner_id` | none | (databases, name) |
| `CreateSchema` | `database_id`, `owner_id` | none | (database, schema name) |
| `CreateRole` | none | none | (roles, name) |
| `CreateCluster` | `owner_id` | `id` | (clusters, name) |
| `CreateClusterReplica` | `cluster_id`, `owner_id` | `replica_id` | (cluster, replica name) |
| `CreateNetworkPolicy` | `owner_id` | none | (network policies, name) |
| `RenameCluster` | id | id | (clusters, `to_name`) |
| `RenameClusterReplica` | `cluster_id` | `replica_id` | (cluster, `to_name`) |
| `RenameSchema` | schema | schema | (database, `new_name`) |
| `Comment` | `object_id` | comment key on `object_id` | none |
| `UpdateOwner` | id, `new_owner` | id | none |
| `GrantRole`, `RevokeRole` | `role_id`, `member_id`, `grantor_id` | membership (`role_id`, `member_id`) | none |
| `UpdatePrivilege` | `target_id` | `target_id` (a dedicated key for `SystemObjectId::System`) | none |
| `UpdateDefaultPrivilege` | none | `privilege_object` key | none |
| `AlterRole`, `AlterNetworkPolicy` | id | id | (namespace, `name`) |
| `UpdateClusterConfig`, `UpdateClusterReplicaConfig` | id(s) | id(s) | none |
| `CheckClusterState` | `cluster_id` | none | none |
| `UpdateSystemConfiguration`, `ResetSystemConfiguration`, `ResetAllSystemConfiguration`, `UpdateScopedSystemParameters`, `InjectAuditEvents` | global | global | global |

Notes on the table:

- Freshly allocated ids (a `CreateItem`'s item id, a `CreateClusterReplica`'s
  replica id) can never appear in a concurrent commit's summary, since ids are
  never reused. They are listed in `writes` anyway for uniformity, the
  asymmetry of `CreateDatabase`/`CreateSchema`/`CreateRole` (whose ids are
  allocated inside op application, so the op does not carry them) is why those
  rows say "none". Neither choice affects correctness.
- `RenameItem` and `RenameSchema` under-declare their write sets on purpose.
  Applying a rename rewrites the `create_sql` of every dependent item (and for
  schema renames, of every contained item), a set that is recomputed from the
  *current* catalog at replay time. Declaring only the renamed object is sound
  because of an invariant this design depends on: **every item's `create_sql`
  embeds its own schema-qualified name, so a committed rename emits an `Item`
  update for each rewritten item**, and any concurrently *created* dependent's
  summary touches the renamed id through its references. Both directions of
  the race therefore intersect. This invariant must be stated in the code next
  to the footprint rules.
- The last row's variants cannot appear in a DDL transaction today (their
  statements are not in the blessed list), so the global rule costs nothing
  and keeps the table total.

Reachable ops in DDL transactions today, for test-scoping purposes:
`CreateItem` and `UpdateSourceReferences` (via `CREATE SOURCE` and
`CREATE TABLE ... FROM SOURCE`), `RenameItem` (`ALTER ... RENAME` on items),
`RenameSchema` and `RenameCluster` (`ALTER SCHEMA/CLUSTER ... RENAME`), and
the swap forms of the latter two, which produce **three** rename ops through a
planning-time temporary name (`sequence_alter_schema_swap`,
`sequence_alter_cluster_swap`). Item-level swap does not exist
(`plan_alter_object_swap` supports only schemas and clusters). The full table
exists so the footprint stays correct as the blessed statement list grows, and
so the same extractor serves the future multi-writer commit path, where every
op variant is reachable.

### The update log

The coordinator keeps a `VecDeque<(u64, Arc<UpdateSummary>)>` of committed
catalog transactions: the revision each commit produced and a pre-classified
summary of its updates.

**Where summaries are produced.** They must be classified from the raw, total
update stream, not from `ParsedStateUpdate`: the implications parser
(`parse_state_update`) intentionally drops `Schema`, `Database`, `Role`,
`Comment`, `SourceReferences`, privilege, and other kinds, which are exactly
the kinds the conflict test needs (a concurrent `ALTER SCHEMA ... RENAME`
must not become invisible). Classification therefore happens inside
`Catalog::transact` (or the in-memory apply it drives), where both the raw
`StateUpdateKind`s and the *parsed items* are in hand, and the summary is
returned to the coordinator as a new field on `TransactionResult`. Building
the summary during apply matters for another reason: an `Item` update's
referenced ids come from the parsed item (`uses()`/`references()`), which the
apply step has already computed. The classifier never re-parses SQL.

An `UpdateSummary` classifies each state update into the same key space as
the footprint:

- **Touched ids**: the updated object's own id, plus, for item additions and
  updates, the item's referenced ids. Including references is what catches the
  reverse-dependency hazard: a concurrent `CREATE VIEW v ON t` must conflict
  with an open transaction that drops `t` (a future multi-writer case,
  `DropObjects` is not reachable in DDL transactions today), and it does
  because `v`'s update entry touches `t`'s id, which is in the drop's write
  set.
- **Claimed names**: names introduced or taken over by the update (additions
  and renames), in the same id-scoped (scope, name) form.

Classification per `StateUpdateKind`, total, with rationale for exemptions:

| StateUpdateKind | Classification |
|---|---|
| `Item` | own id + referenced ids, claimed name on addition |
| `TemporaryItem` | own id + referenced ids, name scoped to the owning connection's temp schema. Disjoint from other sessions' footprints by construction, so temp-table churn stops aborting DDL transactions |
| `Schema`, `Database`, `Role`, `RoleAuth`, `Cluster`, `ClusterReplica`, `NetworkPolicy`, `IntrospectionSourceIndex` | own id, claimed name on addition |
| `Comment` | comment key (object id) |
| `SourceReferences` | source id |
| `DefaultPrivilege`, `SystemPrivilege` | privilege object key |
| `SystemConfiguration`, `ClusterSystemConfiguration`, `ReplicaSystemConfiguration` | config category. Never intersects a footprint: DDL transaction ops do not read config (see Limitations), so parameter syncs never conflict |
| `AuditLog` | **exempt** (touches nothing). Sound because no op application reads prior audit entries, and audit ids are allocated fresh at replay. Without this exemption every DDL commit would conflict with everything, since nearly every commit writes audit events |
| `StorageCollectionMetadata`, `UnfinalizedShard` | **exempt** (touches nothing). Pure storage-side bookkeeping keyed by global ids whose object-level conflicts are already covered by the corresponding `Item` updates in the same commit |
| `SystemObjectMapping` | global (builtin migrations, vanishingly rare at runtime) |

Any future variant added without a classification rule must fail closed: the
enum match is written without a wildcard so a new kind is a compile error.

**Retention.** Ownership is derived from connection state, not a counter (a
counter leaks: `handle_terminate` never calls the coordinator's
`clear_transaction`). The connection's `ConnMeta` in `active_conns` records
the base revision of its open DDL transaction, set when `handle_execute_inner`
initializes `TransactionOps::DDL` and cleared in `clear_connection`, which
runs on both regular transaction end and connection termination. The log
retains entries newer than the minimum base revision across `active_conns`,
which is self-healing on disconnect. Trimming is **lazy**: it happens when
appending new entries, never eagerly when a transaction closes. This matters
at `COMMIT`, where `clear_transaction` runs before the conflict test: eager
trim-on-close would evict the very entries the closing transaction is about
to test against.

A hard cap bounds memory (initial proposal: 10k summary entries, tuning to be
settled during implementation). The trim unit is whole commits: a summary is
evicted only in its entirety. If a single commit's summary alone exceeds the
cap (a schema drop with tens of thousands of items), the log is cleared
entirely and every open transaction older than it falls back to conflict,
conservative and correct. Memory per entry is small (ids and interned names
behind an `Arc`), tens of kilobytes for a thousand-object commit.

### Conflict predicate

Given footprint F and the update summaries U in `(txn_revision, current]`:

conflict iff any of

- some `u` in U is global, or F is global,
- `touched_ids(U)` intersects `F.reads ∪ F.writes`,
- `claimed_names(U)` intersects `F.names`.

Two intentional asymmetries:

- Updates are all writes by definition, so there is no updates-side read set.
- Footprint names are only checked against *claimed* names, not against every
  name mentioned by an update. A rename away from a name the transaction
  wants to claim frees the name, and the dry run performed on refresh
  re-validates availability anyway.

### Guard-site changes

A shared helper on the coordinator does the work at every site:

```text
fn revalidate_ddl_transaction(txn) -> Result<Refreshed | Unchanged, AdapterError>
```

1. **Statement arrival** (`handle_execute_inner`, blessed-statement arm): this
   is the site that fires first for the second and later statements, so it
   must revalidate, not just compare. It runs on the coordinator, so the
   helper is in scope. On refresh, the session transaction's stored
   `revision`, `state`, and `snapshot` are replaced by **direct mutation** of
   the stored `Transaction` (via `TransactionStatus::inner_mut`), not by
   `add_ops`: the `add_ops` equality would reject the very write-back that
   refreshes it.
2. **Op accumulation** (`catalog_transact_with_ddl_transaction`): on revision
   mismatch, run the conflict test. On pass, run a *full* (non-incremental)
   dry run of the accumulated ops against the current catalog
   (`transact_incremental_dry_run` with `prev_snapshot: None` and the current
   catalog state, which is verified to be side-effect-free and idempotent:
   ids were durably allocated at planning, and dry runs discard audit events
   and builtin updates), replacing `state`, `snapshot`, and `revision` by
   direct mutation, then continue with the incremental dry run of the new ops
   as today.
3. **`add_ops`** (`session.rs`): unchanged as an internal consistency check.
   Sites 1 and 2 refresh before any `add_ops` call that would observe a newer
   revision.
4. **`COMMIT`** (`sequence_end_transaction_inner`): on revision mismatch, run
   the conflict test. On pass, proceed directly to the `catalog_transact`
   replay, carrying `expected_revision` (see the backstop above). No dry run
   is needed first: the replay is the same validation code, and it either
   commits or errors atomically.

### Error mapping

Errors surfaced by the refresh dry run or by the `COMMIT` replay that stem
from a concurrent change (anything the conflict test passed but application
rejects, for example a duplicate name from an exempted category) are mapped to
`DDLTransactionRace` with the underlying error as detail. Rationale: clients
and tooling retry on SQLSTATE 40001 (dbt's `try_atomic_swap` keys on exactly
this), and a mid-transaction `42710 duplicate_object` on a statement the user
considers unrelated breaks that contract. A refresh failure *is* a conflict
the footprint failed to predict, and classifying it as such is honest.

`DDLTransactionRace` gains a payload naming the first conflicting update kind
and id, surfaced as error detail. The Display message prefix ("another session
modified the catalog") stays stable: `test/sqllogictest/transactions.slt`,
`test/race-condition/mzcompose.py`, and
`misc/python/materialize/parallel_workload/action.py` match on it, and dbt
keys on the SQLSTATE, which is per-variant and survives the payload.

### RBAC semantics

Privilege checks run at statement sequencing, not at commit, and this design
does not add a commit-time re-check. A privilege revoked between a statement
and `COMMIT` therefore no longer aborts the transaction (today it does, as a
side effect of the revision bump). This matches Postgres, where a revoke after
your `CREATE` does not fail your `COMMIT`. Revokes that manifest as catalog
changes on objects in the footprint (for example `UpdateOwner` or
`UpdatePrivilege` on a referenced object, or `DROP ROLE` of an op's
`owner_id`) still conflict via the id intersection.

## Limitations

- **Planning-time environment reads are not revalidated.** Ops were planned
  under feature flags, session variables, and default privileges as of their
  statement's execution. Concurrent changes to those do not abort the
  transaction. This is the price of exempting system configuration from
  conflicts and is consistent with statement-execution-time semantics in
  Postgres.
- **Default privileges are derived at application time**, not baked into the
  op: `Op::CreateItem` application computes them from the catalog state it is
  applied against. A concurrent `ALTER DEFAULT PRIVILEGES` therefore takes
  effect on the transaction's items at `COMMIT`, and does not conflict (its
  update touches only the default-privilege key). The committed result can
  differ from what the mid-transaction dry-run state showed. Accepted: it is
  at least as current as statement-time semantics.
- **The footprint is id- and name-granular, not semantic.** Two changes to the
  same object always conflict even if they are commutative (for example two
  comments on different sub-components). Conservative and cheap beats clever
  here.

## Alternatives

- **Trust the commit replay alone.** Unsound, see "Why the naive alternative
  is unsound".
- **Pessimistic locking**: have DDL transactions hold the `serialized_ddl`
  lock (or a finer object-lock set) for their whole lifetime. Blocks all other
  DDL and, worse, background system transactions for the duration of
  purification network calls. Rejected.
- **Diff the catalog state instead of logging updates**: keep the base
  `CatalogState` clone in the transaction (it is already cloned at
  initialization) and structurally diff current against base at conflict
  time. `CatalogState`'s `imbl` maps make the diff efficient, and it needs no
  coordinator-side log or retention policy. Rejected in favor of the update
  log because the summaries fall out of the apply step that already parses
  items, the log gives the exact interleaved history rather than endpoint
  states, and the multi-writer future consumes an update stream (a foreign
  diff), not a state pair. The state-diff approach remains a valid fallback
  if the log's retention bookkeeping proves annoying.
- **Track and re-check a read set** for full serializability of catalog
  reads. There is no read set to track today (DDL transactions are
  catalog-write-only), so this is machinery without a customer. Revisit if
  reads inside DDL transactions ever become a thing.

## Observability

- Counter for revalidations, labeled by outcome (`refreshed`, `conflicted`)
  and site (`statement`, `accumulate`, `commit`).
- The conflict detail names the first intersecting update kind and id, see
  Error mapping.

## Documentation

- `doc/developer/guide-adapter.md` gains: (a) the load-bearing invariant that
  all catalog content commits execute on the coordinator loop, with a pointer
  to the `expected_revision` backstop, filed under the existing
  compare-and-append section it instantiates, and (b) a short description of
  the update log and the every-item-rewrite invariant that renames rely on.

## Testing

- **Unit**: footprint extraction per op variant (including the global
  fallback and name normalization failure), update-summary classification per
  `StateUpdateKind` (compile-enforced totality), and the conflict predicate,
  as table-driven tests next to the extractor.
- **Integration** (sqllogictest `simple conn=...` gives deterministic
  same-file interleaving and is what the existing coverage uses; testdrive
  `postgres-execute connection=...` where a real second connection is
  needed): with a DDL transaction open on connection A,
  - unrelated `CREATE TABLE` on connection B, then A's next statement and
    commit succeed (this *replaces*
    `test/sqllogictest/transactions.slt`'s existing assertion of the spurious
    abort, which becomes the positive regression test for this feature),
  - `CREATE TEMPORARY TABLE` and `ALTER SYSTEM SET` on other connections,
    then A commits successfully,
  - B renames an item A's transaction references, A fails with 40001,
  - B creates an item with a name A's transaction claims, A fails with 40001,
  - B drops the schema an op's item lives in, A fails with 40001,
  - conflicts detected at the statement-arrival site as well as at commit,
  - schema and cluster rename and swap (the three-op temp-name form) both as
    the open transaction and as the concurrent committer.
- **Existing suites**: `test/race-condition/mzcompose.py` and
  `misc/python/materialize/parallel_workload/action.py` currently treat the
  race message as an ignorable transient. Revisit both: spurious occurrences
  should drop sharply, and remaining ones are real conflicts.
- **Race coverage**: the platform-checks suite exercises the rename/swap and
  `CREATE SOURCE` flows that are the real users of this machinery.

## Open Questions

- Log caps: is 10k summary entries generous enough for purification-length
  transactions under DDL-heavy load? The failure mode of a too-small cap is
  today's behavior, so tuning can be empirical.
- Should the refresh at the op-accumulation site also re-run
  `validate_resource_limits` against the current catalog? It runs today with
  the combined ops, so likely yes for free, to be confirmed during
  implementation.
