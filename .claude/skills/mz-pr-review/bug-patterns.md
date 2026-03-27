# Typed error information discarded by generic responses
- When a code path produces a domain-specific typed error but the handler returns a hardcoded generic response ignoring the error value, diagnostic information is lost.
- **Red flag:** `Err(err)` handler that **logs** the typed error (`warn!(?err, "...")`) but **returns** a hardcoded literal response ignoring `err`. Multiple error variants all producing the same user-facing message.
- **Fix:** Map each error variant to an appropriate user-facing message via `into_response()` or equivalent on the error type.

# Soft assertions
- `soft_assert_or_log!` always evaluates its condition in all build modes. The condition must be the **invariant** (what should be true), NOT the error condition.
- **Red flag (inverted):** `soft_assert_or_log!(!created && dropped, "dropped non-existent")` — the condition describes the *forbidden* state, not the desired invariant. Should be negated.
- **Red flag (shutdown):** New features holding external references (read holds, subscriptions, cached refs) can cause shutdown assertions (`collections.is_empty()`) to fire spuriously. Prefer `Drop`-based cleanup.
- When a variable is only computed inside `if soft_assertions_enabled()`, use `Option<T>` downstream — a bare default conflates "not computed" with "computed as default."

# Mode and state interactions
- When a PR adds/modifies a boolean flag or operational mode (read-only, degraded, suspended), check all code paths whose correctness depends on assumptions the new mode violates (frontiers always advance, collections eventually hydrate, external state can be modified).
- **Red flag:** Derived value from two signals where one can be permanently stuck in some mode, causing cascading failures.

# Builtin views and system catalog
- When modifying a builtin view's SQL, identify all consumers (other views, internal queries, documentation).
- **Red flag:** `quote_ident()` on output columns consumed programmatically — it corrupts data for equality checks/JOINs. Only use for dynamic SQL interpolation.

# Catalog mutations and audit log consistency
- Every `tx.remove_*()` or `tx.insert_*()` must emit a corresponding audit log event.
- **Red flag:** Removal/insertion in migration or startup paths without audit log events — these paths are separate from the main `Op::DropObjects` handlers where audit logging is established.

# Version-specific vs. stable identifiers (GlobalId vs CatalogItemId)
- `GlobalId` is version-specific (changes on ALTER/REPLACE); `CatalogItemId` is stable.
- **Red flag:** `BTreeMap<GlobalId, ...>` keyed by `latest_global_id()` for grouping — items referencing older versions silently drop from the group.
- **Red flag (registration side):** Drop handler registering only `global_id_writes()` instead of iterating `global_ids()` — lookups by older GIDs (from active subscribes) fail.
- **Fix:** Resolve `GlobalId` to `CatalogItemId` before using as grouping key.

# CLI argument parsing (clap)
- **Red flag:** `serde_json::Value` as a clap field without `value_parser` — wraps input as `Value::String` instead of parsing JSON. Use `#[clap(value_parser = parse_json)]`.

# `Option<Config>` with `.expect()` at activation site
- When optional CLI args produce `Option<Config>` and the feature is activated by a separate mechanism (listener config, feature flag), missing config must produce an error — not `.expect()`.
- **Red flag:** `config.expect("should exist when mode is X")` where the mode and config are independently configurable.

# Parsed-but-unused values (underscore-prefixed)
- **Red flag:** `let (_feature_flag, rest) = extract(input)` with a TODO explaining how the value should be used. The parsed value controls behavior but is never branched on — the feature always takes the default path.
- Especially dangerous when the value controls security-critical decisions (auth method selection, encryption toggle).

# Tree traversal and visitor patterns
- **Red flag:** `self.visit_*()` or `self.try_visit_*()` inside a closure passed to another `self.visit_*()` — operates on root instead of callback parameter, causing correctness or O(n^2) bugs.

# Counterpart/sibling code consistency
- When a fix touches code with a documented counterpart (HIR ↔ MIR, encode ↔ decode), verify the counterpart doesn't have the same bug.
- When code is duplicated to a new location, diff against the original — verify every difference is intentional. Watch for **hardcoded constants** that were intentionally different from surrounding variables.
- **Red flag (counterfactual):** Code that calls the same function twice with one parameter intentionally different (e.g., Serializable vs StrictSerializable) — when duplicated, the differing argument gets mechanically replaced, making both calls identical.

# Standalone dispatch tables duplicating enum knowledge
- **Red flag:** A standalone `fn foo_monotonic(func: &Func) -> bool { match func { A => true, _ => false } }` when `Func` already has `is_monotone()`. Wildcard defaults make divergence invisible.

# New canonical impl alongside existing methods
- When a PR adds `foo_canonical()` alongside existing `foo()`, verify `foo()` delegates to the new one — or both produce identical results.

# Refactoring removing optimization without checking sibling paths
- When a refactoring removes a precomputed structure (skip lists, cached lookups), verify sibling code paths don't need it. Check for TODO comments describing the same optimization need.

# Accidental quadratic complexity
- **Accumulate-and-replay:** Function accumulates ops and each addition replays all ops through expensive pipeline → O(N^2). Fix: process only new ops incrementally.
- **Method substitution:** Refactoring replaces O(1) method with O(N) one (internally `.collect()`s) inside a loop.
- **Full-clone-and-compare:** `let prior = expr.clone(); transform(expr); if prior == *expr` — O(tree_size) for change detection. Fix: use a mutation-tracking flag.

# Copy-on-write state structs
- When a struct is wrapped in `Cow` and cloned per-operation, all fields must have O(1) clone cost. **Red flag:** `BTreeMap` fields that grow with user objects — use `imbl::OrdMap` or `Arc` wrapping.

# Retry loops with stale derived state
- Objects derived from base state (transactions, prepared requests) must be rebuilt inside the retry closure, not captured from outside.
- **Red flag:** `RetryResult::Ok(...)` returned from an error-recovery path — confusing "recoverable" with "recovered" causes silent data loss. Should return `RetryableErr`.

# Protocol and stream message handling
- **Red flag:** `.into_text()` on `tungstenite::Message` without checking variant — panics on Ping/Pong.
- After sending a state-changing response (CopyInResponse, AuthenticationSASL), error paths must drain remaining client messages before returning error.
- **Peek-based protocol detection:** `peek()` + parser must distinguish "invalid" (no header) from "incomplete" (partial header in flight). Treating both the same corrupts the stream.

# Resource cleanup and Drop completeness
- Methods taking `&mut Vec<T>` that normally consume contents must `.clear()` on early returns.
- `Drop` impls must reset **all** external effects (metrics, logging, counters).
- **Red flag:** Calling `async fn` in `Drop` without `.await` — complete no-op. Async fns are entirely lazy in Rust.

# Drop-guarded handles cloned to multiple consumers
- **Red flag:** Struct with `DeleteOnDrop*` fields `.clone()`d to multiple operators with different lifetimes — shorter-lived consumer's Drop prematurely deregisters metrics.
- **Fix:** Wrap in `Arc` and distribute `Arc::clone()`.

# Arithmetic on sentinel MAX values
- **Red flag:** `.unwrap_or(usize::MAX) + offset` — overflows to small value in release mode. Fix: `.map(|l| l.saturating_add(offset)).unwrap_or(usize::MAX)`.

# Sentinel "no progress" defaults as data range bounds
- When a resume point defaults to `T::minimum()` for fresh instances, the range lower bound should be `max(resume_point, as_of)` — not the raw sentinel.

# Unsigned integer underflow in loops
- **Red flag:** `counter -= 1; if counter == 0 { break; }` at loop bottom with no check before decrement — underflows when counter starts at 0.

# Integer overflow before type widening
- **Red flag:** `2_usize.pow(n)` where n comes from input (schema field size) — overflows before the `as f64` conversion. Use math identities: `log10(2^n) = n * log10(2)`.

# Resource-scaling formulas
- **Red flag:** `memory_limit * scale * workers` where workers are threads sharing process memory — `workers` shouldn't be in total memory calculation. Correct: `memory_limit * scale`.

# Parallelism changes and external resource scaling
- Removing `responsible_for()` so every worker processes every item multiplies external resources (connections) by worker count. Verify against external system limits.

# Stream-positional config cloned to parallel workers
- **Red flag:** `params.clone()` in worker-spawn loop where params contain `header: true` — every worker skips its first row, not just the one with the actual header.

# Format-naive byte splitting
- **Red flag:** `data.rposition(|&b| b == b'\n')` to split CSV data — CSV newlines inside quoted fields break records. Splitting must be format-aware.

# Inconsistent per-key state updates across match arms
- **Red flag:** In a processing loop, one match arm calls `.replace()` on per-key state and another is a no-op — subsequent iterations for the same key read stale state.

# Arrangement cursor loops without pre-consolidation
- When `map_times` callback performs expensive work (JoinClosure, datum construction), collect `(time, diff)` pairs and consolidate first — only do expensive work for non-empty consolidated results.

# Missing inter-operator synchronization for external resource creation
- When an upstream operator **creates an external resource** (Iceberg table, Kafka topic, S3 bucket) at startup and downstream operators **load that resource** via independent connections, timely dataflow provides no implicit initialization ordering — operators start concurrently.
- **Red flag:** Upstream calls `load_or_create_resource()` and downstream calls `load_resource()` with no frontier-based signal between them. Downstream may attempt to load before upstream has created. Especially likely when upstream runs on a single worker and downstream on all workers.
- **Fix:** Add a readiness signal stream (`Stream<G, Infallible>`) — upstream holds capability during init, drops after creation. Downstream drains this stream before accessing the resource. Ensure inactive workers also drop the new capability (see next item).

# New outputs in single-worker dataflow operators
- When adding a new output (new capability set), the `if !is_active_worker` block must drop it too. **Red flag:** `caps` array grows but inactive-worker cleanup only drops the old capabilities.

# Unconditional use of optional external dependencies
- **Red flag:** Unconditional `.watches()` for types from optional operators (cert-manager, Prometheus) — fails with 404 where operator isn't installed.

# Missing `skip_serializing_if` on `Option<T>` in external-facing serde structs
- **Red flag:** New `Option<T>` field on a Kubernetes/API struct without `#[serde(skip_serializing_if = "Option::is_none")]` — serializes `None` as `null` instead of omitting.

# Incomplete side-effects in new match arms
- When adding a new arm to a match block, verify it performs **all** side-effects that sibling arms perform. **Red flag:** `_full_name` (underscore-prefixed, unused) where siblings use `full_name`.

# Wildcard match arms swallowing new variants after type migration
- After migrating an entity to a new type category, `_ => {}` wildcards silently absorb the new variant. **Red flag:** `_ => {}` no-op catching a variant that needs specific behavior (compaction window, RETAIN HISTORY).

# Cumulative state fields checked but never updated
- **Red flag (Rust):** `if let Some(x) = self.option_field` **copies** the inner value — mutations to `x` don't write back. Need `if let Some(x) = &mut self.option_field` with `*x = new_value`.

# All-items iteration ignoring targeting/partitioning
- **Red flag:** `for replica in self.replicas.values() { replica.collections.get(&id)? }` when the entity has `target_replica` — only the target has the state.

# Global/aggregate state for per-instance decisions
- **Red flag:** `unreadable_collections` computed from global frontiers, used in per-replica loops — global frontiers advance when *any* replica advances, producing wrong results for unhydrated replicas.
- **Scheduling variant:** Frontier check `deps.all(|d| d.write_frontier() > as_of)` using global frontier to gate per-replica scheduling — can deadlock if dependency isn't scheduled on this replica yet.

# Filtering `Option` values gating multiple behaviors
- **Red flag:** `.filter(|v| v.is_identity())` converting to `None` when the `Option` is used by multiple downstream `if let Some(x)` — disables all behaviors, not just the one the optimization targets. Apply filter narrowly at each use site.

# `Option<bool>` unwrap in authorization guards
- **Red flag:** `.unwrap_or(false)` in privilege guard — collapses `Some(false)` ("explicitly revoke") into `None` ("no change"), bypassing authorization. Use `.is_some()` when any change to a sensitive attribute requires elevated privileges.

# Optimization shortcuts ignoring variant parameters
- **Red flag:** Function takes `kind: JoinKind` but early-return shortcut doesn't reference `kind` — wrong for outer joins which must preserve rows. Check each variant's semantics.

# Type cast closures ignoring target type parameters
- **Red flag:** `CastTemplate::new(|_ecx, _ccx, _from_type, _to_type| { ... })` where target is a parameterized type (Array, Numeric, etc.) — must inspect `to_type` for element type, scale, etc.

# Runtime `unreachable!()` without plan-time validation
- **Red flag:** `unreachable!()` in execution code for type variants users can reach through valid SQL, without planner rejecting the combination first. Empty tables make this worse — panic only fires when first row arrives.

# Lossy type narrowing in stored fields
- **Red flag:** Struct field changed from `RichType` to `NarrowType` where conversion is many-to-one, and downstream reconstructs via `RichType::from(field)` — round-trip loses information. Keep rich type in storage, narrow only at comparison sites.

# Cast elimination invalidating downstream `.unwrap()`
- When eliminating intermediate casts (e.g., varchar_to_text), audit downstream `.unwrap()` on `union()`, `base_eq()` — the casts were ensuring type consistency. Without them, previously-impossible type mismatches reach panicking `.unwrap()` calls.

# Type-compatibility checks replaced with exact equality
- **Red flag:** Migration changes `a.base_eq(b)` to `NewType::from(a) == NewType::from(b)` where `NewType`'s `Eq` is stricter (includes deep nullability). Use the equivalent compatibility operation in the new type system, not `==`.

# Wrapper enum variants silently swallowed by wildcards
- **Red flag:** `Plan::Subscribe(plan) => extract_properties(plan)` has specific handling but `Plan::ExplainPlan(ExplaineeStatement::Subscribe { .. })` falls through to wildcard. Must add wrapper arm.

# Incomplete layers in new route/service setup
- **Red flag:** New router adds `.layer(Extension(A))` but not `.layer(Extension(B))` where siblings add both — causes runtime panic (not compile error) when handler extracts `Extension<B>`.

# Loop sentinel flag updated inside its own conditional
- **Red flag:** `if !first { write!(f, ", ")?; first = false; }` — the `first = false` only runs when `first` is already false. Must be outside the `if`.

# Operator precedence: `!a == b`
- **Red flag:** `!expr.len() == other.len()` — parses as `(!expr.len()) == other.len()`, which is bitwise NOT on integer. Use `!=`.

# Redundant plan-level constraint checks
- Planning-time checks that predict runtime constraint violations must be **no more restrictive** than runtime checks. **Red flag:** Rejecting columns absent from plan projection — they may be filled by defaults at runtime.

# Nested `Result` from async combinators
- **Red flag:** `if let Err(err) = timeout(dur, fallible_future).await` — inner error `Ok(Err(e))` is silently ignored. Use three-arm match: `Ok(Ok(v))`, `Ok(Err(e))`, `Err(elapsed)`.

# Manual string formatting at serialization boundaries
- **Red flag:** `format!("{}us", val)` to produce a string consumed by `humantime::parse_duration` — use the library's own formatter for guaranteed round-tripping.

# Raw SQL string interpolation
- **Red flag:** `format!("... '{}' ...", value)` — SQL injection. Use `escaped_string_literal()` for string values, `Ident::new_unchecked(name).to_ast_string_simple()` for PG identifiers, `mz_sql_server_util::quote_identifier()` for SQL Server.
- **Red flag:** Wrapping quoting-function output in additional manual quotes (`format!(r#"COMMAND "{}""#, ident.to_ast_string_simple())`) — double-quoting.
- On replication connections, replace `SHOW var` with `SELECT current_setting('var')`.

# Feature flags and `enable_for_item_parsing`
- Parser/syntax features: `enable_for_item_parsing: true`. Optimizer features: `enable_for_item_parsing: false` (otherwise expression cache becomes ineffective).
- **Red flag:** Flag whose desc mentions "lowering", "optimizer", "join planning", "MIR transform" with `enable_for_item_parsing: true`.

# Cache key mismatch between population and comparison
- **Red flag:** Config fingerprint constructed once above a per-entity loop for cache comparison, but entities belong to different scopes (clusters) with config overrides. Must use per-entity config.

# Copy-pasted functions with stale references
- **Red flag:** Function named `plan_alter_X_owner` referencing `ObjectType::Y` where Y ≠ X. Also check multi-argument function calls where most args reference new type but one still references old.
- **Red flag:** Sibling boolean wrappers (`count_successes`/`count_errors`) both passing `true` to shared impl — second wrapper forgot to invert.

# Stale `self` after `mem::take(self)`
- **Red flag:** After `let this = mem::take(self)`, any `self.field` reference reads empty default state. All subsequent code must use `this`.

# Error paths in stateful processors not resetting state
- **Red flag:** Success branch resets `self.cursor = 0` but error branch returns `Err(...)` without resetting — stale state corrupts next call.

# Partial writes to shared buffers not rolled back
- **Red flag:** `Encoder::encode` writes type byte + length, then `?` operators can exit leaving partial message bytes. Fix: record `start = dst.len()`, truncate on error.

# `return` statements that should be `continue` after moving code into a loop
- **Red flag:** `return;` inside a `for` loop body — exits the enclosing function, skipping all remaining iterations. Should be `continue` or restructured with `if`.

# Conditional write retries treating conflict as failure
- **Red flag:** Flat error type from conditional write (`If-None-Match: *`) — cannot distinguish "write failed" from "already exists" (original succeeded). Need structured error enum.

# Hardcoded permissive defaults in context objects
- **Red flag:** Helper takes `&QueryContext` (narrow) and constructs `ExprContext` (wide) with `allow_subqueries: true` — callers with restrictions get them silently bypassed.

# Silent type coercion in catch-all decoder arms
- **Red flag:** Decoder `match` with catch-all extracting unsupported types as `Datum::String` — should return an error at ingestion boundary.
- Also check decoder validation: e.g., "both before/after null" may be valid for delete operations — validation must reference the operation discriminator field.

# Ambient auth shadowing explicit credentials
- **Red flag:** Session/cookie checked first, explicit per-request credentials (headers, API key) never reached. Explicit credentials should always take priority.

# Disabled security checks
- Comment at disabled check must document the **compensating control** and where it's implemented — not just why the check is disabled.

# Premature read-state mutation before durability
- **Red flag:** Write mutates shared read-serving state immediately, records in pending WAL batch for later flush — reads between mutation and flush see uncommitted data.

# External-state signals not initialized on recovery
- **Red flag:** Gauge metrics only set in `flush()` but not in constructor/recovery path — gauges show zeros until first flush despite populated internal state.

# Bypassing project utility wrappers
- **Red flag:** Direct `aws_config::defaults()` + `Client::from_conf()` when `mz_aws_util::defaults()` / `mz_aws_util::s3::new_client()` exist. Raw SDK bypasses required config (addressing mode, TLS, creds).

# Bounded drain with rate-limiting sleep on unbounded channels
- **Red flag:** `recv_many(&mut buf, BATCH_SIZE)` + `sleep` on an unbounded channel — throughput ceiling causes unbounded queue growth. Fix: drain all available messages (`recv().await` then `try_recv()` loop).

# Worker-local cleanup of controller-managed resources
- **Red flag:** Worker removes shared-state entries or drops shutdown handles on local channel disconnect — stops dataflow scheduling for all workers. Cleanup must be coordinated by the controller.

# Panicking catalog lookups on plan-phase IDs
- **Red flag:** `self.catalog().get_entry(&id)` where `id` from plan struct — concurrent DDL can remove the entry. Use `try_get_entry` and return graceful error.

# Type-specific fix for shared root cause
- **Red flag:** Fix applies topological sorting only to `CONNECTION` items when the root cause (ALTER can reorder dependencies) also affects tables, sources, MVs. Fix must cover all affected types.

# New data write paths missing schema enforcement
- **Red flag:** New ingestion path going `decode → Row → persist` without `Row::validate()` — NOT NULL violations on unspecified columns cause server panics.

# Partial field aggregation
- When a result struct gains new fields, verify all aggregation loops accumulate them. **Fix:** Implement `AddAssign` on the struct.

# New entity types missing from ownership enumeration
- Functions like `plan_drop_owned` iterate object types in separate `for` loops — adding a new `ObjectId` variant requires a new loop block (compiler gives no exhaustiveness warning).

# Framework constructor bypass
- When replacing high-level `Server::serve(addr)` with manual socket setup, replicate all implicit setup (nonblocking mode, SO_REUSEADDR, etc.).

# Match arm ordering for types with overlapping datum representations
- JSONB uses `Datum::String`, `Datum::True`, etc. Type-specific arm `(_, SqlScalarType::Jsonb)` must come **before** datum-specific arms `(Datum::String(s), _)`.

# Async lock held across `.await` (futurelock)
- **Red flag:** `tokio::Mutex` guard held while awaiting external progress — if future is suspended by `select!`, lock is held indefinitely. Split into: briefly lock → release → wait without lock → re-acquire.

# Refutable patterns in `tokio::select!` stalling streams
- **Red flag:** `Some(Err(e)) = stream.next() =>` — `Some(Ok(_))` disables the branch. Use irrefutable pattern: `Some(result) = stream.next() => if let Err(e) = result { ... }`.

# Null-handling: `unwrap_*()` in non-null-propagating functions
- When `propagates_nulls() = false`, null inputs reach the function body. **Red flag:** `.unwrap_array()` called without null check in such functions — panics on `Datum::Null`.

# Missing runtime data validation at ingestion boundaries
- External data decoded into `Row`s must be validated against `RelationDesc` before persisting — decoders produce structurally valid rows but don't enforce NOT NULL or type constraints.

# SemVer comparisons using derived `PartialOrd` instead of `cmp_precedence`
- **Red flag:** `version_a < version_b` — derived `PartialOrd` lexicographically compares build metadata, but SemVer spec says build metadata must be ignored for precedence. Use `version.cmp_precedence(&other)`.

# Crate splitting retaining heavy dependencies
- **Red flag:** New crate for build-time improvement with TODO acknowledging heavy dependency should be removed. Move lightweight types to a shared types crate.

# SQL function synonyms with different argument orderings
- **Red flag:** New SQL function added as "synonym" for existing one (e.g., `strpos` for `position`) that reuses the same `BinaryFunc` without swapping arguments. `position(substring, string)` vs `strpos(string, substring)` have reversed argument order — direct reuse silently swaps them. Verify both functions' signatures in PostgreSQL docs. Test with asymmetric arguments where swapping produces different results.

# `Ord`/comparison using wrong receiver
- **Red flag:** `self.collection.get(other.range.start)` — index from `other` but data from `self`. Latent when instances share backing data.

# Eager-to-lazy evaluation with short-circuiting consumers
- **Red flag:** Wrapping `eval()` calls in `.map()` iterator passed to `try_from_iter` which short-circuits on NULL — remaining expressions never evaluated. Fix: evaluate eagerly into array first.

# Infallible return types hiding `.unwrap()` on user data
- **Red flag:** `fn into_datum(self) -> Datum` with internal `.unwrap()` on user-data validation (range bounds, array limits) — server crash on invalid input. Return `Result`.

# Unbounded accumulation of external input
- **Red flag:** `loop { recv → buffer.extend(data) }` with no size limit before `process(entire_buffer)` — OOM on large inputs. Use streaming/chunked processing.

# Concrete-to-generic refactoring preserving type-specific assumptions
- When replacing concrete impls with generic `impl<T: Bound>`, verify hardcoded values (`fn fallible() -> bool { false }`) and infallible operations still hold for all `T`. Error paths that were dead code may now be reachable.
