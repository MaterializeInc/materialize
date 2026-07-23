# Two-runtime arrangement sharing lifecycle: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the interactive compute runtime's reads of maintenance-published index arrangements sound: fix the delayed-capability panic and the drop-vs-pending races without reintroducing row-doubling.

**Architecture:** Keep the frontier-tracked replay but single-source its feed from the arrangement stream frontier (F1). Let interactive imports late-bind through pre-allocated publication points (placeholder plus adopt-in-place, F2) so rendering happens in command arrival order with no per-worker deferral. Route to the interactive runtime by a bounded-read predicate. Rely on the controller's read-hold discipline for teardown, and panic on any protocol violation.

**Tech Stack:** Rust, timely-dataflow, differential-dataflow (mz fork), the `mz-compute` crate. Tests run under `bin/cargo-test -p mz-compute`.

**Design doc:** `doc/developer/design/20260720_two_runtime_compute/arrangement-sharing-lifecycle-design.md`.

**Validated spike branches (seed implementation from these):**
- `spike/single-source-publish`: Task 1 (F1 publisher feed + 4 tests). Worktree `.claude/worktrees/agent-aaa41960dc099f895`.
- `spike-f2-placeholder`: Tasks 2-3 (placeholder + adopt + `join_over_placeholder_adopted_late_matches_direct`). Worktree `.claude/worktrees/agent-afa51e8e48a16ad07`.

## Global Constraints

- Branch: `mh/two-runtime-stage2`. Base ref for diffs: `origin/mh/two-runtime-stage2`.
- Feature gated behind `enable_two_runtime_compute` (default on only in the two-runtime test suites).
- Before every commit: run `bin/fmt` and `cargo check -p mz-compute`. Both must be clean.
- Run tests with `bin/cargo-test -p mz-compute -- <filter>` (needs `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257` in the environment). First build is several minutes; use a bash timeout of at least 600000ms.
- No `as` numeric casts. Use `mz_ore::cast`.
- No `std::collections::HashMap`. Use `BTreeMap`.
- Placeholder frontiers are `Antichain::from_elem(<Timestamp as timely::progress::Timestamp>::minimum())`, never `Antichain::new()` (the empty antichain reads as sealed through the end of time).
- The differential join trait import path is `differential_dataflow::operators::join::JoinCore`, not `operators::JoinCore`.
- Code comments: no em-dashes, no structuring semicolons. Split into full sentences.
- Commit messages end with:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01CY7GHdrTBfAG4tgvSgPGJ9`

---

### Task 1: Single-source the publisher replay feed (F1)

Fixes the delayed-capability panic. The publisher sink currently derives `upper` from `agent.map_batches` (the trace), which leads the arrangement stream within a worker step, so a `Frontier(upper)` gets enqueued before a `Batch` whose hint is below it and `caps.delayed(&hint)` panics. Use the stream frontier (the sink's second closure argument, currently ignored as `_frontier`) as the authoritative `upper` instead.

**Files:**
- Modify: `src/compute/src/shared_trace.rs` (the `sink` closure inside `PublishArrangement::publish`, currently `move |(input, _frontier)|`).
- Test: `src/compute/src/shared_trace.rs` (the `tests` module at the bottom).

**Interfaces:**
- Produces: no signature change to `publish`. The behavior change is that `state.upper` and the `Frontier` instructions pushed to importer queues track the arrangement stream frontier rather than the trace's `map_batches` upper.

- [ ] **Step 1: Port the four tests from the spike branch and confirm they fail on current code**

Cherry-pick these tests from branch `spike/single-source-publish` into the `shared_trace.rs` `tests` module (they use the existing `publish_join_input` harness):
- `frontier_ahead_of_batch_trips_delayed_capability` (`#[should_panic]`): drives the publisher so the trace upper leads the stream and the importer hits `caps.delayed` past its frontier. Proves the hazard is real.
- `trace_upper_can_lead_stream_frontier`: asserts, via a `record-frontiers` sink, that `map_batches` upper is observed leading the stream frontier.
- `join_over_single_sourced_import_matches_direct`: a `join_core` over the import must equal `expected_join` exactly (no doubling), including an insert-then-retract that cancels. Requires `use differential_dataflow::operators::join::JoinCore;`.
- `empty_seal_advances_import_frontier_to_completion`: a seal with no data still advances the importer frontier so a bounded read reaches `until`.

Run: `git checkout spike/single-source-publish -- src/compute/src/shared_trace.rs` is too coarse (it also brings the fix). Instead open the spike file and copy only the four `#[mz_ore::test]` functions.

- [ ] **Step 2: Run the join and empty-seal tests to verify they fail (panic) on the unpatched publisher**

Run: `bin/cargo-test -p mz-compute -- shared_trace::tests::join_over_single_sourced_import_matches_direct`
Expected: FAIL or panic ("failed to create a delayed capability ...") on the current two-source feed. (`frontier_ahead_of_batch_trips_delayed_capability` is `should_panic`, so it passes on the unpatched code; that is intentional documentation of the hazard.)

- [ ] **Step 3: Apply the single-source publisher change**

In the `sink` closure of `PublishArrangement::publish`, change the signature from `move |(input, _frontier)|` to `move |(input, frontier)|` and replace the `map_batches`-derived upper:

```rust
// The stream frontier is the authoritative upper. It never leads the batches
// delivered on the stream, unlike the trace's `map_batches` upper, which can run
// ahead within a worker step and strand the importer's capability below a
// not-yet-emitted batch.
let upper = frontier.frontier().to_owned();

// Refresh the chain from the trace, but only accept batches the stream frontier
// has reached, so the published upper and the enqueued batches stay consistent.
let mut chain = Vec::new();
agent.map_batches(|batch| {
    if timely::PartialOrder::less_equal(&batch.upper().borrow(), &upper.borrow()) {
        chain.push(batch.clone());
    }
});
```

Keep the rest of the refresh loop, but use this `upper` where `state.upper` is set and where the `Frontier(upper)` instruction is pushed to importer queues. Do not read `upper` from `map_batches` anymore.

- [ ] **Step 4: Run all four tests and verify they pass**

Run: `bin/cargo-test -p mz-compute -- shared_trace::tests`
Expected: PASS, including `join_over_single_sourced_import_matches_direct`, `empty_seal_advances_import_frontier_to_completion`, `trace_upper_can_lead_stream_frontier`, and the `should_panic` `frontier_ahead_of_batch_trips_delayed_capability`, plus the pre-existing `shared_trace` tests.

- [ ] **Step 5: Format, check, commit**

```bash
bin/fmt
cargo check -p mz-compute
git add src/compute/src/shared_trace.rs
git commit -m "compute: single-source the shared-trace replay feed from the stream frontier

The publisher derived the published upper from the trace's map_batches, which
leads the arrangement stream within a worker step, so a Frontier instruction
could be enqueued before a Batch whose hint is below it and the importer's
caps.delayed panicked. Use the stream frontier, which never leads the delivered
batches, as the authoritative upper for both the published state and the importer
Frontier instructions.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01CY7GHdrTBfAG4tgvSgPGJ9"
```

---

### Task 2: Placeholder publication point and adopt-in-place (F2)

Lets an importer bind a live handle before the arrangement is published. The publisher later fills the same `Arc` in place. `SharedTrace` does not hold the `TraceAgent` (it lives in the sink closure), so adopt is just attaching the sink to an existing `Arc`.

**Files:**
- Modify: `src/compute/src/shared_trace.rs` (`Published`, `PublishArrangement`).
- Test: `src/compute/src/shared_trace.rs` tests, and `src/compute/src/sharing.rs` tests (the placeholder join test lives near the existing arrangement tests).

**Interfaces:**
- Produces:
  - `Published::<Tr>::placeholder(peers: usize) -> Published<Tr>`: an `Arc<SharedTrace>` with empty chain, `since` and `upper` at `from_elem(minimum)`, no publisher. `.handle()` mints `SharedTraceHandle`s immediately.
  - `PublishArrangement::adopt(&self, placeholder: &Published<Tr>)` and `adopt_named(&self, placeholder: &Published<Tr>, name: &str)`: install this arrangement's publisher sink onto `placeholder.shared` (asserting equal `peers`), reusing the existing sink body via `Arc::clone(&placeholder.shared)` for both `Publisher` and `sink_shared`.
  - `publish_named` refactored to `let published = Published { shared: Arc::new(SharedTrace::new_empty(peers)) }; self.adopt_named(&published, name); published` (publish is adopt with no prior reader).

- [ ] **Step 1: Port the placeholder join test from the spike branch and confirm it fails to compile**

From branch `spike-f2-placeholder`, copy the test `join_over_placeholder_adopted_late_matches_direct` (in `sharing.rs`) and the `adopt_join_input` helper it uses. The test: create `Published::placeholder(peers)` before any publisher, mint a handle, `import` it, build `join_core(arr_a, arr_b)` capturing the trace by value while empty, step 64x and assert `probe.less_than(1)` (no output, frontier pinned at `[0]`), then `arranged.adopt(&placeholder_a)`, feed data, seal, and assert the captured output equals `expected_join` exactly.

- [ ] **Step 2: Run it to verify it fails to compile**

Run: `bin/cargo-test -p mz-compute -- sharing::tests::join_over_placeholder_adopted_late_matches_direct`
Expected: FAIL to compile ("no method named `placeholder`" / "no method named `adopt`").

- [ ] **Step 3: Add `placeholder`, `adopt`, `adopt_named`, and refactor `publish`**

Port the `Published::placeholder`, `PublishArrangement::adopt`/`adopt_named`, and the `publish_named` refactor from `spike-f2-placeholder`'s `shared_trace.rs`. Key points:
- `placeholder` builds the same `SharedTrace` the publisher would (empty `chain`, `since = upper = Antichain::from_elem(minimum)`, `closed = false`), with no sink attached.
- `adopt_named` clones `Arc::clone(&placeholder.shared)` into both the `Publisher` guard and `sink_shared`, then attaches the sink exactly as `publish_named` did.
- Assert `scope.peers() == placeholder.peers` in adopt.

- [ ] **Step 4: Run the placeholder join test and the pre-existing tests**

Run: `bin/cargo-test -p mz-compute -- sharing::tests shared_trace::tests`
Expected: PASS, including `join_over_placeholder_adopted_late_matches_direct` and all 8 pre-existing `sharing`/`shared_trace` tests (the `publish` refactor must be behavior-preserving).

- [ ] **Step 5: Format, check, commit**

```bash
bin/fmt
cargo check -p mz-compute
git add src/compute/src/shared_trace.rs src/compute/src/sharing.rs
git commit -m "compute: pre-allocated publication points (placeholder plus adopt-in-place)

A SharedTrace placeholder can be created empty and handed to an importer as a
live handle before the arrangement is published. The maintenance publisher later
adopts the same Arc in place rather than constructing a fresh one. Publishing
becomes the degenerate case of adopting a publication point with no prior reader.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01CY7GHdrTBfAG4tgvSgPGJ9"
```

---

### Task 3: Registry get-or-create

Whichever runtime touches an id first creates its publication point; the other adopts or reads it. Replaces the create-fresh-and-overwrite `insert` so a placeholder a reader already imports is filled in place.

**Files:**
- Modify: `src/compute/src/sharing.rs` (`ArrangementSharingRegistry`).
- Test: `src/compute/src/sharing.rs` tests.

**Interfaces:**
- Produces:
  - `ArrangementSharingRegistry::get_or_create_placeholder(&self, id: GlobalId, worker_index: usize, peers: usize) -> SharedIndexArrangement`: returns the existing slot for `(id, worker_index)`, or creates a placeholder slot (placeholder `oks` and `errs` publication points) and returns it. Callable from either runtime.
  - The maintenance publish path calls `get_or_create_placeholder` and `adopt`s the returned placeholders instead of `insert`ing fresh arrangements.
  - `handles(id, worker_index)` unchanged in signature; returns the (possibly placeholder-backed) handles once a slot exists.

- [ ] **Step 1: Write the failing cross-thread test**

```rust
#[mz_ore::test]
fn get_or_create_converges_on_one_slot() {
    // A reader thread and a publisher thread both touch the same id. Whichever is
    // first creates the placeholder; the second must observe the same Arc, not a
    // second slot, and after adoption the reader sees the published rows.
    let id = GlobalId::User(1);
    let registry = ArrangementSharingRegistry::new();
    // reader creates the placeholder first
    let (oks, _errs) = registry
        .get_or_create_placeholder(id, 0, 1)
        .handles_for_worker(0);
    // publisher adopts the same slot and fills it (reuse publish_index_into,
    // but routed through get_or_create_placeholder + adopt)
    publish_index_into_adopting(&registry, id, test_rows());
    assert_eq!(read_rows(&oks, Timestamp::from(0_u64)), expected_rows(&test_rows()));
}
```

(Model `publish_index_into_adopting` on the existing `publish_index_into`, but replace the `insert` call with `get_or_create_placeholder(id, wi, peers)` followed by `adopt` of its publication points.)

- [ ] **Step 2: Run it to verify it fails**

Run: `bin/cargo-test -p mz-compute -- sharing::tests::get_or_create_converges_on_one_slot`
Expected: FAIL to compile ("no method named `get_or_create_placeholder`").

- [ ] **Step 3: Implement `get_or_create_placeholder` and route publish through it**

Add `get_or_create_placeholder` to the registry: under the `map` lock, if the `(id, worker_index)` slot is present return it, else insert a `SharedIndexArrangement` built from `Published::placeholder(peers)` for both `oks` and `errs` and return it. Change the render publish path (Task 6 wires the interactive side; here just make the maintenance side adopt) so `render.rs`'s publish calls `get_or_create_placeholder` then `adopt`, rather than `insert`. Keep the `remove` and alias behavior unchanged.

- [ ] **Step 4: Run the test and the pre-existing registry tests**

Run: `bin/cargo-test -p mz-compute -- sharing::tests`
Expected: PASS, including `get_or_create_converges_on_one_slot` and the existing registry tests.

- [ ] **Step 5: Format, check, commit**

```bash
bin/fmt
cargo check -p mz-compute
git add src/compute/src/sharing.rs src/compute/src/render.rs
git commit -m "compute: registry get-or-create for publication points

Whichever runtime touches an id first creates its placeholder slot; the other
adopts or reads the same Arc. Replaces create-fresh-and-overwrite so a placeholder
a reader already imports is filled in place and never overwritten.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01CY7GHdrTBfAG4tgvSgPGJ9"
```

---

### Task 4: Placeholder close and eviction

A never-adopted placeholder must not wedge its importers. Two closes: the maintenance publisher's existing close-on-drop covers an adopted slot; a never-adopted placeholder closes and is evicted when its last reader leaves.

**Files:**
- Modify: `src/compute/src/shared_trace.rs` (terminal close reachable without a publisher), `src/compute/src/sharing.rs` (evict a placeholder on last reader).
- Test: `src/compute/src/sharing.rs` tests.

**Interfaces:**
- Produces:
  - `Published::<Tr>::close(&self)` (or a registry-level `close_placeholder(id, worker_index)`): marks the publication point closed and pushes a terminal `Frontier(Antichain::new())` to every importer queue, mirroring `Publisher::drop`.
  - Registry eviction: when the last `SharedTraceHandle` for a never-adopted placeholder drops, the slot is removed. Track reader count on the publication point or detect via `Arc::strong_count` under the `map` lock.

- [ ] **Step 1: Write the failing tests**

```rust
#[mz_ore::test]
fn placeholder_close_terminates_importers() {
    // An import on a placeholder that is closed (never adopted) receives a terminal
    // empty frontier and completes, rather than holding its frontier at minimum.
    // Build a one-worker import over a placeholder, step, assert no output and
    // frontier at [0]; close the placeholder; step; assert the import frontier is
    // empty (the operator completed).
}

#[mz_ore::test]
fn last_reader_evicts_unadopted_placeholder() {
    let id = GlobalId::User(1);
    let registry = ArrangementSharingRegistry::new();
    let slot = registry.get_or_create_placeholder(id, 0, 1);
    assert!(registry.handles(&id, 0).is_some());
    drop(slot); // last reader of a never-adopted placeholder
    registry.evict_unadopted(&id, 0); // or automatic on drop
    assert!(registry.handles(&id, 0).is_none());
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `bin/cargo-test -p mz-compute -- sharing::tests::placeholder_close sharing::tests::last_reader`
Expected: FAIL to compile.

- [ ] **Step 3: Implement close and eviction**

Add the terminal-close path to the publication point (reuse the `closed`-flag and terminal `Frontier` logic from `Publisher::drop`). Add registry eviction of a never-adopted placeholder when its last reader leaves. An adopted slot is NOT evicted this way; it is closed by the maintenance drop through the existing `remove` path.

- [ ] **Step 4: Run the tests**

Run: `bin/cargo-test -p mz-compute -- sharing::tests`
Expected: PASS.

- [ ] **Step 5: Format, check, commit**

```bash
bin/fmt
cargo check -p mz-compute
git add src/compute/src/shared_trace.rs src/compute/src/sharing.rs
git commit -m "compute: close and evict never-adopted placeholders

A placeholder whose index creation is cancelled before it publishes must not
wedge its importers. Closing pushes a terminal empty frontier to them and the
registry evicts the slot when its last reader leaves. An adopted slot is closed
by the maintenance drop instead.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01CY7GHdrTBfAG4tgvSgPGJ9"
```

---

### Task 5: Bounded-read routing predicate

Route a `CreateDataflow` to interactive only when its `until` is bounded (non-empty) and it has no subscribe or copy-to sink. Fixes the current predicate, which routes copy-to to interactive.

**Files:**
- Modify: `src/compute-client/src/multiplex.rs` (the `to_interactive` decision in `send`, currently `desc.is_transient() && desc.subscribe_ids().next().is_none()`).
- Test: `src/compute-client/src/multiplex.rs` tests.

**Interfaces:**
- Consumes: `DataflowDescription::until` (Antichain), `subscribe_ids()`, `copy_to_ids()` (all on `mz-compute-types` `dataflows.rs`).
- Produces: `to_interactive = !desc.until.is_empty() && desc.subscribe_ids().next().is_none() && desc.copy_to_ids().next().is_none()`.

- [ ] **Step 1: Write the failing tests**

```rust
#[mz_ore::test(tokio::test)]
async fn routing_excludes_copy_to_and_subscribe_and_unbounded() {
    // A bounded, transient, sinkless dataflow routes to interactive.
    // A copy-to dataflow (finite until, transient) routes to maintenance.
    // A subscribe dataflow routes to maintenance.
    // An unbounded-until dataflow routes to maintenance.
    // Assert via the harness which runtime received each CreateDataflow.
}
```

Model the harness on the existing `multiplex.rs` routing tests.

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-compute-client -- multiplex::tests::routing_excludes`
Expected: FAIL (copy-to currently routes to interactive).

- [ ] **Step 3: Update the predicate**

Replace the `to_interactive` computation in `send` with `!desc.until.is_empty() && desc.subscribe_ids().next().is_none() && desc.copy_to_ids().next().is_none()`. Add a one-line comment that copy-to is excluded for its S3 sink and reconciliation, not for frontier reasons.

- [ ] **Step 4: Run the test**

Run: `bin/cargo-test -p mz-compute-client -- multiplex::tests`
Expected: PASS.

- [ ] **Step 5: Format, check, commit**

```bash
bin/fmt
cargo check -p mz-compute-client
git add src/compute-client/src/multiplex.rs
git commit -m "compute-client: route to interactive by the bounded-read predicate

Route a CreateDataflow to the interactive runtime only when its until is bounded
and it has no subscribe or copy-to sink. Copy-to is finite-until but drives an S3
sink and is refused by reconciliation, so it belongs on maintenance.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01CY7GHdrTBfAG4tgvSgPGJ9"
```

---

### Task 6: Arrival-order rendering with late-bound imports

Remove the per-worker deferral (the latent construction-order nondeterminism) and render in command arrival order. An interactive import binds through `get_or_create_placeholder`, so a not-yet-published dependency yields a placeholder that adopts later.

**Files:**
- Modify: `src/compute/src/compute_state.rs` (`handle_create_dataflow`, `resolve_dirty`, `pending_work`, `dep_index`, `enqueue_deferred_dataflow`, `scheduled_before_build`), `src/compute/src/render.rs` (`import_index_shared` binds through get-or-create).
- Test: a clusterd-level or integration test plus the regression suites in Task 9. The unit-level guarantee is that `handle_create_dataflow` no longer defers.

**Interfaces:**
- Consumes: `get_or_create_placeholder` (Task 3), placeholder handles (Task 2).
- Produces: `handle_create_dataflow` builds every dataflow immediately in arrival order. `import_index_shared` obtains its input handle from `get_or_create_placeholder(id, worker_index, peers)` rather than from `handles(id).unwrap()`.

- [ ] **Step 1: Write a failing test that a deferred dependency no longer blocks the build**

Add a `compute_state`-level test (model on the existing render/import tests) that renders an interactive dataflow importing an id whose slot is a placeholder (not yet adopted), and asserts the dataflow is built immediately (its collection appears in `collections`) with its output frontier held at the minimum, rather than sitting in `pending_work`.

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-compute -- compute_state::tests::interactive_build_is_immediate`
Expected: FAIL (the dataflow is deferred into `pending_work`).

- [ ] **Step 3: Bind imports through get-or-create in render**

In `render.rs` `import_index_shared`, replace the `registry.handles(id, worker_index)` lookup and its `None` panic with `registry.get_or_create_placeholder(id, worker_index, peers)` and mint the handle from the returned slot. The import is now always buildable; a placeholder yields an empty import held at the minimum frontier that fills on adoption.

- [ ] **Step 4: Remove the deferral in `handle_create_dataflow`**

Delete the deferral branch in `handle_create_dataflow` that enqueues into `pending_work` when a dependency is unpublished. Build every dataflow in arrival order. Remove `enqueue_deferred_dataflow`, the `PendingWork::Dataflow` handling in `resolve_dirty`, `dep_index` entries for deferred dataflows, and `scheduled_before_build` (no longer needed, since builds are not deferred). Keep `pending_work`/`resolve_dirty` only if still used by the shared-index peek path; if that path also binds through get-or-create, remove `pending_work` entirely.

- [ ] **Step 5: Run the test and the compute unit tests**

Run: `bin/cargo-test -p mz-compute -- compute_state::tests render::tests`
Expected: PASS, including `interactive_build_is_immediate`.

- [ ] **Step 6: Format, check, commit**

```bash
bin/fmt
cargo check -p mz-compute
git add src/compute/src/compute_state.rs src/compute/src/render.rs
git commit -m "compute: render interactive dataflows in arrival order with late-bound imports

Remove the per-worker deferral, which built dataflows in each worker's own
publication order and so allocated timely channel ids in a worker-divergent
order. Every dataflow now builds immediately in command arrival order, and an
import of a not-yet-published dependency binds through a registry placeholder that
adopts later.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01CY7GHdrTBfAG4tgvSgPGJ9"
```

---

### Task 7: Assert `since <= as_of` on the interactive import path

The interactive import path lacks the `since <= as_of` check the maintenance path has. A violation is a protocol error, so it panics rather than reading coalesced data.

**Files:**
- Modify: `src/compute/src/render.rs` (`import_index_shared`, where the maintenance path asserts `compaction_frontier <= as_of` around line 709-713).
- Test: `src/compute/src/render.rs` or `shared_trace.rs` tests.

**Interfaces:**
- Consumes: the imported slot's `since` (meet of oks and errs), the dataflow `as_of`.
- Produces: an assertion `meet(oks.since, errs.since) <= as_of` at capture.

- [ ] **Step 1: Write the failing test**

```rust
#[mz_ore::test]
#[should_panic(expected = "since")]
fn import_asserts_since_at_most_as_of() {
    // Publish an index, advance its since past a chosen as_of, then import at that
    // as_of. The assert must fire.
}
```

- [ ] **Step 2: Run to verify it fails (no panic yet)**

Run: `bin/cargo-test -p mz-compute -- import_asserts_since_at_most_as_of`
Expected: FAIL (the `should_panic` does not panic, because the assert is missing).

- [ ] **Step 3: Add the assert**

In `import_index_shared`, after obtaining the slot handles and before capture, assert `PartialOrder::less_equal(&meet(oks.since(), errs.since()).borrow(), &as_of.borrow())` with a message naming the ids and frontiers. Use the same `meet` helper the readiness check uses.

- [ ] **Step 4: Run the test**

Run: `bin/cargo-test -p mz-compute -- import_asserts_since_at_most_as_of`
Expected: PASS.

- [ ] **Step 5: Format, check, commit**

```bash
bin/fmt
cargo check -p mz-compute
git add src/compute/src/render.rs
git commit -m "compute: assert since <= as_of on the interactive import path

Matches the assert the maintenance import path already makes. A since above the
requested as_of means the controller offered an unreadable as_of, a protocol
error, so it panics rather than reading coalesced data silently.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01CY7GHdrTBfAG4tgvSgPGJ9"
```

---

### Task 8: Remove the dead live `import()`

The interactive runtime never issues an unbounded read, so the live `import()` (non-snapshot) is dead outside tests.

**Files:**
- Modify: `src/compute/src/shared_trace.rs` (delete `import`), and any test-only callers.
- Test: existing tests continue to pass.

**Interfaces:**
- Removes: `SharedTraceHandle::import` (the non-snapshot import). `import_snapshot_at` remains the only import path.

- [ ] **Step 1: Confirm no non-test callers**

Run: `grep -rn "\.import(" src/compute/src/ | grep -v import_snapshot_at | grep -v "tests\|#\[cfg(test"`
Expected: only test callers (`sharing.rs` and `shared_trace.rs` test modules).

- [ ] **Step 2: Delete `import` and rewrite its test callers to use `import_snapshot_at`**

Remove `import`. Where a test used `import`, switch to `import_snapshot_at(scope, name, as_of, until)` with an `until` of `as_of.step_forward()` for a single-time read or the test's bound for a range.

- [ ] **Step 3: Run the compute tests**

Run: `bin/cargo-test -p mz-compute -- shared_trace::tests sharing::tests`
Expected: PASS.

- [ ] **Step 4: Format, check, commit**

```bash
bin/fmt
cargo check -p mz-compute
git add src/compute/src/shared_trace.rs src/compute/src/sharing.rs
git commit -m "compute: remove the dead live import(), keep import_snapshot_at

The interactive runtime issues only bounded reads, so the unbounded live import
had no non-test callers. import_snapshot_at is the sole interactive import path.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01CY7GHdrTBfAG4tgvSgPGJ9"
```

---

### Task 9: Regression and concurrency suites under two-runtime on

Confirm the end-to-end behavior against the failures that motivated the design.

**Files:**
- No source changes. Run existing suites with `enable_two_runtime_compute` on.

**Interfaces:** none.

- [ ] **Step 1: Run the row-doubling and panic regressions**

Run each under two-runtime on:
```bash
bin/mzcompose --find testdrive down && bin/mzcompose --find testdrive run default -- linear-join-fuel.td
bin/mzcompose --find testdrive down && bin/mzcompose --find testdrive run default -- materializations.td
```
Expected: both pass, no `delayed capability` panic, no hang after `DROP INDEX`.

- [ ] **Step 2: Run the row-doubling slt files**

```bash
SYSTEM_PARAMETER_DEFAULT="enable_two_runtime_compute=true" bin/sqllogictest -- \
  test/sqllogictest/information_schema_columns.slt test/sqllogictest/object_ownership.slt
```
Expected: PASS (no doubled rows).

- [ ] **Step 3: Run the previously-hanging slt trio**

```bash
SYSTEM_PARAMETER_DEFAULT="enable_two_runtime_compute=true" bin/sqllogictest -- \
  test/sqllogictest/role_membership.slt test/sqllogictest/unsigned_int.slt \
  test/sqllogictest/with_mutually_recursive.slt
```
Expected: PASS (budget the first-run compile time so a timeout does not read as a hang).

- [ ] **Step 4: Run a create/drop/peek concurrency stress**

Run the cluster workflow that exercises create, drop, and peek over shared indexes (for example the `cluster` composition's read-frontier and drop tests) under two-runtime on. Expected: no panic, no stranded read hold, `read frontier` advances.

- [ ] **Step 5: Commit any golden updates**

If any expected-output golden legitimately changed (for example arrangement-size bounds under Arc-batch overhead), rewrite it with the tool's `--rewrite-results` and commit with a message explaining the bound change. Otherwise no commit.

---

## Self-review notes

- **Spec coverage:** F1 single-source feed (Task 1), F2 placeholder/adopt (Task 2), registry get-or-create (Task 3), placeholder close/eviction (Task 4), bounded-read routing incl. copy-to exclusion (Task 5), arrival-order rendering / deferral removal / late-binding (Task 6), `since <= as_of` assert (Task 7), delete live `import()` (Task 8), regression + concurrency (Task 9). The `meet(oks, errs)` readiness/since gating is used in Tasks 6-7. Shared-import-by-role needs no new command, so there is no routing-declaration task beyond Task 5.
- **Ordering dependency:** Task 6 (deferral removal) depends on Tasks 2-3 (placeholder/get-or-create) so imports can late-bind. Do not reorder Task 6 before Task 3.
- **Type consistency:** `get_or_create_placeholder` returns a `SharedIndexArrangement` slot in Tasks 3-6; `Published::placeholder(peers)` and `adopt(&Published)` in Tasks 2-4; the routing predicate reads `until`/`subscribe_ids`/`copy_to_ids` in Task 5.
- **Open validation carried into implementation:** Task 4's eviction-vs-late-adoption race (a publish arriving as the last reader leaves must not lose the slot) is the one remaining design-level unknown; its cross-thread test in Task 4 is where it gets pinned.
