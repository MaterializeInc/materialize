# Architecture review — `adapter::coord::sequencer::inner`

Scope: `src/adapter/src/coord/sequencer/inner/` and all subdirs (≈ 7,133 LOC of Rust).

## 1. Parallel match-on-enum dispatch in `Staged` impls

**Files**
- `src/adapter/src/coord/sequencer/inner/peek.rs:58-99` — `impl Staged for PeekStage` with twin matches on the same 9-variant enum (`validity()` and `stage()`).
- `src/adapter/src/coord/sequencer/inner/subscribe.rs` — same shape for `SubscribeStage`.
- Other `Staged` impls (likely): create_index, create_materialized_view, create_view, create_continual_task, copy_from, explain_timestamp.

**Problem**
Each stage-enum variant carries a `PlanValidity` field and a corresponding handler method on `Coordinator`. Adding or renaming a variant requires updating two parallel match arms in lockstep, plus the variant's struct definition, plus the handler on `Coordinator`. The friction is the *parallel predicates that should live on a single object* pattern from the playbook: `validity()` is just per-variant `&mut self.validity`, and `stage()` is per-variant `coord.<handler>(...)`. Both predicates are mechanical surfaces of "what is variant *X*?".

**Solution sketch**
Replace each `XxxStage` enum with a `Box<dyn StageStep>` trait object (or, if heap is undesirable, a generated `enum_dispatch`-style match). Each variant becomes a struct that owns its `validity: PlanValidity` and implements `async fn run(self: Box<Self>, coord, ctx) -> StageResult<Box<dyn StageStep>>`. The two parallel matches collapse; the `StageStep` trait is the *interface*; each per-variant struct is the *implementation* in one place.

**Benefits**
- Locality: changes to the peek pipeline live in `peek.rs` only — no second match elsewhere to keep in sync.
- Leverage: the `Staged` driver in `inner.rs` becomes a 5-line loop over `dyn StageStep` regardless of how many statement types use it.
- Test surface: each stage step is independently testable as a unit (construct the struct, invoke `run`, assert on the returned `StageResult`).

**Risk / why it might not deepen things**
The `StageResult::Handle` return type currently carries `Box<Self>` for re-enqueueing; converting to `Box<dyn StageStep>` requires the message channel to be polymorphic over stage types. Worth verifying that `Message::PeekStageReady(PeekStage)` and similar message-handler arms are not relying on the concrete enum shape elsewhere. If they are, this refactor is *relocation* rather than *deepening*.

## 2. (Honest skip) "10 files in one directory" is not friction

The 10 child files map to 10 distinct SQL statement classes. Each one has a coherent, non-overlapping responsibility (peek, subscribe, cluster, copy_from, …). The deletion test fails: collapsing them into `inner.rs` would put 7K LOC behind one open buffer. No deepening to find here.

## 3. (Honest skip) `return_if_err!` macro

Looks like the kind of macro that hides control flow, but its three-line expansion is exactly "send error response, return early"; replacing with explicit `match`-and-`return` would pad each call site by 3-4 lines and provide no leverage. Keep.

## What this review did *not* reach

- Per-stage struct interfaces (e.g., `PeekStageOptimize` field shape) — would require reading each handler on `Coordinator` to judge whether the stage struct's fields are minimal. Defer to the `coord/` review.
- Message-handler routing — same. The `Staged` re-queue pathway is half here, half in `coord::message_handler`; a useful seam-level review needs both.
