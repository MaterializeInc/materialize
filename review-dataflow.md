# PR #38910 review — lens: dataflow semantics and cost

**Verdict: LGTM through this lens.** The change is confined to *when* and *how* the
active worker registers its prometheus collector. It does not touch the fold
(`stage_ok`/`stage_err`/`integrate`/`publish_if_healthy`), the exchange routing, the
per-worker frontier reporting, or any arrangement, so the operator's emit/retract/
frontier-advance semantics are unchanged. Retraction correctness, consolidation, and
arrangement sharing are all unaffected. The one real cost delta (timer-driven
activations during a registration collision) is bounded and small. No blocking or
should-fix items from this lens.

## Blocking

None.

## Should-fix

None.

## Nits / observations

- **Bounded extra O(n) rebuilds during the collision window.**
  `src/compute/src/sink/metric_sink.rs:165-168` — while registration is contended the
  active worker self-reschedules every `REGISTRATION_RETRY_INTERVAL` (1s) via
  `activator.activate_after`. Each such timer activation runs the whole closure body,
  including `st.integrate(&frontier)` and `publish_if_healthy()`, and
  `publish_if_healthy` is a documented full O(n) rebuild over the live series set
  (`metric_sink.rs:535`). So during the collision window (seconds, bounded by the old
  dataflow's teardown) the sink pays one extra O(n) rebuild per second on top of any
  data-driven activations. This is *not* a cost cliff: n is the sink's series count,
  the window is short, and a data-driven activation already does the same O(n) work, so
  the timer adds at most ~1 rebuild/sec. Worth noting only because the rebuilt state is
  not even scrapeable until registration succeeds, so it is briefly wasted work. Do
  **not** "fix" this with an early return between the frontier publish and `integrate`:
  the NOTE at `metric_sink.rs:148-153` documents that such a return would advertise
  progress the sink has not folded and downgrade the input's `since` prematurely. The
  current no-early-return shape is deliberately correct; the extra work is the price of
  that invariant and is acceptable.

- **Per-activation `try_register` call stays on the hot path but is O(1).**
  `metric_sink.rs:165` calls `try_register` on every active-worker activation. After
  the first success it short-circuits on `self.handle.is_some()`
  (`metric_sink.rs:244`), i.e. one pointer check per activation. Negligible; noted only
  for completeness since it sits on the per-update path.

- **Registration guard moved from the sink token to the operator closure**
  (`metric_sink.rs:214-216`, return changed from `Some(Rc::new(drop_handle))` to
  `None`). This changes *when* the old incarnation's registration unregisters relative
  to reconciliation's token-nulling. From the dataflow-progress angle it is inert: the
  collection's `sink_token` is still set from `needed_tokens`
  (`render/sinks.rs:168-173`) and `sink_write_frontier` is still installed
  unconditionally (`metric_sink.rs:211-212`), so nothing about frontier/`since`
  reporting or dataflow liveness depends on this operator returning a token. The
  teardown-*ordering* implications (does the guard drop early enough / does it always
  drop) belong to the correctness-under-failure lens; flagging only that this lens
  found no progress or capability hazard in the `None` return. The operator holds no
  output capability (`_capabilities` is ignored), so there is no dropped/held-capability
  stall-or-leak concern.

## Checked and cleared

- **Non-active workers.** `shared_frontier` is updated at `metric_sink.rs:154` *before*
  the `let Some(registration) = ... else { drain; return }` at `:157`, so every worker
  still reports its frontier and the controller's meet is unchanged. The drain-and-
  return still prevents infinite rescheduling on non-active workers.
- **No double registration.** Build-time registration (`:132-136`) sets `handle`;
  the first activation's `try_register` then short-circuits (`:244`). Retry loop
  converges and stops rescheduling once `handle` is set — no unbounded self-activation
  in the success case.
- **No new arrangement / no lost sharing.** `scope.clone()` (`:106`) is an `Rc` clone;
  the collector rebuild path and exchange-by-sink-id routing are untouched.
