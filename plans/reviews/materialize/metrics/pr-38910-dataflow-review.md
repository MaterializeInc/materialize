---
status: ready-for-review
category: metrics
title: "Review of PR #38910 — compute: retry metric sink collector registration (dataflow lens)"
branch: pr-38910
updated: 2026-09-17
outcome: "LGTM through the dataflow semantics/cost lens; no blocking or should-fix findings. Full write-up in review-dataflow.md."
---

# PR #38910 review — dataflow semantics and cost lens

This is lens 1 of a fanned, fresh-eyes review of PR #38910
(`compute: retry metric sink collector registration`). Scope: **is the change
right, and affordable, as incremental dataflow?** Other reviewers cover
correctness-under-failure and general engineering quality; this write-up does not
duplicate them.

The lens review lives at `review-dataflow.md` at the repo root (the file the launch
instructions asked for). This plan file records the process and decisions.

## What the PR does (as it bears on this lens)

- `src/ore/src/metrics.rs`: adds `try_register_collector_with_dropper`, a fallible
  variant of `register_collector_with_dropper`; re-bases the soft-panicking one on it.
- `src/compute/src/sink/metric_sink.rs`: the metric sink's active worker now owns a
  `PendingRegistration`, registers at build time, and retries once a second via
  `activator.activate_after` on a `Desc`-id collision. The registration guard moves
  from the returned sink token into the operator closure; `render_sink` now returns
  `None`.
- `src/compute/src/metrics.rs` + `doc/user/data/metrics.yml`: new counter
  `mz_compute_metric_sink_registration_retries_total`.

## Verdict

LGTM. The change does not touch the fold, exchange routing, per-worker frontier
reporting, or any arrangement, so emit/retract/frontier-advance semantics,
consolidation, and arrangement sharing are unchanged. The one real cost delta —
timer-driven activations during a collision window — is bounded and small, not a
cost cliff. No blocking or should-fix items.

## Decision Log

- **Scoped strictly to the dataflow lens.** Deferred teardown-*ordering* of the guard
  (moved from sink token to operator closure) to the correctness-under-failure lens,
  since the launch instructions assign it there. Recorded only that, from a
  dataflow-progress angle, the `None` return is inert (`sink_write_frontier` and the
  collection's `sink_token` are still installed independently). Rejected treating it as
  a finding here to avoid duplicating another lens.
- **Did not flag the per-second O(n) `publish_if_healthy` rebuild during contention as
  a bug.** Traced it to bounded work (short window, ~1 extra rebuild/sec, n = series
  count) and confirmed the tempting "early-return to skip it" fix is explicitly
  forbidden by the NOTE at `metric_sink.rs:148-153` (it would downgrade `since` ahead
  of the fold). Kept it as a nit, not a should-fix. Rejected recommending the early
  return.
- **Did not build or test locally** (reviewer role; CI compiles/tests every PR).
- **Verification for this lens = tracing logic to a verdict**, per the reviewer
  charter: read `render/sinks.rs` to confirm the `None`-token path and `needed_tokens`
  handling, and read `integrate`/`publish_if_healthy` to bound the timer-activation
  cost. No dangling suspicions.
