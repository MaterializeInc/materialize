// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/** One reading of a source's ingestion watermarks. */
export interface IngestionSample {
  /** Client wall-clock time of the reading, in epoch ms. */
  atMs: number;
  /** Latest upstream offset the source knows about. */
  offsetKnown: number;
  /** Latest offset durably ingested. */
  offsetCommitted: number;
}

export type SourceHealthVerdict =
  /** Lag is within normal steady-state jitter. */
  | "healthy"
  /** Behind, but we don't yet have enough samples to compute rates. */
  | "measuring"
  /** Behind and processing effectively nothing. */
  | "stalled"
  /** Behind and ingesting slower than upstream produces; will not recover. */
  | "fallingBehind"
  /** Behind but ingesting faster than upstream produces. */
  | "catchingUp";

export interface SourceHealthDiagnosis {
  verdict: SourceHealthVerdict;
  /** offset_known - offset_committed at the latest sample. */
  backlog: number | null;
  /** Upstream production rate, offsets/s. Null until two spaced samples. */
  inflowPerSec: number | null;
  /** Ingestion rate, offsets/s. Null until two spaced samples. */
  drainPerSec: number | null;
  /** Estimated time to drain the backlog. Only set for `catchingUp`. */
  etaMs: number | null;
  /** Approximate wall-clock time ingestion last kept up, derived from lag. */
  stalledSinceMs: number | null;
}

/** Lag below this is steady-state jitter, not a problem worth flagging. */
export const HEALTHY_LAG_MS = 5 * 60 * 1000;
/** Source statistics only tick about once a minute upstream, so rates need
 * at least this much spacing to be meaningful. */
export const MIN_RATE_SPAN_MS = 45 * 1000;
/** Drain below this fraction of inflow reads as fully stalled. */
const STALLED_DRAIN_FRACTION = 0.02;
/** A backlog under this many seconds' worth of inflow counts as caught up. */
const CAUGHT_UP_BACKLOG_SECONDS = 60;

export const diagnoseSourceHealth = ({
  samples,
  lagMs,
  nowMs,
}: {
  samples: IngestionSample[];
  lagMs: number | null;
  nowMs: number;
}): SourceHealthDiagnosis => {
  const latest = samples.at(-1);
  const backlog = latest
    ? Math.max(latest.offsetKnown - latest.offsetCommitted, 0)
    : null;
  const stalledSinceMs = lagMs === null ? null : nowMs - lagMs;

  const base = {
    backlog,
    inflowPerSec: null,
    drainPerSec: null,
    etaMs: null,
    stalledSinceMs,
  };

  if (lagMs !== null && lagMs < HEALTHY_LAG_MS) {
    return { verdict: "healthy", ...base };
  }

  // Rates come from the widest available pair of samples, so a brief burst
  // in either direction doesn't flip the verdict.
  const baseline = latest
    ? samples.find((s) => latest.atMs - s.atMs >= MIN_RATE_SPAN_MS)
    : undefined;
  if (!latest || !baseline || baseline === latest) {
    return { verdict: "measuring", ...base };
  }

  const spanSec = (latest.atMs - baseline.atMs) / 1000;
  const inflowPerSec = Math.max(
    (latest.offsetKnown - baseline.offsetKnown) / spanSec,
    0,
  );
  const drainPerSec = Math.max(
    (latest.offsetCommitted - baseline.offsetCommitted) / spanSec,
    0,
  );
  const rates = { ...base, inflowPerSec, drainPerSec };

  // The lag input can be a windowed maximum that stays high long after
  // recovery, so a near-empty backlog wins over it: the source has ingested
  // everything upstream offers, which is as healthy as ingestion gets.
  // Frozen watermarks don't qualify: a paused source (or stale statistics)
  // reports an unmoving backlog of zero and must not read as healthy.
  if (
    backlog !== null &&
    backlog <= Math.max(inflowPerSec * CAUGHT_UP_BACKLOG_SECONDS, 1) &&
    (inflowPerSec > 0 || drainPerSec > 0)
  ) {
    return { verdict: "healthy", ...rates };
  }

  if (drainPerSec <= inflowPerSec * STALLED_DRAIN_FRACTION) {
    return { verdict: "stalled", ...rates };
  }
  if (drainPerSec <= inflowPerSec) {
    return { verdict: "fallingBehind", ...rates };
  }
  const etaMs =
    backlog !== null && backlog > 0
      ? (backlog / (drainPerSec - inflowPerSec)) * 1000
      : null;
  return { verdict: "catchingUp", ...rates, etaMs };
};
