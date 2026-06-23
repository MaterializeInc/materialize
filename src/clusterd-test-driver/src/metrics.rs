// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Scraping clusterd's Prometheus `/metrics` endpoint.
//!
//! The driver speaks the compute protocol but does not see the replica's internal
//! state. Some behaviors are only observable through clusterd's own metrics, so this
//! module reads the Prometheus text endpoint clusterd's internal HTTP server exposes
//! and sums named series for the `metrics` command to assert on.
//!
//! The motivating use case is the persist-sink correction buffer (CLU-131): a
//! read-only materialized-view sink holds a roughly full-MV-size correction buffer
//! that a read-write sink consolidates to ~0. The live buffer occupancy is not a
//! gauge; it is the difference of two monotonic counters,
//! `mz_persist_sink_correction_insertions_total - …_deletions_total`, which the
//! `metrics` command expresses with its `minus` operand.

use anyhow::Context;
use mz_ore::cast::CastLossy;

/// Fetch the raw Prometheus text body from a clusterd `/metrics` URL.
pub async fn fetch(metrics_url: &str) -> anyhow::Result<String> {
    reqwest::get(metrics_url)
        .await
        .with_context(|| format!("scraping clusterd metrics at {metrics_url}"))?
        .error_for_status()
        .context("clusterd metrics endpoint returned an error status")?
        .text()
        .await
        .context("reading clusterd metrics body")
}

/// Sum the values of every series of `name` in a Prometheus text `body`, optionally
/// restricted to series carrying a `select` label (`(label, value)`).
///
/// Summing across series gives a process-wide total over all label combinations
/// (e.g. per-worker series), which is the natural quantity to assert on. A metric
/// absent from the body sums to 0: a counter's family is registered lazily on first
/// use, so before the relevant code path runs the line is simply not present. Values
/// are emitted by the Prometheus text encoder as (possibly floating-point) numbers;
/// the counters and gauges of interest are integral, so a lossy cast back to `u64`
/// recovers the count.
pub fn sum_metric(body: &str, name: &str, select: Option<(&str, &str)>) -> anyhow::Result<u64> {
    let mut total: u64 = 0;
    for line in body.lines() {
        // Skip `# HELP`/`# TYPE` comment lines.
        if line.starts_with('#') {
            continue;
        }
        // A sample line is `series value` (mz's encoder emits no trailing
        // timestamp), where `series` is `name` or `name{labels}` with no unquoted
        // whitespace, so the value is the final whitespace-separated token.
        let Some((series, value)) = line.rsplit_once(char::is_whitespace) else {
            continue;
        };
        if !series_matches(series, name, select) {
            continue;
        }
        let parsed: f64 = value
            .parse()
            .with_context(|| format!("metric {name} value {value:?} is not a number"))?;
        total += u64::cast_lossy(parsed);
    }
    Ok(total)
}

/// Whether a Prometheus `series` token (`name` or `name{labels}`) is named `name`
/// and, when `select` is given, carries that exact `label="value"` pair.
fn series_matches(series: &str, name: &str, select: Option<(&str, &str)>) -> bool {
    let (series_name, labels) = match series.split_once('{') {
        Some((n, rest)) => (n, rest.strip_suffix('}').unwrap_or(rest)),
        None => (series, ""),
    };
    if series_name != name {
        return false;
    }
    match select {
        None => true,
        // Labels are `k1="v1",k2="v2"`; match one entry exactly.
        Some((key, value)) => {
            let needle = format!("{key}=\"{value}\"");
            labels.split(',').any(|entry| entry.trim() == needle)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = "\
# HELP mz_persist_sink_correction_insertions_total The cumulative insertions.
# TYPE mz_persist_sink_correction_insertions_total counter
mz_persist_sink_correction_insertions_total 200000
mz_persist_sink_correction_deletions_total 199950
mz_persist_sink_correction_capacity_increases_total 4096
mz_persist_sink_correction_max_per_sink_worker_len_updates{worker_id=\"0\"} 60000
mz_persist_sink_correction_max_per_sink_worker_len_updates{worker_id=\"1\"} 40000
";

    /// A counter is read by exact name, comment lines are skipped, an absent metric
    /// sums to 0, and a name that is a prefix of another metric does not match it.
    #[mz_ore::test]
    fn sums_unlabeled_counters() {
        assert_eq!(
            sum_metric(BODY, "mz_persist_sink_correction_insertions_total", None).unwrap(),
            200000
        );
        assert_eq!(
            sum_metric(BODY, "mz_persist_sink_correction_deletions_total", None).unwrap(),
            199950
        );
        // Absent metric sums to zero.
        assert_eq!(
            sum_metric(BODY, "mz_persist_sink_absent_total", None).unwrap(),
            0
        );
    }

    /// Multiple label series of one metric sum to a process-wide total, and a label
    /// selector restricts to a single series.
    #[mz_ore::test]
    fn sums_and_selects_labeled_series() {
        let name = "mz_persist_sink_correction_max_per_sink_worker_len_updates";
        assert_eq!(sum_metric(BODY, name, None).unwrap(), 100000);
        assert_eq!(
            sum_metric(BODY, name, Some(("worker_id", "0"))).unwrap(),
            60000
        );
        assert_eq!(
            sum_metric(BODY, name, Some(("worker_id", "1"))).unwrap(),
            40000
        );
        // A selector that matches no series sums to zero.
        assert_eq!(sum_metric(BODY, name, Some(("worker_id", "2"))).unwrap(), 0);
    }

    /// A float-encoded counter value casts back to its integral count.
    #[mz_ore::test]
    fn casts_float_encoded_value() {
        let body = "mz_persist_sink_correction_deletions_total 12345.0\n";
        assert_eq!(
            sum_metric(body, "mz_persist_sink_correction_deletions_total", None).unwrap(),
            12345
        );
    }
}
