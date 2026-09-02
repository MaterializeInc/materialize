// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runtime filtering of the metrics a process exports.
//!
//! Many metric families carry one series per catalog object, persist shard,
//! replica or cluster, so a single `/metrics` response grows with the catalog.
//! Scrapers reject oversized responses outright, which loses every metric of
//! the process at once. The export filter is a gather-time postprocessor on
//! the [`MetricsRegistry`] that drops families and scopes series by cluster or
//! replica according to the `metrics_export_*` dyncfgs, so an operator can
//! shed cardinality at runtime without a restart.
//!
//! The filter changes what is exported, not what is recorded. Filtered series
//! still exist in the registry, so re-enabling a family restores its current
//! values, and the process still pays the cost of maintaining them.
//!
//! The filter's own metrics are registered in the same registry it filters
//! and are exempt from filtering, so an operator can always see what was
//! dropped. The registry gathers them before the postprocessor runs, so each
//! scrape reports the filter's view of the previous scrape.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::raw::{IntCounterVec, UIntGaugeVec};
use mz_ore::metrics::{MetricsRegistry, UIntGauge};
use prometheus::proto::{LabelPair, Metric, MetricFamily};

use crate::dyncfgs::{
    METRICS_EXPORT_CLUSTER_ALLOWLIST, METRICS_EXPORT_DISABLED_FAMILIES,
    METRICS_EXPORT_MAX_SERIES_PER_FAMILY, METRICS_EXPORT_REPLICA_ALLOWLIST,
};

/// Labels whose value identifies a cluster.
const CLUSTER_LABELS: &[&str] = &["instance_id", "cluster_id", "compute_instance"];
/// Labels whose value identifies a replica.
const REPLICA_LABELS: &[&str] = &["replica_id"];
/// Name prefix of the filter's own metrics, which are never filtered.
const SELF_METRIC_PREFIX: &str = "mz_metrics_export_";

/// A family name pattern from `metrics_export_disabled_families`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FamilyPattern {
    Exact(String),
    Prefix(String),
}

impl FamilyPattern {
    fn parse(s: &str) -> Self {
        match s.strip_suffix('*') {
            Some(prefix) => FamilyPattern::Prefix(prefix.to_owned()),
            None => FamilyPattern::Exact(s.to_owned()),
        }
    }

    fn matches(&self, name: &str) -> bool {
        match self {
            FamilyPattern::Exact(exact) => name == exact,
            FamilyPattern::Prefix(prefix) => name.starts_with(prefix),
        }
    }
}

/// Why a series or family was removed from the export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DropReason {
    DisabledFamily,
    ClusterAllowlist,
    ReplicaAllowlist,
    OverCap,
}

impl DropReason {
    fn label(self) -> &'static str {
        match self {
            DropReason::DisabledFamily => "disabled_family",
            DropReason::ClusterAllowlist => "cluster_allowlist",
            DropReason::ReplicaAllowlist => "replica_allowlist",
            DropReason::OverCap => "over_cap",
        }
    }
}

/// The raw dyncfg values the filter is configured from, kept so a gather can
/// detect a change without re-parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RawConfig {
    disabled_families: String,
    cluster_allowlist: String,
    replica_allowlist: String,
    max_samples_per_family: usize,
}

impl RawConfig {
    fn read(config_set: &ConfigSet) -> Self {
        RawConfig {
            disabled_families: METRICS_EXPORT_DISABLED_FAMILIES.get(config_set),
            cluster_allowlist: METRICS_EXPORT_CLUSTER_ALLOWLIST.get(config_set),
            replica_allowlist: METRICS_EXPORT_REPLICA_ALLOWLIST.get(config_set),
            max_samples_per_family: METRICS_EXPORT_MAX_SERIES_PER_FAMILY.get(config_set),
        }
    }
}

/// The parsed form of the export filter dyncfgs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ExportFilterConfig {
    disabled_families: Vec<FamilyPattern>,
    cluster_allowlist: BTreeSet<String>,
    replica_allowlist: BTreeSet<String>,
    max_samples_per_family: usize,
}

impl ExportFilterConfig {
    fn parse(raw: &RawConfig) -> Self {
        Self::new(
            &raw.disabled_families,
            &raw.cluster_allowlist,
            &raw.replica_allowlist,
            raw.max_samples_per_family,
        )
    }

    fn new(
        disabled_families: &str,
        cluster_allowlist: &str,
        replica_allowlist: &str,
        max_samples_per_family: usize,
    ) -> Self {
        ExportFilterConfig {
            disabled_families: parse_list(disabled_families)
                .map(FamilyPattern::parse)
                .collect(),
            cluster_allowlist: parse_list(cluster_allowlist).map(str::to_owned).collect(),
            replica_allowlist: parse_list(replica_allowlist).map(str::to_owned).collect(),
            max_samples_per_family,
        }
    }

    fn family_disabled(&self, name: &str) -> bool {
        self.disabled_families.iter().any(|p| p.matches(name))
    }

    /// Returns why a series with these labels is excluded, if it is.
    ///
    /// A series is excluded when it carries a scoping label with a non-empty
    /// value that is not in the corresponding non-empty allowlist. Series
    /// without the label, or with an empty value, are kept.
    fn series_drop_reason(&self, labels: &[LabelPair]) -> Option<DropReason> {
        let excluded = |allowlist: &BTreeSet<String>, names: &[&str]| {
            !allowlist.is_empty()
                && labels.iter().any(|l| {
                    names.contains(&l.name())
                        && !l.value().is_empty()
                        && !allowlist.contains(l.value())
                })
        };
        if excluded(&self.cluster_allowlist, CLUSTER_LABELS) {
            Some(DropReason::ClusterAllowlist)
        } else if excluded(&self.replica_allowlist, REPLICA_LABELS) {
            Some(DropReason::ReplicaAllowlist)
        } else {
            None
        }
    }
}

fn parse_list(s: &str) -> impl Iterator<Item = &str> {
    s.split(',').map(str::trim).filter(|s| !s.is_empty())
}

/// The number of samples the text encoder emits for one series.
///
/// A histogram expands to one line per bucket plus the implicit `+Inf`
/// bucket, `_count` and `_sum`. A summary expands to one line per quantile
/// plus `_count` and `_sum`. Everything else is one line. Scraper limits are
/// on samples, not label sets, so this is the unit the cap and the series
/// gauge use.
fn exported_samples(series: &Metric) -> usize {
    if let Some(histogram) = series.histogram.as_ref() {
        histogram.bucket.len() + 3
    } else if let Some(summary) = series.summary.as_ref() {
        summary.quantile.len() + 2
    } else {
        1
    }
}

/// Metrics describing what the filter exported and dropped.
#[derive(Debug)]
struct ExportFilterMetrics {
    samples: UIntGaugeVec,
    dropped_samples: IntCounterVec,
    encoded_bytes: UIntGauge,
    /// Families the `samples` gauge currently has a child for, so children of
    /// families that stop being exported can be removed. Updating children in
    /// place rather than resetting the gauge keeps a concurrent gather from
    /// observing an empty gauge.
    reported_families: Mutex<BTreeSet<String>>,
}

impl ExportFilterMetrics {
    fn register_into(registry: &MetricsRegistry) -> Self {
        ExportFilterMetrics {
            samples: registry.register(metric!(
                name: "mz_metrics_export_series",
                help: "Number of samples exported per metric family at the previous gather, after filtering.",
                var_labels: ["family"],
            )),
            dropped_samples: registry.register(metric!(
                name: "mz_metrics_export_dropped_series_total",
                help: "Samples removed from the exported metrics by the export filter.",
                var_labels: ["family", "reason"],
            )),
            encoded_bytes: registry.register(metric!(
                name: "mz_metrics_export_encoded_bytes",
                help: "Size in bytes of the most recently encoded internal metrics response.",
            )),
            reported_families: Mutex::new(BTreeSet::new()),
        }
    }

    fn record_dropped(&self, family: &str, reason: DropReason, samples: usize) {
        if samples > 0 {
            self.dropped_samples
                .with_label_values(&[family, reason.label()])
                .inc_by(u64::cast_from(samples));
        }
    }

    /// Sets the per-family sample gauge to `exported` and removes children for
    /// families no longer exported.
    fn record_exported(&self, exported: &[(&str, usize)]) {
        let mut reported = self.reported_families.lock().expect("lock poisoned");
        let mut still_reported = BTreeSet::new();
        for (family, samples) in exported {
            self.samples
                .with_label_values(&[family])
                .set(u64::cast_from(*samples));
            still_reported.insert((*family).to_owned());
        }
        for stale in reported.difference(&still_reported) {
            // The child may already be gone if the gauge was reset elsewhere.
            let _ = self.samples.remove_label_values(&[stale]);
        }
        *reported = still_reported;
    }
}

/// Applies `config` to gathered `families` in place, recording the outcome in
/// `metrics`.
///
/// Postconditions on every retained family not named with
/// [`SELF_METRIC_PREFIX`]: it matches no disabled pattern, none of its series
/// carries a scoping label value outside a non-empty allowlist, its sample
/// count does not exceed the cap when the cap is non-zero, and it is not
/// empty.
fn apply(
    config: &ExportFilterConfig,
    families: &mut Vec<MetricFamily>,
    metrics: &ExportFilterMetrics,
) {
    families.retain_mut(|family| {
        // Taken out so the series can be filtered while `name` borrows the
        // family; put back only when the family is retained.
        let mut series = std::mem::take(&mut family.metric);
        let name = family.name();
        if name.starts_with(SELF_METRIC_PREFIX) {
            family.metric = series;
            return true;
        }
        if config.family_disabled(name) {
            let samples = series.iter().map(exported_samples).sum();
            metrics.record_dropped(name, DropReason::DisabledFamily, samples);
            return false;
        }
        let mut dropped_cluster = 0;
        let mut dropped_replica = 0;
        let mut kept = 0;
        series.retain(|series| match config.series_drop_reason(&series.label) {
            None => {
                kept += exported_samples(series);
                true
            }
            Some(DropReason::ClusterAllowlist) => {
                dropped_cluster += exported_samples(series);
                false
            }
            Some(_) => {
                dropped_replica += exported_samples(series);
                false
            }
        });
        metrics.record_dropped(name, DropReason::ClusterAllowlist, dropped_cluster);
        metrics.record_dropped(name, DropReason::ReplicaAllowlist, dropped_replica);
        if kept == 0 {
            return false;
        }
        if config.max_samples_per_family > 0 && kept > config.max_samples_per_family {
            metrics.record_dropped(name, DropReason::OverCap, kept);
            return false;
        }
        family.metric = series;
        true
    });
    let exported: Vec<(&str, usize)> = families
        .iter()
        .filter(|family| !family.name().starts_with(SELF_METRIC_PREFIX))
        .map(|family| {
            (
                family.name(),
                family.metric.iter().map(exported_samples).sum(),
            )
        })
        .collect();
    metrics.record_exported(&exported);
}

/// The filter's state, shared between the registry postprocessor and the
/// [`ExportFilter`] handles.
#[derive(Debug)]
struct Inner {
    dyncfgs: Arc<ConfigSet>,
    /// The last configuration read from `dyncfgs`, re-parsed only when the raw
    /// values change.
    cached: Mutex<(RawConfig, Arc<ExportFilterConfig>)>,
    metrics: ExportFilterMetrics,
}

impl Inner {
    /// Returns the current configuration, re-parsing it if the dyncfgs changed
    /// since the previous gather.
    fn config(&self) -> Arc<ExportFilterConfig> {
        let raw = RawConfig::read(&self.dyncfgs);
        let mut cached = self.cached.lock().expect("lock poisoned");
        if cached.0 != raw {
            let parsed = Arc::new(ExportFilterConfig::parse(&raw));
            tracing::info!(?parsed, "metrics export filter updated");
            *cached = (raw, parsed);
        }
        Arc::clone(&cached.1)
    }
}

/// A handle to the export filter installed on a [`MetricsRegistry`].
#[derive(Debug, Clone)]
pub struct ExportFilter {
    inner: Arc<Inner>,
}

impl ExportFilter {
    /// Installs the filter as a postprocessor on `registry`.
    ///
    /// `dyncfgs` must contain the `metrics_export_*` configs (see
    /// [`crate::all_dyncfgs`]) and is read at every gather, so it must be the
    /// set that receives live configuration updates. Install at most once per
    /// registry.
    pub fn install(registry: &MetricsRegistry, dyncfgs: Arc<ConfigSet>) -> Self {
        let raw = RawConfig::read(&dyncfgs);
        let parsed = Arc::new(ExportFilterConfig::parse(&raw));
        let inner = Arc::new(Inner {
            dyncfgs,
            cached: Mutex::new((raw, parsed)),
            metrics: ExportFilterMetrics::register_into(registry),
        });
        registry.register_postprocessor({
            let inner = Arc::clone(&inner);
            move |families| {
                let config = inner.config();
                apply(&config, families, &inner.metrics);
            }
        });
        ExportFilter { inner }
    }

    /// Records the encoded size of a metrics response in
    /// `mz_metrics_export_encoded_bytes`.
    pub fn record_encoded_bytes(&self, bytes: usize) {
        self.inner.metrics.encoded_bytes.set(u64::cast_from(bytes));
    }
}

#[cfg(test)]
mod tests {
    use mz_dyncfg::{ConfigUpdates, ConfigVal};
    use mz_ore::metrics::IntGauge;
    use mz_ore::metrics::raw::{HistogramVec, IntGaugeVec as RawIntGaugeVec};
    use prometheus::core::Collector;

    use super::*;

    fn registry_with_series() -> (MetricsRegistry, ExportFilterMetrics) {
        let registry = MetricsRegistry::new();
        let per_replica: RawIntGaugeVec = registry.register(metric!(
            name: "test_per_replica",
            help: "per replica",
            var_labels: ["instance_id", "replica_id"],
        ));
        per_replica.with_label_values(&["u1", "u10"]).set(1);
        per_replica.with_label_values(&["u1", "u11"]).set(1);
        per_replica.with_label_values(&["u2", "u20"]).set(1);
        let per_cluster: RawIntGaugeVec = registry.register(metric!(
            name: "test_per_cluster",
            help: "per cluster",
            var_labels: ["cluster_id"],
        ));
        per_cluster.with_label_values(&["u1"]).set(1);
        per_cluster.with_label_values(&["u2"]).set(1);
        per_cluster.with_label_values(&[""]).set(1);
        let plain: IntGauge = registry.register(metric!(
            name: "test_plain",
            help: "no labels",
        ));
        plain.set(1);
        let other: IntGauge = registry.register(metric!(
            name: "other_plain",
            help: "no labels",
        ));
        other.set(1);
        // Register the filter's metrics into a separate registry so they do
        // not appear in the gathered output under test.
        let metrics = ExportFilterMetrics::register_into(&MetricsRegistry::new());
        (registry, metrics)
    }

    fn names_and_counts(families: &[MetricFamily]) -> Vec<(&str, usize)> {
        families
            .iter()
            .map(|f| (f.name(), f.metric.len()))
            .collect()
    }

    fn dropped(metrics: &ExportFilterMetrics, family: &str, reason: &str) -> u64 {
        metrics
            .dropped_samples
            .with_label_values(&[family, reason])
            .get()
    }

    fn gauge_families(metrics: &ExportFilterMetrics) -> Vec<String> {
        metrics
            .samples
            .collect()
            .into_iter()
            .flat_map(|f| f.metric.into_iter())
            .map(|m| m.label[0].value().to_owned())
            .collect()
    }

    #[mz_ore::test]
    fn parse_config() {
        let config = ExportFilterConfig::new(" a, b* ,,", "u1 , u2", "", 5);
        assert_eq!(
            config.disabled_families,
            vec![
                FamilyPattern::Exact("a".into()),
                FamilyPattern::Prefix("b".into())
            ]
        );
        assert_eq!(
            config.cluster_allowlist,
            ["u1", "u2"].into_iter().map(String::from).collect()
        );
        assert!(config.replica_allowlist.is_empty());
        assert_eq!(config.max_samples_per_family, 5);
        assert_eq!(
            ExportFilterConfig::new("", "", "", 0),
            ExportFilterConfig::default()
        );
    }

    #[mz_ore::test]
    fn default_config_changes_nothing_but_still_counts() {
        let (registry, metrics) = registry_with_series();
        let before = registry.gather();
        let mut after = before.clone();
        apply(&ExportFilterConfig::default(), &mut after, &metrics);
        assert_eq!(names_and_counts(&before), names_and_counts(&after));
        assert_eq!(
            metrics
                .samples
                .with_label_values(&["test_per_replica"])
                .get(),
            3
        );
    }

    #[mz_ore::test]
    fn disabled_families_exact_and_prefix() {
        let (registry, metrics) = registry_with_series();
        let mut families = registry.gather();
        let config = ExportFilterConfig::new("test_plain,test_per_*", "", "", 0);
        apply(&config, &mut families, &metrics);
        assert_eq!(names_and_counts(&families), vec![("other_plain", 1)]);
        assert_eq!(dropped(&metrics, "test_per_replica", "disabled_family"), 3);
        assert_eq!(dropped(&metrics, "test_per_cluster", "disabled_family"), 3);
        assert_eq!(dropped(&metrics, "test_plain", "disabled_family"), 1);
    }

    #[mz_ore::test]
    fn cluster_allowlist_scopes_both_label_names_and_keeps_empty_values() {
        let (registry, metrics) = registry_with_series();
        let mut families = registry.gather();
        let config = ExportFilterConfig::new("", "u2", "", 0);
        apply(&config, &mut families, &metrics);
        assert_eq!(
            names_and_counts(&families),
            vec![
                ("other_plain", 1),
                ("test_per_cluster", 2),
                ("test_per_replica", 1),
                ("test_plain", 1),
            ]
        );
        for series in &families[1].metric {
            assert!(series.label[0].value() == "u2" || series.label[0].value().is_empty());
        }
        assert_eq!(
            dropped(&metrics, "test_per_replica", "cluster_allowlist"),
            2
        );
        assert_eq!(
            dropped(&metrics, "test_per_cluster", "cluster_allowlist"),
            1
        );
    }

    #[mz_ore::test]
    fn replica_allowlist_and_empty_family_removal() {
        let (registry, metrics) = registry_with_series();
        let mut families = registry.gather();
        let config = ExportFilterConfig::new("", "", "u99", 0);
        apply(&config, &mut families, &metrics);
        assert_eq!(
            names_and_counts(&families),
            vec![
                ("other_plain", 1),
                ("test_per_cluster", 3),
                ("test_plain", 1)
            ]
        );
        assert_eq!(
            dropped(&metrics, "test_per_replica", "replica_allowlist"),
            3
        );
    }

    #[mz_ore::test]
    fn cap_drops_whole_family_after_scoping() {
        let (registry, metrics) = registry_with_series();
        let mut families = registry.gather();
        let config = ExportFilterConfig::new("", "", "", 2);
        apply(&config, &mut families, &metrics);
        assert_eq!(
            names_and_counts(&families),
            vec![("other_plain", 1), ("test_plain", 1)]
        );
        assert_eq!(dropped(&metrics, "test_per_replica", "over_cap"), 3);
        assert_eq!(dropped(&metrics, "test_per_cluster", "over_cap"), 3);

        // Scoping runs before the cap, so a family that fits once scoped stays.
        let mut families = registry.gather();
        let config = ExportFilterConfig::new("", "u1", "", 2);
        apply(&config, &mut families, &metrics);
        assert_eq!(
            names_and_counts(&families),
            vec![
                ("other_plain", 1),
                ("test_per_cluster", 2),
                ("test_per_replica", 2),
                ("test_plain", 1),
            ]
        );
    }

    #[mz_ore::test]
    fn histograms_count_exported_samples() {
        let registry = MetricsRegistry::new();
        let histogram: HistogramVec = registry.register(metric!(
            name: "test_histogram",
            help: "histogram",
            var_labels: ["instance_id"],
            buckets: vec![1.0, 2.0],
        ));
        histogram.with_label_values(&["u1"]).observe(1.0);
        histogram.with_label_values(&["u2"]).observe(1.0);
        let metrics = ExportFilterMetrics::register_into(&MetricsRegistry::new());

        // Two series, each two buckets plus +Inf, _count and _sum.
        let mut families = registry.gather();
        apply(&ExportFilterConfig::default(), &mut families, &metrics);
        assert_eq!(
            metrics.samples.with_label_values(&["test_histogram"]).get(),
            10
        );

        // A cap between the label-set count and the sample count still drops it.
        let mut families = registry.gather();
        apply(
            &ExportFilterConfig::new("", "", "", 5),
            &mut families,
            &metrics,
        );
        assert!(families.is_empty());
        assert_eq!(dropped(&metrics, "test_histogram", "over_cap"), 10);

        let mut families = registry.gather();
        apply(
            &ExportFilterConfig::new("", "u1", "", 5),
            &mut families,
            &metrics,
        );
        assert_eq!(names_and_counts(&families), vec![("test_histogram", 1)]);
        assert_eq!(dropped(&metrics, "test_histogram", "cluster_allowlist"), 5);
    }

    #[mz_ore::test]
    fn series_gauge_tracks_retained_families_only() {
        let (registry, metrics) = registry_with_series();
        let mut families = registry.gather();
        apply(
            &ExportFilterConfig::new("", "", "", 0),
            &mut families,
            &metrics,
        );
        assert_eq!(
            metrics
                .samples
                .with_label_values(&["test_per_replica"])
                .get(),
            3
        );

        let mut families = registry.gather();
        apply(
            &ExportFilterConfig::new("test_per_replica", "", "", 0),
            &mut families,
            &metrics,
        );
        let reported = gauge_families(&metrics);
        assert!(!reported.contains(&"test_per_replica".to_owned()));
        assert!(reported.contains(&"test_plain".to_owned()));
    }

    #[mz_ore::test]
    fn self_metrics_are_exempt() {
        let (registry, metrics) = registry_with_series();
        // Register the filter's own families into the registry under test and
        // give one of them more series than the cap allows.
        let own = ExportFilterMetrics::register_into(&registry);
        own.samples.with_label_values(&["a"]).set(1);
        own.samples.with_label_values(&["b"]).set(1);
        let mut families = registry.gather();
        let config = ExportFilterConfig::new("mz_*", "", "", 1);
        apply(&config, &mut families, &metrics);
        assert_eq!(
            names_and_counts(&families),
            vec![
                ("mz_metrics_export_encoded_bytes", 1),
                ("mz_metrics_export_series", 2),
                ("other_plain", 1),
                ("test_plain", 1),
            ]
        );
        assert!(!gauge_families(&metrics).contains(&"mz_metrics_export_series".to_owned()));
    }

    #[mz_ore::test]
    fn installed_filter_follows_live_dyncfgs() {
        let registry = MetricsRegistry::new();
        let gauge: RawIntGaugeVec = registry.register(metric!(
            name: "test_gauge",
            help: "test",
            var_labels: ["instance_id"],
        ));
        gauge.with_label_values(&["u1"]).set(1);
        let dyncfgs = Arc::new(crate::all_dyncfgs(ConfigSet::default()));
        let filter = ExportFilter::install(&registry, Arc::clone(&dyncfgs));
        assert!(registry.gather().iter().any(|f| f.name() == "test_gauge"));

        let mut updates = ConfigUpdates::default();
        updates.add_dynamic(
            "metrics_export_disabled_families",
            ConfigVal::String("test_gauge".into()),
        );
        updates.apply(&dyncfgs);
        assert!(!registry.gather().iter().any(|f| f.name() == "test_gauge"));

        let mut updates = ConfigUpdates::default();
        updates.add_dynamic(
            "metrics_export_disabled_families",
            ConfigVal::String("".into()),
        );
        updates.apply(&dyncfgs);
        assert!(registry.gather().iter().any(|f| f.name() == "test_gauge"));

        filter.record_encoded_bytes(42);
        let bytes = registry
            .gather()
            .into_iter()
            .find(|f| f.name() == "mz_metrics_export_encoded_bytes")
            .expect("registered");
        assert_eq!(bytes.metric[0].gauge.value(), 42.0);
    }
}
