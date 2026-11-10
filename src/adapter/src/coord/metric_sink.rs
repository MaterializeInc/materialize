// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Curated metric sinks, the curated counterpart to `CREATE METRIC SINK`.
//!
//! A curated metric sink is a [`CURATED`] entry rendered on every replica, publishing its series
//! into that replica's process-local Prometheus registry. Unlike a user's `CREATE METRIC SINK` it
//! is not a catalog item: it gets a transient [`mz_repr::GlobalId`], targets one replica rather than a
//! cluster. The writer durably selects its plan and the replica executes it independently of
//! adapter lifetime.
//! Keeping the curated set out of the SQL catalog means changing a definition needs no builtin
//! migration.
//!
//! Every replica means every replica of every cluster, user clusters included. Each definition is
//! therefore a dataflow, with its arrangements, on customer compute, charged to that customer's
//! cluster, and the cost scales with `CURATED`. `coord::introspection` already accepts this for its
//! subscribes.
//!
//! The `disabled_metric_sinks` system var denies definitions by name, withdrawing a denied sink's
//! selection rather than only gating future admission. Writer admission reconciles selections in
//! the config transaction.

use std::collections::BTreeSet;

use anyhow::bail;
use mz_catalog::memory::objects::CatalogItem;
use mz_ore::collections::CollectionExt;
use mz_repr::{CatalogItemId, RelationDesc};
use mz_sql::catalog::SessionCatalog;
use mz_sql::plan::{
    HirRelationExpr, METRIC_SINK_CURATED_PREFIX_MARKER, Params, Plan, SubscribeFrom, SubscribePlan,
    validate_metric_sink_desc,
};
use mz_sql::session::vars::SystemVars;
use tracing::warn;

use crate::catalog::CatalogState;

mod written;

/// A curated metric sink: SQL producing the canonical metric-sink columns, plus the name it is
/// known by in logs.
#[derive(Debug)]
struct CuratedMetricSink {
    /// Stable identifier for the definition: used in logs, in the durable replica plan owner,
    /// and as the `sink` label on the health gauges (the `GlobalId` is transient, the name is not).
    /// Must be unique within [`CURATED`].
    name: &'static str,
    /// A `SELECT` producing the canonical metric-sink columns (`metric_name`, `metric_type`,
    /// `labels`, `value`, `help`), the contract `mz_sql::plan::validate_metric_sink_desc` checks.
    ///
    /// The query must read only introspection relations. A catalog-backed relation would put
    /// envd's write frontier on the sink's emission path, which is exactly the coupling these
    /// sinks exist to avoid: the sink would stall whenever envd did, taking the freshness signal
    /// with it.
    source_sql: &'static str,
    /// Prepended to every row's `metric_name` to form the published name, exactly as a user's
    /// `CREATE METRIC SINK ... WITH (PREFIX = ...)`. Every definition in [`CURATED`] uses
    /// [`METRIC_SINK_CURATED_PREFIX_MARKER`], which user sinks are barred from, so nothing a user
    /// publishes can collide with a curated family.
    prefix: &'static str,
}

/// The curated metric sinks, installed on every replica.
///
/// Sources read the raw `..._raw` logging relations, not a derived view that re-aggregates them
/// (`mz_dataflow_arrangement_sizes`) or a join view over the logs (`mz_dataflow_operator_dataflows`),
/// for performance: those churn even on static data and their cost scales with the replica's
/// dataflow-creation rate. A single-relation filter like `mz_compute_exports` carries no aggregation
/// and is read freely.
///
/// Every family sums across workers, so a series carries no `worker_id`, and a multi-process replica
/// reports one number per grouping key rather than one per process. The size families emit one series
/// per dataflow, the errors family one per export.
const CURATED: &[CuratedMetricSink] = &[
    CuratedMetricSink {
        name: "mz_metric_arrangement_sizes",
        prefix: METRIC_SINK_CURATED_PREFIX_MARKER,
        // Key on the export id from `mz_compute_exports`, not `mz_dataflow_global_ids`: a
        // materialized view builds under a transient view id and appears only as an export, so a
        // global-id label names a `t<N>` that maps to no catalog object and churns on every
        // re-render.
        //
        // Logs are per operator, so map operator -> dataflow -> export id. Take the operator ->
        // dataflow step off `mz_dataflow_addresses_per_worker` (`address[1]` is the dataflow id),
        // which avoids the multi-way join behind `mz_dataflow_operator_dataflows`.
        //
        // `min(export_id)` collapses a multi-export dataflow to one series (lexicographic, so
        // `min('u10', 'u2')` is `'u10'`: arbitrary but stable). The group-size hint stops that `min`
        // from rendering the 8-level hierarchy, which would otherwise show up as tuning advice for
        // the sink's own dataflow in `mz_expected_group_size_advice`.
        //
        // NOTE: counting an arrangement this sink reads is a feedback loop. Every change to it
        // changes its logged size, the sink reads that on the next logging tick, and the dataflow
        // re-runs every tick for the replica's lifetime (SQL-730). `NOT LIKE 't%'` keeps out the
        // sink's own dataflow and the replica's introspection subscribes. `NOT LIKE 'si%'` keeps
        // out the logging dataflow: `si<N>` names only the introspection source indexes it
        // exports, and a plain system index prints `s<N>`. Only that filter excludes it under
        // `INTROSPECTION DEBUGGING`, which registers the loggers before the logging dataflow is
        // built, so its own operators get log rows. The cost is that transient dataflows'
        // arrangements go unreported, so these families sum below what the replica holds.
        //
        // `f` joins each raw log separately to reuse its `(operator_id, worker_id)` index
        // (`LogVariant::index_by`). One union would arrange all three logs' rows afresh.
        source_sql: "
WITH ex AS (
    SELECT dataflow_id, min(export_id) AS export_id
    FROM mz_introspection.mz_compute_exports
    WHERE export_id NOT LIKE 't%' AND export_id NOT LIKE 'si%'
    GROUP BY dataflow_id OPTIONS (AGGREGATE INPUT GROUP SIZE = 1)
),
oe AS (
    SELECT a.id, a.worker_id, ex.export_id
    FROM mz_introspection.mz_dataflow_addresses_per_worker a
    JOIN ex ON ex.dataflow_id = a.address[1]
),
f AS (
    SELECT 'arrangement_size_bytes'::text AS metric_name, oe.export_id
    FROM mz_introspection.mz_arrangement_heap_size_raw r
    JOIN oe ON r.operator_id = oe.id AND r.worker_id = oe.worker_id
    UNION ALL
    SELECT 'arrangement_records'::text, oe.export_id
    FROM mz_introspection.mz_arrangement_records_raw r
    JOIN oe ON r.operator_id = oe.id AND r.worker_id = oe.worker_id
    UNION ALL
    SELECT 'arrangement_batches'::text, oe.export_id
    FROM mz_introspection.mz_arrangement_batches_raw r
    JOIN oe ON r.operator_id = oe.id AND r.worker_id = oe.worker_id
)
SELECT metric_name, 'gauge'::text AS metric_type,
       map_build(LIST[ROW('id', export_id)])::map[text=>text] AS labels,
       count(*)::double precision AS value,
       CASE metric_name
           WHEN 'arrangement_size_bytes' THEN 'arrangement heap size in bytes'
           WHEN 'arrangement_records' THEN 'number of records in arrangement heaps'
           WHEN 'arrangement_batches' THEN 'number of batches in arrangements'
       END AS help
FROM f
GROUP BY metric_name, export_id",
    },
    CuratedMetricSink {
        name: "mz_metric_dataflow_errors",
        prefix: METRIC_SINK_CURATED_PREFIX_MARKER,
        // Raw log, not the `mz_compute_error_counts` view: the view joins the storage-managed
        // `mz_internal.mz_compute_dependencies`, which `ensure_reads_only_logs` rejects. `count` is
        // per-worker, so sum per export. `HAVING` drops the healthy ones.
        //
        // NOTE: direct errors only. The raw log attributes an error to the export that raised it. The
        // view also forwards counts onto index-reuse exports, so a broken reuse-index reads 0 here and
        // its errors show under the underlying export, undercounting against the view.
        source_sql: "
SELECT 'dataflow_error_count'::text AS metric_name, 'gauge'::text AS metric_type,
       map_build(LIST[ROW('id', export_id::text)])::map[text=>text] AS labels,
       sum(count)::double precision AS value, 'count of errors in the dataflow'::text AS help
FROM mz_introspection.mz_compute_error_counts_raw
GROUP BY export_id
HAVING sum(count) > 0",
    },
];

/// Warns about disabled names that match no curated definition.
///
/// Unknown names are inert. Selection changes belong to writer admission, not this diagnostic.
pub(super) fn warn_unknown_disabled_metric_sinks(system_config: &SystemVars) {
    for entry in system_config.disabled_metric_sinks() {
        if !CURATED.iter().any(|definition| definition.name == entry) {
            warn!(
                name = %entry,
                "disabled_metric_sinks entry matches no curated definition"
            );
        }
    }
}

/// Whether `disabled_metric_sinks` denies the curated definition called `name`.
///
/// An entry naming no definition is never asked about, so a stale or misspelled one is inert.
fn metric_sink_denied(system_config: &SystemVars, name: &str) -> bool {
    system_config
        .disabled_metric_sinks()
        .iter()
        .any(|denied| denied == name)
}

/// Enforces the introspection-only contract from [`CuratedMetricSink::source_sql`]: every relation
/// the definition reads, walking views transitively, must be a log collection.
///
/// Checked here against what the definition reads rather than by import kind after optimization: the
/// import split (storage vs index) depends on which indexes the target cluster happens to have, so
/// it gives the same definition different verdicts on different clusters.
fn ensure_reads_only_logs(
    catalog: &CatalogState,
    dependencies: &BTreeSet<CatalogItemId>,
) -> Result<(), anyhow::Error> {
    let mut to_visit: Vec<_> = dependencies.iter().copied().collect();
    let mut visited = BTreeSet::new();
    while let Some(id) = to_visit.pop() {
        if !visited.insert(id) {
            continue;
        }
        let entry = catalog.get_entry(&id);
        match entry.item() {
            // The only data leaf allowed.
            CatalogItem::Log(_) => {}
            // Allowed only if everything it reads is, so walk its dependencies.
            CatalogItem::View(_) => to_visit.extend(entry.uses()),
            // No data dependency; a view over logs still references these.
            CatalogItem::Type(_) | CatalogItem::Func(_) => {}
            _ => bail!(
                "curated metric sink reads {}, which is not an introspection log relation \
                 (only logs and views over logs are allowed)",
                catalog.resolve_full_name(entry.name(), None)
            ),
        }
    }
    Ok(())
}

impl CuratedMetricSink {
    /// Plans `source_sql` against a session-less catalog, returning the query, its output shape,
    /// and the catalog items it reads.
    fn plan_source(
        &self,
        catalog: &dyn SessionCatalog,
    ) -> Result<(HirRelationExpr, RelationDesc, BTreeSet<CatalogItemId>), anyhow::Error> {
        // A definition is a single statement. Reject the count explicitly for a clear error.
        let statements = mz_sql::parse::parse(self.source_sql)?;
        if statements.len() != 1 {
            bail!(
                "source SQL must be exactly one statement, got {}",
                statements.len()
            );
        }

        // A metric sink's source is a continuously maintained dataflow, like a SUBSCRIBE, so plan it
        // as one. A maintained lifetime folds any finishing into the expression (an ORDER BY over a
        // maintained collection is dropped, a LIMIT becomes a TopK) rather than leaving it beside the
        // query, so `MetricSinkFrom::Query` gets a self-contained expression whose arity matches its
        // `desc`. This mirrors `coord::introspection`, which plans its specs as subscribes too.
        let subscribe_sql = format!("SUBSCRIBE ({})", self.source_sql);
        let parsed = mz_sql::parse::parse(&subscribe_sql)?.into_element();
        let (stmt, resolved_ids) = mz_sql::names::resolve(catalog, parsed.ast)?;
        let (plan, sql_impl_ids) =
            mz_sql::plan::plan(None, catalog, stmt, &Params::empty(), &resolved_ids)?;
        let Plan::Subscribe(SubscribePlan {
            from: SubscribeFrom::Query { expr, desc },
            ..
        }) = plan
        else {
            bail!("source SQL must be a single SELECT");
        };
        validate_metric_sink_desc(&desc)?;

        // Fold in ids from SQL-implemented function bodies. `plan` keeps them out of `resolved_ids`
        // since a one-shot statement doesn't depend on a function's body, but a metric sink inlines
        // that body into its dataflow, so the body's reads are real imports the gate must check.
        let dependencies = resolved_ids
            .items()
            .chain(sql_impl_ids.items())
            .copied()
            .collect();
        Ok((expr, desc, dependencies))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use mz_catalog::memory::objects::CatalogItem;
    use mz_sql::plan::{
        METRIC_SINK_CURATED_PREFIX_MARKER, validate_metric_sink_prefix,
        validate_user_metric_sink_prefix,
    };
    use mz_sql::session::vars::{DISABLED_METRIC_SINKS, SystemVars, Var, VarInput};

    use crate::catalog::Catalog;
    use crate::coord::metric_sink::{
        CURATED, CuratedMetricSink, ensure_reads_only_logs, metric_sink_denied,
    };

    /// The var is a `Vec<Ident>`, so parsing follows the SQL identifier-list rules: surrounding
    /// whitespace is tolerated, an unquoted name folds to lowercase, and a quoted name keeps its
    /// case. Matching is otherwise exact (no prefix match).
    #[mz_ore::test]
    fn denylist_matches_names_leniently() {
        let denied = |list: &str, name: &str| {
            let mut vars = SystemVars::new();
            vars.set(DISABLED_METRIC_SINKS.name(), VarInput::Flat(list))
                .expect("valid denylist");
            metric_sink_denied(&vars, name)
        };

        assert!(!denied("", "a"));
        assert!(denied("a", "a"));
        assert!(denied("a,b", "b"));
        assert!(denied("  a , b  ", "a"));
        // An unknown name denies nothing but is carried without error.
        assert!(!denied("nope", "a"));
        assert!(denied("nope,a", "a"));
        // Exact match only: no prefix match.
        assert!(!denied("a", "ab"));
        assert!(!denied("ab", "a"));
        // Unquoted names fold to lowercase; quoting pins the case.
        assert!(denied("A", "a"));
        assert!(!denied("\"A\"", "a"));

        // An empty entry between commas is rejected by the identifier parser (unlike the old
        // naive split, which silently dropped it).
        let mut vars = SystemVars::new();
        assert!(
            vars.set(DISABLED_METRIC_SINKS.name(), VarInput::Flat("a,,b"))
                .is_err()
        );
    }

    #[mz_ore::test]
    fn curated_prefixes_are_valid() {
        for definition in CURATED {
            validate_metric_sink_prefix(definition.prefix).unwrap_or_else(|err| {
                panic!(
                    "curated metric sink {:?} has an invalid prefix {:?}: {err}",
                    definition.name, definition.prefix
                )
            });
        }
    }

    #[mz_ore::test]
    fn curated_prefixes_are_reserved_against_user_sinks() {
        for definition in CURATED {
            assert!(
                definition
                    .prefix
                    .starts_with(METRIC_SINK_CURATED_PREFIX_MARKER),
                "curated metric sink {:?} does not use the reserved curated prefix: {:?}",
                definition.name,
                definition.prefix
            );
            assert!(
                validate_user_metric_sink_prefix(definition.prefix).is_err(),
                "a user could claim curated metric sink {:?}'s prefix {:?}",
                definition.name,
                definition.prefix
            );
        }
    }

    /// Admission identifies definitions by name, and collectors use it as the `sink` label.
    /// A duplicate would conflate selections and collide in the registry.
    #[mz_ore::test]
    fn curated_names_are_unique() {
        let mut seen = BTreeSet::new();
        for definition in CURATED {
            assert!(
                seen.insert(definition.name),
                "duplicate curated metric sink name {:?}",
                definition.name
            );
        }
    }

    /// Admission requires valid source SQL and log-only dependencies. A source can plan cleanly
    /// yet still read a storage-backed relation transitively (a builtin view joining in an
    /// introspection source), which only `ensure_reads_only_logs` catches.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn curated_definitions_plan() {
        Catalog::with_debug(|catalog| async move {
            let session_catalog = catalog.for_system_session();
            for definition in CURATED {
                let (_, _, dependencies) =
                    definition
                        .plan_source(&session_catalog)
                        .unwrap_or_else(|err| {
                            panic!(
                                "curated metric sink {:?} does not plan: {err}",
                                definition.name
                            )
                        });
                ensure_reads_only_logs(catalog.state(), &dependencies).unwrap_or_else(|err| {
                    panic!(
                        "curated metric sink {:?} reads a non-introspection relation: {err}",
                        definition.name
                    )
                });
            }
        })
        .await
    }

    /// The five canonical columns, no finishing: the shape a definition must produce.
    const VALID_SOURCE: &str = "SELECT 'n'::text AS metric_name, 'gauge'::text AS metric_type, \
        NULL::map[text=>text] AS labels, NULL::double AS value, 'h'::text AS help";

    /// `VALID_SOURCE` with an ORDER BY appended. Maintained-lifetime planning folds it away rather
    /// than rejecting it, since ordering has no meaning for a continuously-consumed collection.
    const ORDERED_SOURCE: &str = "SELECT 'n'::text AS metric_name, 'gauge'::text AS metric_type, \
        NULL::map[text=>text] AS labels, NULL::double AS value, 'h'::text AS help ORDER BY 1";

    /// `plan_source` accepts the canonical column contract (including a source with a finishing,
    /// which maintained-lifetime planning folds in) and rejects a source missing the columns.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn plan_source_enforces_the_metric_sink_contract() {
        Catalog::with_debug(|catalog| async move {
            let session_catalog = catalog.for_system_session();
            let plan = |source_sql: &'static str| {
                CuratedMetricSink {
                    name: "test",
                    source_sql,
                    prefix: "mz_metric_sink_test_",
                }
                .plan_source(&session_catalog)
            };

            assert!(plan(VALID_SOURCE).is_ok());

            // An ORDER BY is folded away by maintained-lifetime planning, not rejected.
            assert!(plan(ORDERED_SOURCE).is_ok());

            // Missing the canonical columns: rejected by `validate_metric_sink_desc`.
            assert!(plan("SELECT 1 AS foo").is_err());

            // Not exactly one statement: rejected by the explicit count guard.
            assert!(plan("").is_err());
            assert!(plan("SELECT 1; SELECT 2").is_err());
        })
        .await
    }

    /// A SQL-implemented builtin hides its reads: `pg_get_viewdef`'s body reads
    /// `mz_catalog.mz_views`, which the dataflow imports but the statement's resolved ids omit.
    /// `plan_source` must surface those reads so the gate rejects them.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn ensure_reads_only_logs_sees_sql_impl_function_reads() {
        Catalog::with_debug(|catalog| async move {
            let session_catalog = catalog.for_system_session();
            let (_, _, dependencies) = CuratedMetricSink {
                name: "test",
                source_sql: "SELECT pg_get_viewdef('x') AS metric_name, 'gauge'::text AS metric_type, \
                    NULL::map[text=>text] AS labels, NULL::double AS value, 'h'::text AS help",
                prefix: "mz_metric_sink_test_",
            }
            .plan_source(&session_catalog)
            .expect("plans against the system catalog");
            assert!(ensure_reads_only_logs(catalog.state(), &dependencies).is_err());
        })
        .await
    }

    /// The introspection-only contract: a log dependency is accepted, a storage-backed one is
    /// rejected. Checked against what the definition reads, so the verdict does not depend on the
    /// target cluster's index layout.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn ensure_reads_only_logs_accepts_logs_rejects_storage() {
        Catalog::with_debug(|catalog| async move {
            let log_id = catalog
                .entries()
                .find(|e| matches!(e.item(), CatalogItem::Log(_)))
                .expect("debug catalog has a builtin log")
                .id();
            assert!(ensure_reads_only_logs(catalog.state(), &BTreeSet::from([log_id])).is_ok());

            let storage_id = catalog
                .entries()
                .find(|e| matches!(e.item(), CatalogItem::Table(_) | CatalogItem::Source(_)))
                .expect("debug catalog has a builtin table or source")
                .id();
            assert!(
                ensure_reads_only_logs(catalog.state(), &BTreeSet::from([storage_id])).is_err()
            );
        })
        .await
    }
}
