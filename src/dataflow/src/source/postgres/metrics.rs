// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use mz_dataflow_types::postgres_source::PostgresSourceDetails;
use mz_expr::SourceInstanceId;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use prometheus::core::AtomicU64;

use crate::source::metrics::SourceBaseMetrics;

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug, Hash, Copy, Clone)]
pub enum PgOp {
    INSERT,
    UPDATE,
    DELETE,
}

impl From<&PgOp> for String {
    fn from(op: &PgOp) -> String {
        match op {
            PgOp::INSERT => String::from("INSERT"),
            PgOp::UPDATE => String::from("UPDATE"),
            PgOp::DELETE => String::from("DELETE"),
        }
    }
}
pub(super) struct PgSourceMetrics {
    /// Maps table_oid -> operation -> DeleteOnDropCounter with appropriate tag values
    table_ops: HashMap<u32, HashMap<PgOp, DeleteOnDropCounter<'static, AtomicU64, Vec<String>>>>,
    ignored: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    transactions: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    tables: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    lsn: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
}

impl PgSourceMetrics {
    pub(super) fn new(
        base_metrics: &SourceBaseMetrics,
        source_id: SourceInstanceId,
        pg_source: &PostgresSourceDetails,
    ) -> Self {
        let pg_metrics = &base_metrics.postgres_source_specific;
        let source_label = &[source_id.to_string()];
        let ops = vec![PgOp::INSERT, PgOp::UPDATE, PgOp::DELETE];
        let table_metrics = HashMap::from_iter(pg_source.tables.iter().map(|table| {
            (
                table.relation_id,
                HashMap::from_iter(ops.iter().map(|o| {
                    let labels = &[
                        source_id.to_string(),
                        table.namespace.clone(),
                        table.name.clone(),
                        o.into(),
                    ];
                    (
                        *o,
                        pg_metrics
                            .dml_messages
                            .get_delete_on_drop_counter(labels.to_vec()),
                    )
                })),
            )
        }));

        Self {
            table_ops: table_metrics,
            tables: pg_metrics
                .tables_in_publication
                .get_delete_on_drop_gauge(source_label.to_vec()),
            lsn: pg_metrics
                .wal_lsn
                .get_delete_on_drop_gauge(source_label.to_vec()),
            ignored: pg_metrics
                .ignored_messages
                .get_delete_on_drop_counter(source_label.to_vec()),
            transactions: pg_metrics
                .transactions
                .get_delete_on_drop_counter(source_label.to_vec()),
        }
    }

    pub fn op(&self, table_id: u32, op: PgOp) {
        self.table_ops
            .get(&table_id)
            .and_then(|t| t.get(&op))
            .and_then(|ctr| Some(ctr.inc()));
    }

    pub fn lsn(&self, lsn: u64) {
        self.lsn.set(lsn);
    }

    pub fn tables(&self) {
        self.tables.inc();
    }

    pub fn transactions(&self) {
        self.transactions.inc();
    }

    pub fn ignored(&self) {
        self.ignored.inc();
    }
}
