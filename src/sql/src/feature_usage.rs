// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Extraction of the SQL features a statement uses, for usage telemetry.
//!
//! A [`Feature`] is a `category:name` tag drawn from a closed vocabulary:
//! statement kinds, builtin function and operator names, syntax constructs,
//! source and sink types, formats, envelopes, and option names. No tag ever
//! carries an identifier or literal from the statement, so a [`FeatureSet`] is
//! safe to export from an environment without redaction. Function and operator
//! names are only emitted when they match a builtin, which keeps a misspelled or
//! unknown name from leaking.
//!
//! The rendered tags are a contract with downstream analytics, which query
//! them by string across releases. Renaming a tag breaks those queries, so the
//! tests in this module pin the rendering.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt;

use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit::{self, Visit};
use mz_sql_parser::ast::*;

use crate::func::{
    INFORMATION_SCHEMA_BUILTINS, MZ_CATALOG_BUILTINS, MZ_INTERNAL_BUILTINS, MZ_UNSAFE_BUILTINS,
    OP_IMPLS, PG_CATALOG_BUILTINS,
};

/// The category of a [`Feature`], rendered as the tag's prefix. Variant order
/// is the order categories appear in a rendered [`FeatureSet`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FeatureCategory {
    Statement,
    Source,
    Sink,
    Format,
    Envelope,
    Function,
    Operator,
    Syntax,
    Option,
}

impl FeatureCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeatureCategory::Statement => "stmt",
            FeatureCategory::Function => "fn",
            FeatureCategory::Operator => "op",
            FeatureCategory::Syntax => "syntax",
            FeatureCategory::Source => "source",
            FeatureCategory::Sink => "sink",
            FeatureCategory::Format => "format",
            FeatureCategory::Envelope => "envelope",
            FeatureCategory::Option => "option",
        }
    }
}

/// A SQL feature used by a statement. Renders as `category:name`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Feature {
    pub category: FeatureCategory,
    pub name: Cow<'static, str>,
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.category.as_str(), self.name)
    }
}

/// The distinct features used by a statement, in a canonical order.
pub type FeatureSet = BTreeSet<Feature>;

/// Returns the features `stmt` uses.
pub fn statement_features(stmt: &Statement<Raw>) -> FeatureSet {
    let mut collector = FeatureCollector::default();
    collector.add(
        FeatureCategory::Statement,
        statement_kind_label_value(StatementKind::from(stmt)),
    );
    collector.visit_statement(stmt);
    collector.features
}

#[derive(Default)]
struct FeatureCollector {
    features: FeatureSet,
}

impl FeatureCollector {
    fn add(&mut self, category: FeatureCategory, name: impl Into<Cow<'static, str>>) {
        self.features.insert(Feature {
            category,
            name: name.into(),
        });
    }

    fn syntax(&mut self, name: &'static str) {
        self.add(FeatureCategory::Syntax, name);
    }

    /// Records an option name. `context` disambiguates option names shared
    /// between statements, e.g. `option:materialized_view:retain_history`.
    ///
    /// Callers must only pass option name enums whose rendering is a fixed
    /// keyword. An enum with a variant that renders user input would leak it.
    fn option<N: AstDisplay>(&mut self, context: &'static str, name: &N) {
        let name = name.to_ast_string_simple().to_lowercase().replace(' ', "_");
        self.add(FeatureCategory::Option, format!("{context}:{name}"));
    }

    fn format(&mut self, format: &Format<Raw>) {
        let name = match format {
            Format::Bytes => "bytes",
            Format::Avro(_) => "avro",
            Format::Protobuf(_) => "protobuf",
            Format::Regex(_) => "regex",
            Format::Csv { .. } => "csv",
            Format::Json { .. } => "json",
            Format::Text => "text",
        };
        self.add(FeatureCategory::Format, name);
    }

    fn format_specifier(&mut self, format: &Option<FormatSpecifier<Raw>>) {
        match format {
            None => {}
            Some(FormatSpecifier::Bare(format)) => self.format(format),
            Some(FormatSpecifier::KeyValue { key, value }) => {
                self.syntax("key_value_format");
                self.format(key);
                self.format(value);
            }
        }
    }
}

/// Returns the registry key of the builtin function `name` refers to, if any.
///
/// Lookup is by unqualified name across all builtin schemas. Users cannot
/// create functions, so every resolvable function is a builtin, and an
/// unqualified match on a name that only exists in a schema off the search
/// path still names a real builtin.
fn builtin_function_name(name: &RawItemName) -> Option<&'static str> {
    let name = name.name().0.last()?.as_str();
    [
        &*PG_CATALOG_BUILTINS,
        &*MZ_CATALOG_BUILTINS,
        &*MZ_INTERNAL_BUILTINS,
        &*INFORMATION_SCHEMA_BUILTINS,
        &*MZ_UNSAFE_BUILTINS,
    ]
    .into_iter()
    .find_map(|builtins| builtins.get_key_value(name).map(|(k, _)| *k))
}

macro_rules! visit_option_names {
    ($($method:ident: $ty:ty => $context:literal,)*) => {
        $(
            fn $method(&mut self, node: &'ast $ty) {
                self.option($context, node);
            }
        )*
    };
}

impl<'ast> Visit<'ast, Raw> for FeatureCollector {
    fn visit_function(&mut self, node: &'ast Function<Raw>) {
        if let Some(name) = builtin_function_name(&node.name) {
            self.add(FeatureCategory::Function, name);
        }
        if node.over.is_some() {
            self.syntax("window_function");
        }
        if node.filter.is_some() {
            self.syntax("aggregate_filter");
        }
        if node.distinct {
            self.syntax("aggregate_distinct");
        }
        if let FunctionArgs::Args { order_by, .. } = &node.args {
            if !order_by.is_empty() {
                self.syntax("aggregate_order_by");
            }
        }
        visit::visit_function(self, node);
    }

    fn visit_expr(&mut self, node: &'ast Expr<Raw>) {
        match node {
            Expr::Op { op, .. } => {
                if let Some((name, _)) = OP_IMPLS.get_key_value(op.op.as_str()) {
                    self.add(FeatureCategory::Operator, *name);
                }
            }
            Expr::HomogenizingFunction { function, .. } => {
                let name = match function {
                    HomogenizingFunction::Coalesce => "coalesce",
                    HomogenizingFunction::Greatest => "greatest",
                    HomogenizingFunction::Least => "least",
                };
                self.add(FeatureCategory::Function, name);
            }
            Expr::NullIf { .. } => self.add(FeatureCategory::Function, "nullif"),
            Expr::Case { .. } => self.syntax("case"),
            Expr::Exists(_) => self.syntax("exists_subquery"),
            Expr::InSubquery { .. } => self.syntax("in_subquery"),
            Expr::AnySubquery { .. } | Expr::AllSubquery { .. } => self.syntax("any_all_subquery"),
            Expr::Subquery(_) => self.syntax("scalar_subquery"),
            Expr::ArraySubquery(_) | Expr::ListSubquery(_) | Expr::MapSubquery(_) => {
                self.syntax("collection_subquery")
            }
            _ => {}
        }
        visit::visit_expr(self, node);
    }

    fn visit_query(&mut self, node: &'ast Query<Raw>) {
        if let CteBlock::MutuallyRecursive(_) = &node.ctes {
            self.syntax("with_mutually_recursive");
        } else if !node.ctes.is_empty() {
            self.syntax("cte");
        }
        if !node.order_by.is_empty() {
            self.syntax("order_by");
        }
        if node.limit.is_some() {
            self.syntax("limit");
        }
        if node.offset.is_some() {
            self.syntax("offset");
        }
        visit::visit_query(self, node);
    }

    fn visit_select(&mut self, node: &'ast Select<Raw>) {
        match &node.distinct {
            None => {}
            Some(Distinct::EntireRow) => self.syntax("distinct"),
            Some(Distinct::On(_)) => self.syntax("distinct_on"),
        }
        if !node.group_by.is_empty() {
            self.syntax("group_by");
        }
        if node.having.is_some() {
            self.syntax("having");
        }
        if node.qualify.is_some() {
            self.syntax("qualify");
        }
        visit::visit_select(self, node);
    }

    fn visit_set_expr(&mut self, node: &'ast SetExpr<Raw>) {
        if let SetExpr::SetOperation { op, all, .. } = node {
            let name = match (op, all) {
                (SetOperator::Union, false) => "union",
                (SetOperator::Union, true) => "union_all",
                (SetOperator::Except, false) => "except",
                (SetOperator::Except, true) => "except_all",
                (SetOperator::Intersect, false) => "intersect",
                (SetOperator::Intersect, true) => "intersect_all",
            };
            self.syntax(name);
        }
        visit::visit_set_expr(self, node);
    }

    fn visit_table_factor(&mut self, node: &'ast TableFactor<Raw>) {
        match node {
            TableFactor::Derived { lateral: true, .. } => self.syntax("lateral"),
            TableFactor::Function {
                with_ordinality: true,
                ..
            }
            | TableFactor::RowsFrom {
                with_ordinality: true,
                ..
            } => self.syntax("with_ordinality"),
            _ => {}
        }
        if let TableFactor::RowsFrom { .. } = node {
            self.syntax("rows_from");
        }
        visit::visit_table_factor(self, node);
    }

    fn visit_join(&mut self, node: &'ast Join<Raw>) {
        let (name, constraint) = match &node.join_operator {
            JoinOperator::Inner(c) => ("inner_join", Some(c)),
            JoinOperator::LeftOuter(c) => ("left_join", Some(c)),
            JoinOperator::RightOuter(c) => ("right_join", Some(c)),
            JoinOperator::FullOuter(c) => ("full_join", Some(c)),
            JoinOperator::CrossJoin => ("cross_join", None),
        };
        self.syntax(name);
        match constraint {
            Some(JoinConstraint::Using { .. }) => self.syntax("join_using"),
            Some(JoinConstraint::Natural) => self.syntax("natural_join"),
            Some(JoinConstraint::On(_)) | None => {}
        }
        visit::visit_join(self, node);
    }

    fn visit_as_of(&mut self, node: &'ast AsOf<Raw>) {
        match node {
            AsOf::At(_) => self.syntax("as_of"),
            AsOf::AtLeast(_) => self.syntax("as_of_at_least"),
        }
        visit::visit_as_of(self, node);
    }

    fn visit_subscribe_statement(&mut self, node: &'ast SubscribeStatement<Raw>) {
        if node.up_to.is_some() {
            self.syntax("subscribe_up_to");
        }
        let output = match &node.output {
            SubscribeOutput::Diffs => None,
            SubscribeOutput::WithinTimestampOrderBy { .. } => {
                Some("subscribe_within_timestamp_order_by")
            }
            SubscribeOutput::EnvelopeUpsert { .. } => Some("subscribe_envelope_upsert"),
            SubscribeOutput::EnvelopeDebezium { .. } => Some("subscribe_envelope_debezium"),
        };
        if let Some(output) = output {
            self.syntax(output);
        }
        visit::visit_subscribe_statement(self, node);
    }

    fn visit_create_source_statement(&mut self, node: &'ast CreateSourceStatement<Raw>) {
        let source = match &node.connection {
            CreateSourceConnection::Kafka { .. } => "kafka",
            CreateSourceConnection::Postgres { .. } => "postgres",
            CreateSourceConnection::SqlServer { .. } => "sql_server",
            CreateSourceConnection::MySql { .. } => "mysql",
            CreateSourceConnection::LoadGenerator { .. } => "load_generator",
        };
        self.add(FeatureCategory::Source, source);
        self.format_specifier(&node.format);
        if let Some(envelope) = &node.envelope {
            let envelope = match envelope {
                SourceEnvelope::None => "none",
                SourceEnvelope::Debezium => "debezium",
                SourceEnvelope::Upsert { .. } => "upsert",
                SourceEnvelope::CdcV2 => "materialize",
            };
            self.add(FeatureCategory::Envelope, envelope);
        }
        if !node.include_metadata.is_empty() {
            self.syntax("include_metadata");
        }
        visit::visit_create_source_statement(self, node);
    }

    fn visit_create_sink_statement(&mut self, node: &'ast CreateSinkStatement<Raw>) {
        let sink = match &node.connection {
            CreateSinkConnection::Kafka { .. } => "kafka",
            CreateSinkConnection::Iceberg { .. } => "iceberg",
        };
        self.add(FeatureCategory::Sink, sink);
        self.format_specifier(&node.format);
        if let Some(envelope) = &node.envelope {
            let envelope = match envelope {
                SinkEnvelope::Debezium => "debezium",
                SinkEnvelope::Upsert => "upsert",
            };
            self.add(FeatureCategory::Envelope, envelope);
        }
        visit::visit_create_sink_statement(self, node);
    }

    visit_option_names! {
        visit_select_option_name: SelectOptionName => "select",
        visit_mut_rec_block_option_name: MutRecBlockOptionName => "mutually_recursive",
        visit_materialized_view_option_name: MaterializedViewOptionName => "materialized_view",
        visit_create_source_option_name: CreateSourceOptionName => "source",
        visit_kafka_source_config_option_name: KafkaSourceConfigOptionName => "kafka_source",
        visit_pg_config_option_name: PgConfigOptionName => "postgres_source",
        visit_my_sql_config_option_name: MySqlConfigOptionName => "mysql_source",
        visit_sql_server_config_option_name: SqlServerConfigOptionName => "sql_server_source",
        visit_table_from_source_option_name: TableFromSourceOptionName => "table_from_source",
        visit_create_sink_option_name: CreateSinkOptionName => "sink",
        visit_kafka_sink_config_option_name: KafkaSinkConfigOptionName => "kafka_sink",
        visit_iceberg_sink_config_option_name: IcebergSinkConfigOptionName => "iceberg_sink",
        visit_index_option_name: IndexOptionName => "index",
        visit_cluster_option_name: ClusterOptionName => "cluster",
        visit_subscribe_option_name: SubscribeOptionName => "subscribe",
        visit_copy_option_name: CopyOptionName => "copy",
    }
}

#[cfg(test)]
mod tests {
    use mz_sql_parser::parser::parse_statements;

    use super::*;

    #[track_caller]
    fn features(sql: &str) -> Vec<String> {
        let stmts = parse_statements(sql).expect("valid sql");
        assert_eq!(stmts.len(), 1);
        statement_features(&stmts[0].ast)
            .iter()
            .map(|f| f.to_string())
            .collect()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn select_features() {
        assert_eq!(
            features(
                "SELECT DISTINCT a, row_number() OVER (PARTITION BY b ORDER BY c), \
                 coalesce(d, 1), j->>'k' \
                 FROM t LEFT JOIN u USING (id), LATERAL (SELECT * FROM v WHERE v.x = t.x LIMIT 1) \
                 WHERE EXISTS (SELECT 1) GROUP BY 1 \
                 OPTIONS (AGGREGATE INPUT GROUP SIZE = 10)"
            ),
            vec![
                "stmt:select",
                "fn:coalesce",
                "fn:row_number",
                "op:->>",
                "op:=",
                "syntax:distinct",
                "syntax:exists_subquery",
                "syntax:group_by",
                "syntax:join_using",
                "syntax:lateral",
                "syntax:left_join",
                "syntax:limit",
                "syntax:window_function",
                "option:select:aggregate_input_group_size",
            ]
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn unknown_names_do_not_leak() {
        assert_eq!(
            features("SELECT secret_customer_fn(x) FROM secret_table"),
            vec!["stmt:select"]
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn ddl_features() {
        assert_eq!(
            features(
                "CREATE MATERIALIZED VIEW mv WITH (RETAIN HISTORY FOR '1h') AS \
                 WITH MUTUALLY RECURSIVE r (x int) AS (SELECT 1 UNION ALL SELECT x FROM r) \
                 SELECT sum(x) FILTER (WHERE x > 0) FROM r"
            ),
            vec![
                "stmt:create_materialized_view",
                "fn:sum",
                "op:>",
                "syntax:aggregate_filter",
                "syntax:union_all",
                "syntax:with_mutually_recursive",
                "option:materialized_view:retain_history",
            ]
        );
        assert_eq!(
            features(
                "CREATE SOURCE s FROM KAFKA CONNECTION k (TOPIC 't') \
                 KEY FORMAT TEXT VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION c \
                 INCLUDE PARTITION ENVELOPE UPSERT"
            ),
            vec![
                "stmt:create_source",
                "source:kafka",
                "format:avro",
                "format:text",
                "envelope:upsert",
                "syntax:include_metadata",
                "syntax:key_value_format",
                "option:kafka_source:topic",
            ]
        );
    }
}
