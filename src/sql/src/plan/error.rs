// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt;
use std::io;
use std::num::ParseIntError;
use std::num::TryFromIntError;
use std::sync::Arc;

use mz_expr::EvalError;
use mz_ore::stack::RecursionLimitError;
use mz_ore::str::StrExt;
use mz_repr::adt::char::InvalidCharLengthError;
use mz_repr::adt::numeric::InvalidNumericMaxScaleError;
use mz_repr::adt::system::Oid;
use mz_repr::adt::varchar::InvalidVarCharMaxLengthError;
use mz_repr::strconv;
use mz_repr::ColumnName;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::UnresolvedObjectName;
use mz_sql_parser::parser::ParserError;

use crate::catalog::CatalogError;
use crate::names::PartialObjectName;
use crate::names::ResolvedObjectName;
use crate::plan::plan_utils::JoinSide;
use crate::plan::scope::ScopeItem;
use crate::query_model::QGMError;

#[derive(Clone, Debug)]
pub enum PlanError {
    Unsupported {
        feature: String,
        issue_no: Option<usize>,
    },
    UnknownColumn {
        table: Option<PartialObjectName>,
        column: ColumnName,
    },
    UngroupedColumn {
        table: Option<PartialObjectName>,
        column: ColumnName,
    },
    WrongJoinTypeForLateralColumn {
        table: Option<PartialObjectName>,
        column: ColumnName,
    },
    AmbiguousColumn(ColumnName),
    AmbiguousTable(PartialObjectName),
    UnknownColumnInUsingClause {
        column: ColumnName,
        join_side: JoinSide,
    },
    AmbiguousColumnInUsingClause {
        column: ColumnName,
        join_side: JoinSide,
    },
    MisqualifiedName(String),
    OverqualifiedDatabaseName(String),
    OverqualifiedSchemaName(String),
    UnderqualifiedColumnName(String),
    SubqueriesDisallowed {
        context: String,
    },
    UnknownParameter(usize),
    RecursionLimit(RecursionLimitError),
    StrconvParse(strconv::ParseError),
    Catalog(CatalogError),
    UpsertSinkWithoutKey,
    InvalidNumericMaxScale(InvalidNumericMaxScaleError),
    InvalidCharLength(InvalidCharLengthError),
    InvalidObject(ResolvedObjectName),
    InvalidVarCharMaxLength(InvalidVarCharMaxLengthError),
    InvalidSecret(ResolvedObjectName),
    InvalidTemporarySchema,
    Parser(ParserError),
    Qgm(QGMError),
    DropViewOnMaterializedView(String),
    DropSubsource {
        subsource: String,
        source: String,
    },
    AlterViewOnMaterializedView(String),
    ShowCreateViewOnMaterializedView(String),
    ExplainViewOnMaterializedView(String),
    UnacceptableTimelineName(String),
    UnrecognizedTypeInPostgresSource {
        cols: Vec<(String, Oid)>,
    },
    FetchingCsrSchemaFailed {
        schema_lookup: String,
        cause: Arc<dyn Error + Send + Sync>,
    },
    FetchingPostgresPublicationInfoFailed {
        cause: Arc<mz_postgres_util::PostgresError>,
    },
    InvalidProtobufSchema {
        cause: protobuf_native::OperationFailedError,
    },
    InvalidOptionValue {
        // Expected to be generated from the `to_ast_string` value on the option
        // name.
        option_name: String,
        err: Box<PlanError>,
    },
    UnexpectedDuplicateReference {
        name: UnresolvedObjectName,
    },
    // TODO(benesch): eventually all errors should be structured.
    Unstructured(String),
}

impl PlanError {
    pub(crate) fn ungrouped_column(item: &ScopeItem) -> PlanError {
        PlanError::UngroupedColumn {
            table: item.table_name.clone(),
            column: item.column_name.clone(),
        }
    }

    pub fn detail(&self) -> Option<String> {
        match self {
            Self::FetchingCsrSchemaFailed { cause, .. } => Some(cause.to_string()),
            Self::FetchingPostgresPublicationInfoFailed { cause } => Some(cause.to_string()),
            Self::InvalidProtobufSchema { cause } => Some(cause.to_string()),
            Self::InvalidOptionValue { err, .. } => err.detail(),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            Self::DropViewOnMaterializedView(_) => {
                Some("Use DROP MATERIALIZED VIEW to remove a materialized view.".into())
            }
            Self::DropSubsource { source, .. } => Some(format!(
                "Use DROP SOURCE {source} to drop this subsource's primary source and all of its other subsources"
            )),
            Self::AlterViewOnMaterializedView(_) => {
                Some("Use ALTER MATERIALIZED VIEW to rename a materialized view.".into())
            }
            Self::ShowCreateViewOnMaterializedView(_) => {
                Some("Use SHOW CREATE MATERIALIZED VIEW to show a materialized view.".into())
            }
            Self::ExplainViewOnMaterializedView(_) => {
                Some("Use EXPLAIN [...] MATERIALIZED VIEW to explain a materialized view.".into())
            }
            Self::UnacceptableTimelineName(_) => {
                Some("The prefix \"mz_\" is reserved for system timelines.".into())
            }
            Self::UnrecognizedTypeInPostgresSource {
                cols: _,
            } => Some(
                "Use the TEXT COLUMNS option naming the listed columns, and Materialize can ingest their values \
                as text."
                    .into(),
            ),
            Self::FetchingPostgresPublicationInfoFailed { cause } => {
                if let Some(cause) = cause.source() {
                    if let Some(cause) = cause.downcast_ref::<io::Error>() {
                        if cause.kind() == io::ErrorKind::TimedOut {
                            return Some(
                                "Do you have a firewall or security group that is \
                                preventing Materialize from conecting to your PostgreSQL server?"
                                    .into(),
                            );
                        }
                    }
                }
                None
            }
            Self::InvalidOptionValue {  err, .. } => err.hint(),
            _ => None,
        }
    }
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Unsupported { feature, issue_no } => {
                write!(f, "{} not yet supported", feature)?;
                if let Some(issue_no) = issue_no {
                    write!(f, ", see https://github.com/MaterializeInc/materialize/issues/{} for more details", issue_no)?;
                }
                Ok(())
            }
            Self::UnknownColumn { table, column } => write!(
                f,
                "column {} does not exist",
                ColumnDisplay { table, column }
            ),
            Self::UngroupedColumn { table, column } => write!(
                f,
                "column {} must appear in the GROUP BY clause or be used in an aggregate function",
                ColumnDisplay { table, column },
            ),
            Self::WrongJoinTypeForLateralColumn { table, column } => write!(
                f,
                "column {} cannot be referenced from this part of the query: \
                the combining JOIN type must be INNER or LEFT for a LATERAL reference",
                ColumnDisplay { table, column },
            ),
            Self::AmbiguousColumn(column) => write!(
                f,
                "column reference {} is ambiguous",
                column.as_str().quoted()
            ),
            Self::AmbiguousTable(table) => write!(
                f,
                "table reference {} is ambiguous",
                table.item.as_str().quoted()
            ),
            Self::UnknownColumnInUsingClause { column, join_side } => write!(
                f,
                "column {} specified in USING clause does not exist in {} table",
                column.as_str().quoted(),
                join_side,
            ),
            Self::AmbiguousColumnInUsingClause { column, join_side } => write!(
                f,
                "common column name {} appears more than once in {} table",
                column.as_str().quoted(),
                join_side,
            ),
            Self::MisqualifiedName(name) => write!(
                f,
                "qualified name did not have between 1 and 3 components: {}",
                name
            ),
            Self::OverqualifiedDatabaseName(name) => write!(
                f,
                "database name '{}' does not have exactly one component",
                name
            ),
            Self::OverqualifiedSchemaName(name) => write!(
                f,
                "schema name '{}' cannot have more than two components",
                name
            ),
            Self::UnderqualifiedColumnName(name) => write!(
                f,
                "column name '{}' must have at least a table qualification",
                name
            ),
            Self::UnacceptableTimelineName(name) => {
                write!(f, "unacceptable timeline name {}", name.quoted(),)
            }
            Self::SubqueriesDisallowed { context } => {
                write!(f, "{} does not allow subqueries", context)
            }
            Self::UnknownParameter(n) => write!(f, "there is no parameter ${}", n),
            Self::RecursionLimit(e) => write!(f, "{}", e),
            Self::StrconvParse(e) => write!(f, "{}", e),
            Self::Catalog(e) => write!(f, "{}", e),
            Self::UpsertSinkWithoutKey => write!(f, "upsert sinks must specify a key"),
            Self::InvalidNumericMaxScale(e) => e.fmt(f),
            Self::InvalidCharLength(e) => e.fmt(f),
            Self::InvalidVarCharMaxLength(e) => e.fmt(f),
            Self::Parser(e) => e.fmt(f),
            Self::Unstructured(e) => write!(f, "{}", e),
            Self::InvalidObject(i) => write!(f, "{} is not a database object", i.full_name_str()),
            Self::InvalidSecret(i) => write!(f, "{} is not a secret", i.full_name_str()),
            Self::Qgm(e) => e.fmt(f),
            Self::InvalidTemporarySchema => {
                write!(f, "cannot create temporary item in non-temporary schema")
            }
            Self::DropViewOnMaterializedView(name)
            | Self::AlterViewOnMaterializedView(name)
            | Self::ShowCreateViewOnMaterializedView(name)
            | Self::ExplainViewOnMaterializedView(name) => write!(f, "{name} is not a view"),
            Self::UnrecognizedTypeInPostgresSource { cols } => {
                let mut cols = cols.to_owned();
                cols.sort();

                write!(
                    f,
                    "the following columns contain unsupported types:\n{}",
                    itertools::join(
                        cols.into_iter().map(|(col, Oid(oid))| format!("{} (OID {})", col, oid)),
                        "\n"
                    )
                )
            },
            Self::FetchingCsrSchemaFailed { schema_lookup, .. } => {
                write!(f, "failed to fetch schema {schema_lookup} from schema registry")
            }
            Self::FetchingPostgresPublicationInfoFailed { .. } => {
                write!(f, "failed to fetch publication information from PostgreSQL database")
            }
            Self::InvalidProtobufSchema { .. } => {
                write!(f, "invalid protobuf schema")
            }
            Self::DropSubsource{subsource, source: _} => write!(f, "SOURCE {subsource} is a subsource and cannot be dropped independently of its primary source"),
            Self::InvalidOptionValue { option_name, err } => write!(f, "invalid {} option value: {}", option_name, err),
            Self::UnexpectedDuplicateReference { name } => write!(f, "unexpected multiple references to {}", name.to_ast_string()),
        }
    }
}

impl Error for PlanError {}

impl From<CatalogError> for PlanError {
    fn from(e: CatalogError) -> PlanError {
        PlanError::Catalog(e)
    }
}

impl From<strconv::ParseError> for PlanError {
    fn from(e: strconv::ParseError) -> PlanError {
        PlanError::StrconvParse(e)
    }
}

impl From<RecursionLimitError> for PlanError {
    fn from(e: RecursionLimitError) -> PlanError {
        PlanError::RecursionLimit(e)
    }
}

impl From<InvalidNumericMaxScaleError> for PlanError {
    fn from(e: InvalidNumericMaxScaleError) -> PlanError {
        PlanError::InvalidNumericMaxScale(e)
    }
}

impl From<InvalidCharLengthError> for PlanError {
    fn from(e: InvalidCharLengthError) -> PlanError {
        PlanError::InvalidCharLength(e)
    }
}

impl From<InvalidVarCharMaxLengthError> for PlanError {
    fn from(e: InvalidVarCharMaxLengthError) -> PlanError {
        PlanError::InvalidVarCharMaxLength(e)
    }
}

impl From<anyhow::Error> for PlanError {
    fn from(e: anyhow::Error) -> PlanError {
        sql_err!("{:#}", e)
    }
}

impl From<TryFromIntError> for PlanError {
    fn from(e: TryFromIntError) -> PlanError {
        sql_err!("{:#}", e)
    }
}

impl From<ParseIntError> for PlanError {
    fn from(e: ParseIntError) -> PlanError {
        sql_err!("{:#}", e)
    }
}

impl From<EvalError> for PlanError {
    fn from(e: EvalError) -> PlanError {
        sql_err!("{:#}", e)
    }
}

impl From<ParserError> for PlanError {
    fn from(e: ParserError) -> PlanError {
        PlanError::Parser(e)
    }
}

impl From<QGMError> for PlanError {
    fn from(e: QGMError) -> PlanError {
        PlanError::Qgm(e)
    }
}

struct ColumnDisplay<'a> {
    table: &'a Option<PartialObjectName>,
    column: &'a ColumnName,
}

impl<'a> fmt::Display for ColumnDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(table) = &self.table {
            format!("{}.{}", table.item, self.column).quoted().fmt(f)
        } else {
            self.column.as_str().quoted().fmt(f)
        }
    }
}
