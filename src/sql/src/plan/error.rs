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
use std::num::ParseIntError;
use std::num::TryFromIntError;

use mz_expr::EvalError;
use mz_ore::stack::RecursionLimitError;
use mz_ore::str::StrExt;
use mz_repr::adt::char::InvalidCharLengthError;
use mz_repr::adt::numeric::InvalidNumericMaxScaleError;
use mz_repr::adt::varchar::InvalidVarCharMaxLengthError;
use mz_repr::strconv;
use mz_repr::ColumnName;
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
    InvalidVarCharMaxLength(InvalidVarCharMaxLengthError),
    InvalidSecret(ResolvedObjectName),
    InvalidTemporarySchema,
    Parser(ParserError),
    Qgm(QGMError),
    DropViewOnMaterializedView(String),
    AlterViewOnMaterializedView(String),
    ShowCreateViewOnMaterializedView(String),
    ExplainViewOnMaterializedView(String),
    UnacceptableTimelineName(String),
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
        None
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            Self::DropViewOnMaterializedView(_) => {
                Some("Use DROP MATERIALIZED VIEW to remove a materialized view.".into())
            }
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
            Self::UnacceptableTimelineName(name) => write!(
                f,
                "unacceptable timeline name {}",
                name.quoted(),
            ),
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
            Self::InvalidSecret(i) => write!(f, "{} is not a secret", i.full_name_str()),
            Self::Qgm(e) => e.fmt(f),
            Self::InvalidTemporarySchema => {
                write!(f, "cannot create temporary item in non-temporary schema")
            }
            Self::DropViewOnMaterializedView(name)
            | Self::AlterViewOnMaterializedView(name)
            | Self::ShowCreateViewOnMaterializedView(name)
            | Self::ExplainViewOnMaterializedView(name) => write!(f, "{name} is not a view"),
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
