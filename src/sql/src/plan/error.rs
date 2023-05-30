// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::error::Error;
use std::num::{ParseIntError, TryFromIntError};
use std::sync::Arc;
use std::{fmt, io};

use itertools::Itertools;
use mz_expr::EvalError;
use mz_ore::error::ErrorExt;
use mz_ore::stack::RecursionLimitError;
use mz_ore::str::{separated, StrExt};
use mz_postgres_util::PostgresError;
use mz_repr::adt::char::InvalidCharLengthError;
use mz_repr::adt::numeric::InvalidNumericMaxScaleError;
use mz_repr::adt::system::Oid;
use mz_repr::adt::varchar::InvalidVarCharMaxLengthError;
use mz_repr::{strconv, ColumnName, GlobalId};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{ObjectType, Privilege, UnresolvedItemName};
use mz_sql_parser::parser::ParserError;

use crate::catalog::{CatalogError, CatalogItemType};
use crate::names::{PartialItemName, ResolvedItemName};
use crate::plan::plan_utils::JoinSide;
use crate::plan::scope::ScopeItem;
use crate::session::vars::VarError;

#[derive(Clone, Debug)]
pub enum PlanError {
    /// This feature is not yet supported, but may be supported at some point in the future.
    Unsupported {
        feature: String,
        issue_no: Option<usize>,
    },
    /// This feature is not supported, and will likely never be supported.
    NeverSupported {
        feature: String,
        documentation_link: String,
    },
    UnknownColumn {
        table: Option<PartialItemName>,
        column: ColumnName,
    },
    UngroupedColumn {
        table: Option<PartialItemName>,
        column: ColumnName,
    },
    WrongJoinTypeForLateralColumn {
        table: Option<PartialItemName>,
        column: ColumnName,
    },
    AmbiguousColumn(ColumnName),
    AmbiguousTable(PartialItemName),
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
    InvalidWmrRecursionLimit(String),
    InvalidNumericMaxScale(InvalidNumericMaxScaleError),
    InvalidCharLength(InvalidCharLengthError),
    InvalidId(GlobalId),
    InvalidObject(Box<ResolvedItemName>),
    InvalidObjectType {
        expected_type: ObjectType,
        actual_type: ObjectType,
        object_name: String,
    },
    InvalidPrivilegeTypes {
        privilege_types: Vec<Privilege>,
        object_type: ObjectType,
    },
    InvalidVarCharMaxLength(InvalidVarCharMaxLengthError),
    InvalidSecret(Box<ResolvedItemName>),
    InvalidTemporarySchema,
    Parser(ParserError),
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
    PostgresConnectionErr {
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
        name: UnresolvedItemName,
    },
    /// Declaration of a recursive type did not match the inferred type.
    RecursiveTypeMismatch(String, Vec<String>, Vec<String>),
    UnknownFunction {
        name: String,
        arg_types: Vec<String>,
    },
    IndistinctFunction {
        name: String,
        arg_types: Vec<String>,
    },
    UnknownOperator {
        name: String,
        arg_types: Vec<String>,
    },
    IndistinctOperator {
        name: String,
        arg_types: Vec<String>,
    },
    InvalidPrivatelinkAvailabilityZone {
        name: String,
        supported_azs: BTreeSet<String>,
    },
    InvalidSchemaName,
    ItemAlreadyExists {
        name: String,
        item_type: CatalogItemType,
    },
    ModifyLinkedCluster {
        cluster_name: String,
        linked_object_name: String,
    },
    EmptyPublication(String),
    DuplicateSubsourceReference {
        name: UnresolvedItemName,
        upstream_references: Vec<UnresolvedItemName>,
    },
    PostgresDatabaseMissingFilteredSchemas {
        schemas: Vec<String>,
    },
    InvalidKeysInSubscribeEnvelopeUpsert,
    InvalidKeysInSubscribeEnvelopeDebezium,
    InvalidOrderByInSubscribeWithinTimestampOrderBy,
    FromValueRequiresParen,
    VarError(VarError),
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
            Self::FetchingCsrSchemaFailed { cause, .. } => Some(cause.to_string_with_causes()),
            Self::PostgresConnectionErr { cause } => Some(cause.to_string_with_causes()),
            Self::InvalidProtobufSchema { cause } => Some(cause.to_string_with_causes()),
            Self::InvalidOptionValue { err, .. } => err.detail(),
            Self::VarError(e) => e.detail(),
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
            Self::PostgresConnectionErr { cause } => {
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
            Self::InvalidOptionValue { err, .. } => err.hint(),
            Self::UnknownFunction { ..} => Some("No function matches the given name and argument types. You might need to add explicit type casts.".into()),
            Self::IndistinctFunction {..} => {
                Some("Could not choose a best candidate function. You might need to add explicit type casts.".into())
            }
            Self::UnknownOperator {..} => {
                Some("No operator matches the given name and argument types. You might need to add explicit type casts.".into())
            }
            Self::IndistinctOperator {..} => {
                Some("Could not choose a best candidate operator. You might need to add explicit type casts.".into())
            },
            Self::InvalidPrivatelinkAvailabilityZone { supported_azs, ..} => {
                let supported_azs_str = supported_azs.iter().join("\n  ");
                Some(format!("Did you supply an availability zone name instead of an ID? Known availability zone IDs:\n  {}", supported_azs_str))
            }
            Self::DuplicateSubsourceReference { .. } => {
                Some("Specify target table names using FOR TABLES (foo AS bar), or limit the upstream tables using FOR SCHEMAS (foo)".into())
            }
            Self::InvalidKeysInSubscribeEnvelopeUpsert => {
                Some("All keys must be columns on the underlying relation.".into())
            }
            Self::InvalidKeysInSubscribeEnvelopeDebezium => {
                Some("All keys must be columns on the underlying relation.".into())
            }
            Self::InvalidOrderByInSubscribeWithinTimestampOrderBy => {
                Some("All order bys must be output columns.".into())
            }
            Self::Catalog(e) => e.hint(),
            Self::VarError(e) => e.hint(),
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
            Self::NeverSupported { feature, documentation_link: documentation_path } => {
                write!(f, "{feature} is not supported, for more information consult the documentation at https://materialize.com/docs/{documentation_path}",)?;
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
            Self::InvalidWmrRecursionLimit(msg) => write!(f, "Invalid WITH MUTUALLY RECURSIVE recursion limit. {}", msg),
            Self::InvalidNumericMaxScale(e) => e.fmt(f),
            Self::InvalidCharLength(e) => e.fmt(f),
            Self::InvalidVarCharMaxLength(e) => e.fmt(f),
            Self::Parser(e) => e.fmt(f),
            Self::Unstructured(e) => write!(f, "{}", e),
            Self::InvalidId(id) => write!(f, "invalid id {}", id),
            Self::InvalidObject(i) => write!(f, "{} is not a database object", i.full_name_str()),
            Self::InvalidObjectType{expected_type, actual_type, object_name} => write!(f, "{actual_type} {object_name} is not a {expected_type}"),
            Self::InvalidPrivilegeTypes{privilege_types, object_type} => {
                write!(f, "invalid privilege types {} for {}", privilege_types.into_iter().join(", "), object_type)
            },
            Self::InvalidSecret(i) => write!(f, "{} is not a secret", i.full_name_str()),
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
            Self::PostgresConnectionErr { .. } => {
                write!(f, "failed to connect to PostgreSQL database")
            }
            Self::InvalidProtobufSchema { .. } => {
                write!(f, "invalid protobuf schema")
            }
            Self::DropSubsource{subsource, source: _} => write!(f, "SOURCE {subsource} is a subsource and cannot be dropped independently of its primary source"),
            Self::InvalidOptionValue { option_name, err } => write!(f, "invalid {} option value: {}", option_name, err),
            Self::UnexpectedDuplicateReference { name } => write!(f, "unexpected multiple references to {}", name.to_ast_string()),
            Self::RecursiveTypeMismatch(name, declared, inferred) => {
                let declared = separated(", ", declared);
                let inferred = separated(", ", inferred);
                let name = name.quoted();
                write!(f, "declared type ({declared}) of WITH MUTUALLY RECURSIVE query {name} did not match inferred type ({inferred})")
            },
            Self::UnknownFunction {name, arg_types, ..} => {
                write!(f, "function {}({}) does not exist", name, arg_types.join(", "))
            },
            Self::IndistinctFunction {name, arg_types, ..} => {
                write!(f, "function {}({}) is not unique", name, arg_types.join(", "))
            },
            Self::UnknownOperator {name, arg_types, ..} => {
                write!(f, "operator does not exist: {}", match arg_types.as_slice(){
                    [typ] => format!("{} {}", name, typ),
                    [ltyp, rtyp] => {
                        format!("{} {} {}", ltyp, name, rtyp)
                    }
                    _ => unreachable!("non-unary non-binary operator"),
                })
            },
            Self::IndistinctOperator {name, arg_types, ..} => {
                write!(f, "operator is not unique: {}", match arg_types.as_slice(){
                    [typ] => format!("{} {}", name, typ),
                    [ltyp, rtyp] => {
                        format!("{} {} {}", ltyp, name, rtyp)
                    }
                    _ => unreachable!("non-unary non-binary operator"),
                })
            },
            Self::InvalidPrivatelinkAvailabilityZone { name, ..} => write!(f, "invalid AWS PrivateLink availability zone {}", name.quoted()),
            Self::InvalidSchemaName => write!(f, "no schema has been selected to create in"),
            Self::ItemAlreadyExists { name, item_type } => write!(f, "{item_type} {} already exists", name.quoted()),
            Self::ModifyLinkedCluster {cluster_name, ..} => write!(f, "cannot modify linked cluster {}", cluster_name.quoted()),
            Self::EmptyPublication(publication) => write!(f, "PostgreSQL PUBLICATION {publication} is empty"),
            Self::DuplicateSubsourceReference { name, upstream_references } => {
                write!(f, "multiple tables with name {}: {}", name.to_ast_string_stable(), itertools::join(upstream_references.iter().map(|n| n.to_ast_string_stable()), ", "))
            },
            Self::PostgresDatabaseMissingFilteredSchemas { schemas} => {
                write!(f, "FOR SCHEMAS (..) included {}, but PostgreSQL database has no schema with that name", itertools::join(schemas.iter(), ", "))
            }
            Self::InvalidKeysInSubscribeEnvelopeUpsert => {
                write!(f, "invalid keys in SUBSCRIBE ENVELOPE UPSERT (KEY (..))")
            }
            Self::InvalidKeysInSubscribeEnvelopeDebezium => {
                write!(f, "invalid keys in SUBSCRIBE ENVELOPE DEBEZIUM (KEY (..))")
            }
            Self::InvalidOrderByInSubscribeWithinTimestampOrderBy => {
                write!(f, "invalid ORDER BY in SUBSCRIBE WITHIN TIMESTAMP ORDER BY")
            }
            Self::FromValueRequiresParen => f.write_str(
                "VALUES expression in FROM clause must be surrounded by parentheses"
            ),
            Self::VarError(e) => e.fmt(f),
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
        // WIP: Do we maybe want to keep the alternate selector for these?
        sql_err!("{}", e.display_with_causes())
    }
}

impl From<TryFromIntError> for PlanError {
    fn from(e: TryFromIntError) -> PlanError {
        sql_err!("{}", e.display_with_causes())
    }
}

impl From<ParseIntError> for PlanError {
    fn from(e: ParseIntError) -> PlanError {
        sql_err!("{}", e.display_with_causes())
    }
}

impl From<EvalError> for PlanError {
    fn from(e: EvalError) -> PlanError {
        sql_err!("{}", e.display_with_causes())
    }
}

impl From<ParserError> for PlanError {
    fn from(e: ParserError) -> PlanError {
        PlanError::Parser(e)
    }
}

impl From<PostgresError> for PlanError {
    fn from(e: PostgresError) -> PlanError {
        PlanError::PostgresConnectionErr { cause: Arc::new(e) }
    }
}

impl From<VarError> for PlanError {
    fn from(e: VarError) -> Self {
        PlanError::VarError(e)
    }
}

struct ColumnDisplay<'a> {
    table: &'a Option<PartialItemName>,
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
