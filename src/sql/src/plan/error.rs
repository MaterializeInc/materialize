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

use expr::EvalError;
use ore::stack::RecursionLimitError;
use ore::str::StrExt;
use repr::strconv::ParseError;

use crate::catalog::CatalogError;

#[derive(Debug)]
pub enum PlanError {
    Unsupported {
        feature: String,
        issue_no: Option<usize>,
    },
    UnknownColumn(String),
    AmbiguousColumn(String),
    MisqualifiedName(String),
    OverqualifiedDatabaseName(String),
    OverqualifiedSchemaName(String),
    SubqueriesDisallowed {
        context: String,
    },
    UnknownParameter(usize),
    RecursionLimit(RecursionLimitError),
    Parse(ParseError),
    Catalog(CatalogError),
    UpsertSinkWithoutKey,
    // TODO(benesch): eventually all errors should be structured.
    Unstructured(String),
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
            Self::UnknownColumn(name) => write!(f, "column {} does not exist", name.quoted()),
            Self::AmbiguousColumn(name) => write!(f, "column name {} is ambiguous", name.quoted()),
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
            Self::SubqueriesDisallowed { context } => {
                write!(f, "{} does not allow subqueries", context)
            }
            Self::UnknownParameter(n) => write!(f, "there is no parameter ${}", n),
            Self::RecursionLimit(e) => write!(f, "{}", e),
            Self::Parse(e) => write!(f, "{}", e),
            Self::Catalog(e) => write!(f, "{}", e),
            Self::UpsertSinkWithoutKey => write!(f, "upsert sinks must specify a key"),
            Self::Unstructured(e) => write!(f, "{}", e),
        }
    }
}

impl Error for PlanError {}

impl From<CatalogError> for PlanError {
    fn from(e: CatalogError) -> PlanError {
        PlanError::Catalog(e)
    }
}

impl From<ParseError> for PlanError {
    fn from(e: ParseError) -> PlanError {
        PlanError::Parse(e)
    }
}

impl From<RecursionLimitError> for PlanError {
    fn from(e: RecursionLimitError) -> PlanError {
        PlanError::RecursionLimit(e)
    }
}

impl From<anyhow::Error> for PlanError {
    fn from(e: anyhow::Error) -> PlanError {
        PlanError::Unstructured(format!("{:#}", e))
    }
}

impl From<TryFromIntError> for PlanError {
    fn from(e: TryFromIntError) -> PlanError {
        PlanError::Unstructured(format!("{:#}", e))
    }
}

impl From<ParseIntError> for PlanError {
    fn from(e: ParseIntError) -> PlanError {
        PlanError::Unstructured(format!("{:#}", e))
    }
}

impl From<EvalError> for PlanError {
    fn from(e: EvalError) -> PlanError {
        PlanError::Unstructured(format!("{:#}", e))
    }
}
