// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines QGM-related errors and traits for those errors.
//!
//! The public interface consists of the [`QGMError`] method and the
//! implemented traits.

use std::error::Error;
use std::fmt;

use crate::plan::{HirRelationExpr, HirScalarExpr};
use crate::query_model::model::{BoxScalarExpr, BoxType, QuantifierType};

/// Errors that can occur while handling a QGM model.
///
/// A bunch of the error types exist because our support for HIR ⇒ QGM
/// conversion and QGM ⇒ MIR conversion is currently incomplete. They will be
/// removed once these limitations are addressed.
#[derive(Debug, Clone)]
pub enum QGMError {
    /// Indicates HIR ⇒ QGM conversion failure due to unsupported [`HirRelationExpr`].
    UnsupportedHirRelationExpr(UnsupportedHirRelationExpr),
    /// Indicates HIR ⇒ QGM conversion failure due to unsupported [`HirScalarExpr`].
    UnsupportedHirScalarExpr(UnsupportedHirScalarExpr),
    /// Indicates QGM ⇒ MIR conversion failure due to unsupported box type.
    UnsupportedBoxType(UnsupportedBoxType),
    /// Indicates QGM ⇒ MIR conversion failure due to unsupported scalar expression.
    UnsupportedBoxScalarExpr(UnsupportedBoxScalarExpr),
    /// Indicates QGM ⇒ MIR conversion failure due to unsupported quantifier type.
    UnsupportedQuantifierType(UnsupportedQuantifierType),
    /// Indicates QGM ⇒ MIR conversion failure due to lack of support for decorrelation.
    UnsupportedDecorrelation(UnsupportedDecorrelation),
    /// An unstructured error.
    Internal(Internal),
}

impl Error for QGMError {}

impl fmt::Display for QGMError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QGMError::UnsupportedHirRelationExpr(e) => write!(f, "{}", e),
            QGMError::UnsupportedHirScalarExpr(e) => write!(f, "{}", e),
            QGMError::UnsupportedBoxType(e) => write!(f, "{}", e),
            QGMError::UnsupportedBoxScalarExpr(e) => write!(f, "{}", e),
            QGMError::UnsupportedQuantifierType(e) => write!(f, "{}", e),
            QGMError::UnsupportedDecorrelation(e) => write!(f, "{}", e),
            QGMError::Internal(e) => write!(f, "{}", e),
        }
    }
}

impl From<QGMError> for String {
    fn from(error: QGMError) -> Self {
        format!("{}", error)
    }
}

#[derive(Debug, Clone)]
pub struct UnsupportedHirRelationExpr {
    pub(crate) expr: HirRelationExpr,
    pub(crate) explanation: Option<String>,
}

impl From<UnsupportedHirRelationExpr> for QGMError {
    fn from(inner: UnsupportedHirRelationExpr) -> Self {
        QGMError::UnsupportedHirRelationExpr(inner)
    }
}

impl fmt::Display for UnsupportedHirRelationExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Unsupported HirRelationExpr variant in QGM conversion: {}.",
            serde_json::to_string(&self.expr).unwrap()
        )?;
        if let Some(explanation) = &self.explanation {
            write!(f, " Explanation: {}", explanation)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct UnsupportedHirScalarExpr {
    pub(crate) scalar: HirScalarExpr,
}

impl From<UnsupportedHirScalarExpr> for QGMError {
    fn from(inner: UnsupportedHirScalarExpr) -> Self {
        QGMError::UnsupportedHirScalarExpr(inner)
    }
}

impl fmt::Display for UnsupportedHirScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Unsupported HirScalarExpr variant in QGM conversion: {}",
            serde_json::to_string(&self.scalar).unwrap()
        )
    }
}

#[derive(Debug, Clone)]
pub struct UnsupportedBoxType {
    pub(crate) box_type: BoxType,
    pub(crate) explanation: Option<String>,
}

impl From<UnsupportedBoxType> for QGMError {
    fn from(inner: UnsupportedBoxType) -> Self {
        QGMError::UnsupportedBoxType(inner)
    }
}

impl fmt::Display for UnsupportedBoxType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Unsupported box type in MIR conversion: {:?}.",
            self.box_type
        )?;
        if let Some(explanation) = &self.explanation {
            write!(f, " Explanation: {}", explanation)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct UnsupportedQuantifierType {
    pub(crate) quantifier_type: QuantifierType,
}

impl From<UnsupportedQuantifierType> for QGMError {
    fn from(inner: UnsupportedQuantifierType) -> Self {
        QGMError::UnsupportedQuantifierType(inner)
    }
}

impl fmt::Display for UnsupportedQuantifierType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Unsupported quantifier type in MIR conversion: {}.",
            self.quantifier_type
        )
    }
}

#[derive(Debug, Clone)]
pub struct UnsupportedBoxScalarExpr {
    pub(crate) scalar: BoxScalarExpr,
}

impl From<UnsupportedBoxScalarExpr> for QGMError {
    fn from(inner: UnsupportedBoxScalarExpr) -> Self {
        QGMError::UnsupportedBoxScalarExpr(inner)
    }
}

impl fmt::Display for UnsupportedBoxScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Unsupported QGM scalar expression in MIR conversion: {}.",
            self.scalar
        )
    }
}

#[derive(Debug, Clone)]
pub struct UnsupportedDecorrelation {
    pub(crate) msg: String,
}

impl From<UnsupportedDecorrelation> for QGMError {
    fn from(inner: UnsupportedDecorrelation) -> Self {
        QGMError::UnsupportedDecorrelation(inner)
    }
}

impl fmt::Display for UnsupportedDecorrelation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

#[derive(Debug, Clone)]
pub struct Internal {
    pub(crate) msg: String,
}

impl From<Internal> for QGMError {
    fn from(inner: Internal) -> Self {
        QGMError::Internal(inner)
    }
}

impl fmt::Display for Internal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}
