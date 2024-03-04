// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use itertools::Itertools;
use uncased::UncasedStr;

use mz_ore::str::StrExt;

use crate::session::vars::Var;

/// Errors that can occur when working with [`Var`]s
///
/// [`Var`]: crate::session::vars::Var
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum VarError {
    /// The specified session parameter is constrained to a finite set of values.
    #[error(
        "invalid value for parameter {}: {}",
        name.quoted(),
        invalid_values.iter().map(|v| v.quoted()).join(",")
    )]
    ConstrainedParameter {
        /// Name of the parameter.
        name: &'static str,
        invalid_values: Vec<String>,
        valid_values: Option<Vec<&'static str>>,
    },
    /// The specified parameter is fixed to a single specific value.
    ///
    /// We allow setting the parameter to its fixed value for compatibility
    /// with PostgreSQL-based tools.
    #[error(
        "parameter {} can only be set to {}",
        name.quoted(),
        value.quoted(),
    )]
    FixedValueParameter {
        /// Name of the parameter.
        name: &'static str,
        /// The value the parameter is fixed at.
        value: String,
    },
    /// The value for the specified parameter does not have the right type.
    #[error(
        "parameter {} requires a {} value",
        name.quoted(),
        required_type.quoted()
    )]
    InvalidParameterType {
        /// Name of the parameter.
        name: &'static str,
        /// Required type of the parameter.
        required_type: Cow<'static, str>,
    },
    /// The value of the specified parameter is incorrect.
    #[error(
        "parameter {} cannot have value {}: {}",
        name.quoted(),
        invalid_values
            .iter()
            .map(|v| v.quoted().to_string())
            .collect::<Vec<_>>()
            .join(","),
        reason,
    )]
    InvalidParameterValue {
        /// Name of the parameter.
        name: &'static str,
        /// Invalid values.
        invalid_values: Vec<String>,
        /// Reason the values are invalid.
        reason: String,
    },
    /// The specified session parameter is read only.
    #[error("parameter {} cannot be changed", .0.quoted())]
    ReadOnlyParameter(&'static str),
    /// The named parameter is unknown to the system.
    #[error("unrecognized configuration parameter {}", .0.quoted())]
    UnknownParameter(String),
    /// The specified session parameter is read only unless in unsafe mode.
    #[error("parameter {} can only be set in unsafe mode", .0.quoted())]
    RequiresUnsafeMode(&'static str),
    #[error("{} is not supported", .feature)]
    RequiresFeatureFlag {
        feature: String,
        detail: Option<String>,
        /// If we're running in unsafe mode and hit this error, we should surface the flag name that
        /// needs to be set to make the feature work.
        name_hint: Option<&'static UncasedStr>,
    },
}

impl VarError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::RequiresFeatureFlag { detail, .. } => {
                match detail {
                    None => Some("The requested feature is typically meant only for internal development and testing of Materialize.".into()),
                    o => o.clone()
                }
            }
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            VarError::ConstrainedParameter {
                valid_values: Some(valid_values),
                ..
            } => Some(format!("Available values: {}.", valid_values.join(", "))),
            VarError::RequiresFeatureFlag { name_hint, .. } => {
                name_hint.map(|name| format!("Enable with {name} flag"))
            }
            _ => None,
        }
    }
}

/// Errors that can occur when parsing [`VarInput`].
///
/// Note: This exists as a separate type from [`VarError`] because [`VarError`] wants to know about
/// the [`Var`] we're parsing. We could provide this info to [`Value::parse`] but it's simpler to
/// later enrich with [`VarParseError::into_var_error`].
///
/// [`VarInput`]: crate::session::vars::VarInput
/// [`Value::parse`]: crate::session::vars::value::Value::parse
#[derive(Debug)]
pub enum VarParseError {
    /// Minimal version of [`VarError::ConstrainedParameter`].
    ConstrainedParameter {
        invalid_values: Vec<String>,
        valid_values: Option<Vec<&'static str>>,
    },
    /// Minimal version of [`VarError::FixedValueParameter`].
    FixedValueParameter,
    /// Minimal version of [`VarError::InvalidParameterType`].
    InvalidParameterType,
    /// Minimal version of [`VarError::InvalidParameterValue`].
    InvalidParameterValue {
        invalid_values: Vec<String>,
        reason: String,
    },
}

impl VarParseError {
    /// Enrich this [`VarParseError`] with information about the [`Var`] we parsed.
    pub fn into_var_error(self, var: &dyn Var) -> VarError {
        match self {
            VarParseError::ConstrainedParameter {
                invalid_values,
                valid_values,
            } => VarError::ConstrainedParameter {
                name: var.name(),
                invalid_values,
                valid_values,
            },
            VarParseError::FixedValueParameter => VarError::FixedValueParameter {
                name: var.name(),
                value: var.value(),
            },
            VarParseError::InvalidParameterType => VarError::InvalidParameterType {
                name: var.name(),
                required_type: var.type_name().into(),
            },
            VarParseError::InvalidParameterValue {
                invalid_values,
                reason,
            } => VarError::InvalidParameterValue {
                name: var.name(),
                invalid_values,
                reason,
            },
        }
    }
}
