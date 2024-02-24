// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use uncased::UncasedStr;

use mz_ore::str::StrExt;

use crate::session::vars::Var;

/// Errors that can occur when working with [`Var`]s
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum VarError {
    /// The specified session parameter is constrained to a finite set of
    /// values.
    #[error(
        "invalid value for parameter {}: {}",
        parameter.name.quoted(),
        values.iter().map(|v| v.quoted()).join(",")
    )]
    ConstrainedParameter {
        parameter: VarErrParam,
        values: Vec<String>,
        valid_values: Option<Vec<&'static str>>,
    },
    /// The specified parameter is fixed to a single specific value.
    ///
    /// We allow setting the parameter to its fixed value for compatibility
    /// with PostgreSQL-based tools.
    #[error(
        "parameter {} can only be set to {}",
        .0.name.quoted(),
        .0.value.quoted(),
    )]
    FixedValueParameter(VarErrParam),
    /// The value for the specified parameter does not have the right type.
    #[error(
        "parameter {} requires a {} value",
        .0.name.quoted(),
        .0.type_name.quoted()
    )]
    InvalidParameterType(VarErrParam),
    /// The value of the specified parameter is incorrect.
    #[error(
        "parameter {} cannot have value {}: {}",
        parameter.name.quoted(),
        values
            .iter()
            .map(|v| v.quoted().to_string())
            .collect::<Vec<_>>()
            .join(","),
        reason,
    )]
    InvalidParameterValue {
        parameter: VarErrParam,
        values: Vec<String>,
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

#[derive(Debug, Clone, Eq, PartialEq)]
/// We don't want to hold a static reference to a variable while erroring, so take an owned version
/// of the fields we want.
pub struct VarErrParam {
    name: &'static str,
    value: String,
    type_name: String,
}

impl<'a, V: Var + Send + Sync + ?Sized> From<&'a V> for VarErrParam {
    fn from(var: &'a V) -> VarErrParam {
        VarErrParam {
            name: var.name(),
            value: var.value(),
            type_name: var.type_name(),
        }
    }
}
