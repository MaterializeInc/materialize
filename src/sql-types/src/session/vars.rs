// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Session variable input types.

use serde::Serialize;

/// Represents the input to a variable.
///
/// Each variable has different rules for how it handles each style of input.
/// This type allows us to defer interpretation of the input until the
/// variable-specific interpretation can be applied.
#[derive(Debug, Clone, Copy)]
pub enum VarInput<'a> {
    /// The input has been flattened into a single string.
    ///
    /// NOTE: when adding a new variant here (or in [`OwnedVarInput`]), extend
    /// the `mz_catalog.mz_role_parameters` materialized view in
    /// `src/catalog/src/builtin/mz_catalog.rs`. That MV discriminates on the
    /// externally-tagged JSON shape of [`OwnedVarInput`] to format
    /// `parameter_value`.
    Flat(&'a str),
    /// The input comes from a SQL `SET` statement and is jumbled across
    /// multiple components.
    ///
    /// NOTE: see the doc-comment on [`VarInput::Flat`] — adding a new variant
    /// requires extending `mz_catalog.mz_role_parameters`.
    SqlSet(&'a [String]),
}

impl<'a> VarInput<'a> {
    /// Converts the variable input to an owned vector of strings.
    pub fn to_vec(&self) -> Vec<String> {
        match self {
            VarInput::Flat(v) => vec![v.to_string()],
            VarInput::SqlSet(values) => values.into_iter().map(|v| v.to_string()).collect(),
        }
    }
}

/// An owned version of [`VarInput`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum OwnedVarInput {
    /// See [`VarInput::Flat`].
    ///
    /// NOTE: adding a new variant requires extending the
    /// `mz_catalog.mz_role_parameters` materialized view in
    /// `src/catalog/src/builtin/mz_catalog.rs`, which discriminates on the
    /// externally-tagged JSON shape of this enum.
    Flat(String),
    /// See [`VarInput::SqlSet`].
    ///
    /// NOTE: see the doc-comment on [`OwnedVarInput::Flat`].
    SqlSet(Vec<String>),
}

impl OwnedVarInput {
    /// Converts this owned variable input as a [`VarInput`].
    pub fn borrow(&self) -> VarInput<'_> {
        match self {
            OwnedVarInput::Flat(v) => VarInput::Flat(v),
            OwnedVarInput::SqlSet(v) => VarInput::SqlSet(v),
        }
    }
}
