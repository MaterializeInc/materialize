// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod attribute;
mod dot;
mod hir;
mod mir;
mod model;
mod rewrite;
#[cfg(test)]
mod test;
mod validator;

pub use model::{BoxId, DistinctOperation, Model, QuantifierId};
pub use validator::{ValidationError, ValidationResult};
