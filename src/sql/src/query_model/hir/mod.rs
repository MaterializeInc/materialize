// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Conversion methods a `Model` to a `HirRelationExpr` and back.
//!
//! The public interface consists of:
//! * `From<HirRelationExpr>` implemenation for `Result<Model, QGMError>`.
//! * `From<Model>` implemenation for `HirRelationExpr`.

mod hir_from_qgm;
mod qgm_from_hir;
