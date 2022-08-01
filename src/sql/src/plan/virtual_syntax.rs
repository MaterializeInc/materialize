// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A set of virtual nodes that are used to recover some high-level
//! concepts that are desugared to non-trival terms in some IRs.

use mz_sql::plan::{HirRelationExpr, HirScalarExpr};
