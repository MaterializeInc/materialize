// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implements outerjoin simplification as a variant of Algorithm A in the seminal
//! paper by Rosenthal and Galindo-Legaria[^1].
//!
//! [^1]: [Galindo-Legaria, Cesar, and Arnon Rosenthal.
//! "Outerjoin simplification and reordering for query optimization."
//! ACM Transactions on Database Systems (TODS) 22.1 (1997): 43-74.
//! ](https://www.academia.edu/26160408/Outerjoin_simplification_and_reordering_for_query_optimization)

pub(crate) mod simplify_outer_joins;
