// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations that fuse together others of their kind.

pub mod filter;
pub mod flatmap_to_map;
pub mod join;
pub mod map;
pub mod negate;
pub mod project;
pub mod reduce;
pub mod top_k;
pub mod union;
