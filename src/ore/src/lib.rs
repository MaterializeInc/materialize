// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Internal utility libraries for Materialize.
//!
//! **ore** (_n_): the raw material from which more valuable materials are extracted.
//! Modules are included in this crate when they are broadly useful but too
//! small to warrant their own crate.

#![deny(missing_docs, missing_debug_implementations)]

pub mod cast;
pub mod collections;
pub mod env;
pub mod fmt;
pub mod future;
pub mod hash;
pub mod hint;
pub mod iter;
pub mod netio;
pub mod option;
pub mod panic;
pub mod result;
pub mod retry;
pub mod stats;
pub mod sync;
pub mod test;
pub mod thread;
pub mod tokio;
pub mod vec;
