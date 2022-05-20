// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A persistent, compacting, indexed data structure of `(Key, Value, Time,
//! i64)` updates.

// NB: These really don't need to be public, but the public doc lint is nice.
pub mod background;
pub mod cache;
pub mod columnar;
pub mod encoding;
pub mod metrics;
