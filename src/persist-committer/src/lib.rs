// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Centralized persist consensus committer service.
//!
//! See `doc/developer/design/20260527_persist_committer.md`.

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/mz_persist_committer.rs"));
}

pub mod cache;
pub mod metrics;
pub mod refresh;
pub mod server;
pub mod subscribe;

pub use server::PersistCommitter;
