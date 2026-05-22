// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-memory gRPC backend for the persist `Consensus` trait.
//!
//! `bogo-consensus` is a fast, non-durable consensus backend used for
//! performance testing. It runs as a standalone server that keeps all state in
//! memory (mirroring [`mz_persist::mem::MemConsensus`]) and exposes it over
//! gRPC. The client side, in [`client`], implements just enough surface to be
//! plugged into `mz_persist::cfg::ConsensusConfig` via the `bogo://` URL
//! scheme.
//!
//! It is **not durable, not HA, not multi-writer-safe across crashes** — the
//! name is a deliberate nod to bogosort. Use it only when you need to take
//! Postgres/CRDB out of the loop for benchmarking.

#![allow(missing_docs)]

pub mod client;
pub mod metrics;
pub mod server;

#[allow(
    clippy::clone_on_ref_ptr,
    clippy::as_conversions,
    clippy::enum_variant_names
)]
pub mod proto {
    //! Generated protobuf types and tonic stubs.
    tonic::include_proto!("mz_bogo_consensus");
}
