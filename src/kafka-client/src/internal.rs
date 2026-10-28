// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Crate-internal machinery behind the public facade.
//!
//! * [`runtime`]: the process-wide `kafka-io` runtime and the sync/async
//!   bridges.
//! * [`hooks`]: type-erased access to the user's context, and log filtering.
//! * [`cluster`]: connections, bootstrap, metadata cache, broker lookup.
//! * [`consumer`], [`producer`], [`admin`]: the client engines.
//! * [`records`], [`compression`], [`partitioner`]: the record batch codec and
//!   partitioner hashing.

pub(crate) mod admin;
pub(crate) mod cluster;
pub(crate) mod compression;
pub(crate) mod consumer;
pub(crate) mod hooks;
pub(crate) mod partitioner;
pub(crate) mod producer;
pub(crate) mod records;
pub(crate) mod runtime;
// pub(crate) mod net;
// pub(crate) mod protocol;
