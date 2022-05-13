// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An adaptor to Jepsen Maelstrom's txn-list-append workload
pub mod api;
pub mod node;
pub mod services;
pub mod txn;

/// An adaptor to Jepsen Maelstrom's txn-list-append workload
///
/// Example usage:
///
///     cargo build --example persistcli && java -jar /path/to/maelstrom.jar test -w txn-list-append --bin ./target/debug/examples/persistcli maelstrom
///
/// The [Maelstrom docs] are a great place to start for an understanding of
/// the specifics of what's going on here.
///
/// [maelstrom docs]: https://github.com/jepsen-io/maelstrom/tree/v0.2.1#documentation
#[derive(Debug, Clone, clap::Parser)]
pub struct Args {
    /// Blob to use, defaults to Maelstrom lin-kv service
    #[clap(long)]
    blob_uri: Option<String>,

    /// Consensus to use, defaults to Maelstrom lin-kv service
    #[clap(long)]
    consensus_uri: Option<String>,

    /// How much unreliability to inject into Blob and Consensus usage
    ///
    /// This value must be in [0, 1]. 0 means no-op, 1 means complete
    /// unreliability.
    #[clap(long, default_value_t = 0.05)]
    unreliability: f64,
}
