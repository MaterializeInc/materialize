// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An adaptor to Jepsen Maelstrom's txn-list-append workload

use mz_ore::{task::RuntimeExt, url::SensitiveUrl};
use tokio::runtime::Handle;
use tracing::Span;

use crate::maelstrom::node::Service;
pub mod api;
pub mod node;
pub mod services;
pub mod txn_list_append_multi;
pub mod txn_list_append_single;

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
    blob_uri: Option<SensitiveUrl>,

    /// Consensus to use, defaults to Maelstrom lin-kv service
    #[clap(long)]
    consensus_uri: Option<SensitiveUrl>,

    /// How much unreliability to inject into Blob and Consensus usage
    ///
    /// This value must be in [0, 1]. 0 means no-op, 1 means complete
    /// unreliability.
    #[clap(long, default_value_t = 0.05)]
    unreliability: f64,
}

pub async fn run<S: Service + 'static>(args: Args) -> Result<(), anyhow::Error> {
    // Persist internally has a bunch of sanity check assertions. If
    // maelstrom tickles one of these, we very much want to bubble this
    // up into a process exit with non-0 status. It's surprisingly
    // tricky to be confident that we're not accidentally swallowing
    // panics in async tasks (in fact there was a bug that did exactly
    // this at one point), so abort on any panics to be extra sure.
    mz_ore::panic::install_enhanced_handler();

    // Run the maelstrom stuff in a spawn_blocking because it internally
    // spawns tasks, so the runtime needs to be in the TLC.
    Handle::current()
        .spawn_blocking_named(
            || "maelstrom::run",
            move || {
                Span::current().in_scope(|| {
                    let read = std::io::stdin();
                    let write = std::io::stdout();
                    crate::maelstrom::node::run::<_, _, S>(args, read.lock(), write)
                })
            },
        )
        .await
}
