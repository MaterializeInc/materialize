// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::{Context, bail};
use timely::communication::allocator::zero_copy::allocator::TcpBuilder;
use timely::communication::allocator::zero_copy::bytes_slab::BytesRefill;
use timely::communication::allocator::zero_copy::initialize::initialize_networking_from_sockets;
use timely::communication::allocator::{GenericBuilder, PeerBuilder};
use tracing::{info, warn};

use crate::communication_v2::create_sockets;

/// Creates communication mesh from cluster config
pub async fn initialize_networking<P>(
    workers: usize,
    process: usize,
    addresses: Vec<String>,
    refill: BytesRefill,
    builder_fn: impl Fn(TcpBuilder<P::Peer>) -> GenericBuilder,
) -> Result<(Vec<GenericBuilder>, Box<dyn Any + Send>), anyhow::Error>
where
    P: PeerBuilder,
{
    info!(
        process,
        ?addresses,
        "initializing network for timely instance",
    );
    let sockets = loop {
        match create_sockets(process, &addresses).await {
            Ok(sockets) => break sockets,
            Err(error) if error.is_fatal() => bail!("failed to set up Timely sockets: {error}"),
            Err(error) => info!("creating sockets failed: {error}; retrying"),
        }
    };

    if sockets
        .iter()
        .filter_map(|s| s.as_ref())
        .all(|s| s.is_tcp())
    {
        let sockets = sockets
            .into_iter()
            .map(|s| s.map(|s| s.unwrap_tcp().into_std()).transpose())
            .collect::<Result<Vec<_>, _>>()
            .map_err(anyhow::Error::from)
            .context("failed to get standard sockets from tokio sockets")?;
        initialize_networking_inner::<_, P, _>(sockets, process, workers, refill, builder_fn)
    } else if sockets
        .iter()
        .filter_map(|s| s.as_ref())
        .all(|s| s.is_unix())
    {
        let sockets = sockets
            .into_iter()
            .map(|s| s.map(|s| s.unwrap_unix().into_std()).transpose())
            .collect::<Result<Vec<_>, _>>()
            .map_err(anyhow::Error::from)
            .context("failed to get standard sockets from tokio sockets")?;
        initialize_networking_inner::<_, P, _>(sockets, process, workers, refill, builder_fn)
    } else {
        anyhow::bail!("cannot mix TCP and Unix streams");
    }
}

fn initialize_networking_inner<S, P, PF>(
    sockets: Vec<Option<S>>,
    process: usize,
    workers: usize,
    refill: BytesRefill,
    builder_fn: PF,
) -> Result<(Vec<GenericBuilder>, Box<dyn Any + Send>), anyhow::Error>
where
    S: timely::communication::allocator::zero_copy::stream::Stream + 'static,
    P: PeerBuilder,
    PF: Fn(TcpBuilder<P::Peer>) -> GenericBuilder,
{
    for s in &sockets {
        if let Some(s) = s {
            s.set_nonblocking(false)
                .context("failed to set socket to non-blocking")?;
        }
    }

    match initialize_networking_from_sockets::<_, P>(
        sockets,
        process,
        workers,
        refill,
        Arc::new(|_| None),
    ) {
        Ok((stuff, guard)) => {
            info!(process = process, "successfully initialized network");
            Ok((stuff.into_iter().map(builder_fn).collect(), Box::new(guard)))
        }
        Err(err) => {
            warn!(process, "failed to initialize network: {err}");
            Err(anyhow::Error::from(err).context("failed to initialize networking from sockets"))
        }
    }
}
