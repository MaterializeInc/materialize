// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to spin up communication mesh for a cluster replica.
//!
//! The startup protocol is as follows:
//! The controller in `environmentd`, after having connected to all the
//! `clusterd` processes in a replica, sends each of them a `CreateTimely` command
//! containing an epoch value (which is the same across all copies of the command).
//! The meaning of this value is irrelevant,
//! as long as it is totally ordered and
//! increases monotonically (including across `environmentd` restarts)
//!
//! In the past, we've seen issues caused by `environmentd`'s replica connections
//! flapping repeatedly and causing several instances of the startup code to spin up
//! in short succession (or even simultaneously) in response to different `CreateTimely`
//! commands, causing mass confusion among the processes
//! and possible crash loops. To avoid this, we do not allow processes to connect to each
//! other unless they are responding to a `CreateTimely` command with the same epoch value.
//! If a process discovers the existence of a peer with a lower epoch value, it ignores it,
//! and if it discovers one with a higher epoch value, it aborts the connection.
//! Such a process is guaranteed to eventually hear about the higher epoch value
//! (and, thus, successfully connect to its peers), since
//! `environmentd` sends `CreateTimely` commands to all processes in a replica.
//!
//! Concretely, each process awaits connections from its peers with higher indices,
//! and initiates connections to those with lower indices. Having established
//! a TCP connection, they exchange epochs, to enable the logic described above.

use std::any::Any;
use std::cmp::Ordering;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::TryFutureExt;
use mz_ore::netio::{Listener, SocketAddr, Stream};
use mz_ore::retry::Retry;
use timely::communication::allocator::zero_copy::initialize::initialize_networking_from_sockets;
use timely::communication::allocator::GenericBuilder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn};

/// Creates communication mesh from cluster config
pub async fn initialize_networking(
    workers: usize,
    bind_address: String,
    peer_addresses: Vec<String>,
) -> Result<(Vec<GenericBuilder>, Box<dyn Any + Send>), anyhow::Error> {
    info!(
        ?workers,
        ?bind_address,
        ?peer_addresses,
        "initializing network for timely instance"
    );
    let (process, sockets) = create_sockets(bind_address, peer_addresses)
        .await
        .context("failed to set up timely sockets")?;

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
        initialize_networking_inner(sockets, process, workers)
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
        initialize_networking_inner(sockets, process, workers)
    } else {
        anyhow::bail!("cannot mix TCP and Unix streams");
    }
}

fn initialize_networking_inner<S>(
    sockets: Vec<Option<S>>,
    process: usize,
    workers: usize,
) -> Result<(Vec<GenericBuilder>, Box<dyn Any + Send>), anyhow::Error>
where
    S: timely::communication::allocator::zero_copy::stream::Stream + 'static,
{
    for s in &sockets {
        if let Some(s) = s {
            s.set_nonblocking(false)
                .context("failed to set socket to non-blocking")?;
        }
    }

    match initialize_networking_from_sockets(sockets, process, workers, Arc::new(|_| None)) {
        Ok((stuff, guard)) => {
            info!(process = process, "successfully initialized network");
            Ok((
                stuff.into_iter().map(GenericBuilder::ZeroCopy).collect(),
                Box::new(guard),
            ))
        }
        Err(err) => {
            warn!(process, "failed to initialize network: {err}");
            Err(anyhow::Error::from(err).context("failed to initialize networking from sockets"))
        }
    }
}

/// Creates socket connections from a list of host addresses.
///
/// The item at index i in the resulting vec, is a Some(TcpSocket) to process i, except
/// for item `my_index` which is None (no socket to self).
///
// FIXME: make this robust against peers crashing
async fn create_sockets(
    bind_address: String,
    peer_addresses: Vec<String>,
) -> Result<(usize, Vec<Option<Stream>>), anyhow::Error> {
    let peers = peer_addresses.len();

    // If `bind_address` contains a hostname, replace it with `0.0.0.0`, to avoid a DNS lookup and
    // prevent issues such as cloud#5070.
    let bind_address = match SocketAddr::from_str(&bind_address)? {
        SocketAddr::Inet(mut addr) => {
            addr.set_ip([0, 0, 0, 0].into());
            SocketAddr::Inet(addr)
        }
        addr @ SocketAddr::Unix(_) => addr,
    };

    let listener = Retry::default()
        .initial_backoff(Duration::from_secs(1))
        .clamp_backoff(Duration::from_secs(1))
        .max_tries(10)
        .retry_async(|_| {
            Listener::bind(&bind_address)
                .inspect_err(|error| warn!(%bind_address, "failed to listen: {error}"))
        })
        .await?;

    // FIXME: this won't work with hostnames
    let listen_addr = listener.local_addr().unwrap().to_string();
    let my_index = peer_addresses
        .iter()
        .position(|s| *s == listen_addr)
        .unwrap();
    info!("listen at: {listen_addr}, my_index = {my_index}");

    let accept = accept_peer_connections(listener, peer_addresses.len());
    let connect = start_peer_connections(my_index, peer_addresses);
    let (mut accepted, mut connected) = futures::try_join!(accept, connect)?;

    let streams = (0..peers)
        .map(|peer| match peer.cmp(&my_index) {
            Ordering::Less => Some(connected.remove(peer)),
            Ordering::Equal => None,
            Ordering::Greater => Some(accepted.remove(peer)),
        })
        .collect();

    Ok((my_index, streams))
}

async fn start_peer_connections(
    my_index: usize,
    peer_addresses: Vec<String>,
) -> Result<Vec<Stream>, anyhow::Error> {
    async fn try_connect(my_index: usize, address: &str) -> Result<Stream, anyhow::Error> {
        let mut s = Stream::connect(address).await?;
        if let Stream::Tcp(tcp) = &s {
            tcp.set_nodelay(true)?;
        }

        s.write_all(&my_index.to_be_bytes()).await?;

        Ok(s)
    }

    let mut streams = Vec::new();
    for (peer, address) in peer_addresses.iter().enumerate() {
        info!(peer, address, "attempting to connect to process");

        let stream = Retry::default()
            .initial_backoff(Duration::from_secs(1))
            .clamp_backoff(Duration::from_secs(1))
            .retry_async(|_| {
                try_connect(my_index, address).inspect_err(|error| {
                    info!(peer, address, "failed to connect: {error}; retrying");
                })
            })
            .await?;

        streams.push(stream);

        info!(peer, "connected to process");
    }

    Ok(streams)
}

async fn accept_peer_connections(
    listener: Listener,
    peers: usize,
) -> Result<Vec<Stream>, anyhow::Error> {
    async fn try_accept(listener: &Listener) -> Result<(usize, Stream), anyhow::Error> {
        let (mut s, _) = listener.accept().await?;
        if let Stream::Tcp(tcp) = &s {
            tcp.set_nodelay(true)?;
        }

        let mut buf = [0; 8];
        s.read_exact(&mut buf).await?;
        let index = usize::from_be_bytes(buf);

        Ok((index, s))
    }

    let mut streams = Vec::new();
    for _ in 0..peers {
        streams.push(None);
    }

    while streams.iter().any(|s| s.is_none()) {
        info!("accepting a connection");

        let (peer, stream) = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .clamp_backoff(Duration::from_millis(1))
            .retry_async(|_| {
                try_accept(&listener).inspect_err(|error| {
                    info!("failed to accept: {error}; retrying");
                })
            })
            .await?;

        streams[peer] = Some(stream);
    }

    let streams = streams.into_iter().map(|s| s.unwrap()).collect();
    Ok(streams)
}
