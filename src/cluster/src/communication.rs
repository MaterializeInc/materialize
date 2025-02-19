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
//! The startup protocol is as follows: The controller in `environmentd`, after having connected to
//! all the `clusterd` processes in a replica, sends each of them a `CreateTimely` command
//! containing a nonce value (which is the same across all copies of the command). The nonce is
//! guaranteed to be different for each iteration of the compute protocol.
//!
//! In the past, we've seen issues caused by the controller's replica connections flapping
//! repeatedly and causing several instances of the startup code to spin up in short succession (or
//! even simultaneously) in response to different `CreateTimely` commands, causing mass confusion
//! among the processes and possible crash loops. To avoid this, we do not allow processes to
//! connect to each other unless they are responding to a `CreateTimely` command with the same
//! nonce value. If a process connects to a peer with a different nonce, it aborts the connection
//! and retries. Eventually, processes still executing an old iteration of the cluster protocol
//! learn about the controller reconnection and enter the current iteration of the cluster
//! protocol, at which point all inter-process connections succeed.
//!
//! Concretely, each process awaits connections from its peers with higher indices, and initiates
//! connections to those with lower indices. Having established a TCP connection, they exchange
//! nonces, to enable the logic described above.

use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use mz_cluster_client::client::Nonce;
use mz_ore::cast::CastFrom;
use mz_ore::netio::{Listener, Stream};
use timely::communication::allocator::zero_copy::initialize::initialize_networking_from_sockets;
use timely::communication::allocator::GenericBuilder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};

/// Creates communication mesh from cluster config
pub async fn initialize_networking(
    workers: usize,
    my_index: usize,
    addresses: Vec<String>,
    my_nonce: Nonce,
) -> Result<(Vec<GenericBuilder>, Box<dyn Any + Send>), anyhow::Error> {
    info!(my_index, %my_nonce, ?addresses, "initializing network for timely instance");

    let sockets = create_sockets(addresses, u64::cast_from(my_index), my_nonce)
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
        initialize_networking_inner(sockets, my_index, workers)
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
        initialize_networking_inner(sockets, my_index, workers)
    } else {
        anyhow::bail!("cannot mix TCP and Unix streams");
    }
}

fn initialize_networking_inner<S>(
    sockets: Vec<Option<S>>,
    my_index: usize,
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

    match initialize_networking_from_sockets(sockets, my_index, workers, Arc::new(|_| None)) {
        Ok((stuff, guard)) => {
            info!(my_index, "successfully initialized network");
            Ok((
                stuff.into_iter().map(GenericBuilder::ZeroCopy).collect(),
                Box::new(guard),
            ))
        }
        Err(err) => {
            warn!(my_index, "failed to initialize network: {err}");
            Err(anyhow::Error::from(err).context("failed to initialize networking from sockets"))
        }
    }
}

/// Creates socket connections from a list of host addresses.
///
/// The item at index i in the resulting vec, is a Some(TcpSocket) to process i, except
/// for item `my_index` which is None (no socket to self).
async fn create_sockets(
    addresses: Vec<String>,
    my_index: u64,
    my_nonce: Nonce,
) -> Result<Vec<Option<Stream>>, anyhow::Error> {
    let my_index_uz = usize::cast_from(my_index);
    assert!(my_index_uz < addresses.len());
    let n_peers = addresses.len() - 1;
    let mut results: Vec<_> = (0..addresses.len()).map(|_| None).collect();

    let my_address = &addresses[my_index_uz];

    // [btv] Binding to the address (which is of the form
    // `hostname:port`) unnecessarily involves a DNS query. We should
    // get the port from here, but otherwise just bind to `0.0.0.0`.
    // Previously we bound to `my_address`, which caused
    // https://github.com/MaterializeInc/cloud/issues/5070 .
    let bind_address = match regex::Regex::new(r":(\d{1,5})$")
        .unwrap()
        .captures(my_address)
    {
        Some(cap) => {
            let p: u16 = cap[1].parse().context("Port out of range")?;
            format!("0.0.0.0:{p}")
        }
        None => {
            // Address is not of the form `hostname:port`; it's
            // probably a path to a Unix-domain socket.
            my_address.to_string()
        }
    };
    let listener = loop {
        let mut tries = 0;
        match Listener::bind(&bind_address).await {
            Ok(ok) => break ok,
            Err(e) => {
                warn!(my_index, %my_nonce, "failed to listen on address {bind_address}: {e}");
                tries += 1;
                if tries == 10 {
                    return Err(e.into());
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };

    struct ConnectionEstablished {
        peer_index: u64,
        stream: Stream,
    }

    let mut futs = FuturesUnordered::new();
    for i in 0..my_index {
        let address = addresses[usize::cast_from(i)].clone();
        futs.push(
            async move {
                let stream = start_connection(address, my_index, my_nonce).await;
                ConnectionEstablished {
                    peer_index: i,
                    stream,
                }
            }
            .boxed(),
        );
    }

    futs.push({
        let f = async {
            let (stream, peer_index) = await_connection(&listener, my_index, my_nonce).await;
            ConnectionEstablished { peer_index, stream }
        }
        .boxed();
        f
    });

    while results.iter().filter(|maybe| maybe.is_some()).count() != n_peers {
        let ConnectionEstablished { peer_index, stream } = futs
            .next()
            .await
            .expect("we should always at least have a listener task");

        let from_listener = match my_index.cmp(&peer_index) {
            Ordering::Less => true,
            Ordering::Equal => panic!("someone claimed to be us"),
            Ordering::Greater => false,
        };

        if from_listener {
            futs.push({
                let f = async {
                    let (stream, peer_index) =
                        await_connection(&listener, my_index, my_nonce).await;
                    ConnectionEstablished { peer_index, stream }
                }
                .boxed();
                f
            });
        }

        let old = std::mem::replace(&mut results[usize::cast_from(peer_index)], Some(stream));
        if old.is_some() {
            panic!("connected to peer {peer_index} multiple times");
        }
    }

    Ok(results)
}

async fn start_connection(address: String, my_index: u64, my_nonce: Nonce) -> Stream {
    async fn try_connect(
        address: &str,
        my_index: u64,
        my_nonce: Nonce,
    ) -> Result<(Stream, Nonce), anyhow::Error> {
        let mut s = Stream::connect(address).await?;

        if let Stream::Tcp(tcp) = &s {
            tcp.set_nodelay(true)?;
        }

        s.write_all(&my_index.to_be_bytes()).await?;
        s.write_all(&my_nonce.to_bytes()).await?;

        let mut buffer = [0u8; 16];
        s.read_exact(&mut buffer).await?;
        let peer_nonce = Nonce::from_bytes(buffer);

        Ok((s, peer_nonce))
    }

    loop {
        info!(my_index, %my_nonce, "attempting to connect to process at {address}");

        match try_connect(&address, my_index, my_nonce).await {
            Ok((stream, peer_nonce)) => {
                debug!(my_index, %my_nonce, "start: received peer nonce {peer_nonce}");

                if peer_nonce == my_nonce {
                    return stream;
                } else {
                    warn!(
                        my_index, %my_nonce,
                        "peer at address {address} gave mismatching nonce: {peer_nonce}",
                    );
                }
            }
            Err(err) => {
                info!(
                    my_index, %my_nonce,
                    "error connecting to process at {address}: {err}; will retry"
                );
            }
        };

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn await_connection(listener: &Listener, my_index: u64, my_nonce: Nonce) -> (Stream, u64) {
    async fn try_accept(
        listener: &Listener,
        my_nonce: Nonce,
    ) -> Result<(Stream, u64, Nonce), anyhow::Error> {
        let mut s = listener.accept().await?.0;

        if let Stream::Tcp(tcp) = &s {
            tcp.set_nodelay(true)?;
        }

        let mut buffer = [0u8; 16];
        s.read_exact(&mut buffer[0..8]).await?;
        let peer_index = u64::from_be_bytes((&buffer[0..8]).try_into().unwrap());

        s.read_exact(&mut buffer).await?;
        let peer_nonce = Nonce::from_bytes(buffer);

        s.write_all(&my_nonce.to_bytes()[..]).await?;

        Ok((s, peer_index, peer_nonce))
    }

    loop {
        info!(my_index, %my_nonce, "awaiting connection from peer");

        match try_accept(listener, my_nonce).await {
            Ok((stream, peer_index, peer_nonce)) => {
                debug!(
                    my_index, %my_nonce,
                    "await: received peer (index, nonce): ({peer_index}, {peer_nonce})",
                );

                if peer_nonce == my_nonce {
                    return (stream, peer_index);
                } else {
                    warn!(
                        my_index, %my_nonce,
                        "peer {peer_index} gave mismatching nonce: {peer_nonce}",
                    );
                }
            }
            Err(err) => {
                info!(my_index, %my_nonce, "error accepting connection: {err}; will retry");
            }
        }
    }
}
