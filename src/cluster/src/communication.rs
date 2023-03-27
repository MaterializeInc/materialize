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
use std::fmt::Display;
use std::time::Duration;

use anyhow::Context;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use timely::communication::allocator::zero_copy::initialize::initialize_networking_from_sockets;
use timely::communication::allocator::GenericBuilder;
use tracing::{debug, info, warn};

use mz_cluster_client::client::ClusterStartupEpoch;
use mz_ore::cast::CastFrom;
use mz_ore::netio::{Listener, Stream};

/// Creates communication mesh from cluster config
pub async fn initialize_networking(
    workers: usize,
    process: usize,
    addresses: Vec<String>,
    epoch: ClusterStartupEpoch,
) -> Result<(Vec<GenericBuilder>, Box<dyn Any + Send>), anyhow::Error> {
    info!(
        process = process,
        ?addresses,
        "initializing network for timely instance, with {} processes for epoch number {epoch}",
        addresses.len()
    );
    let sockets = create_sockets(addresses, u64::cast_from(process), epoch)
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

    match initialize_networking_from_sockets(sockets, process, workers, Box::new(|_| None)) {
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
async fn create_sockets(
    addresses: Vec<String>,
    my_index: u64,
    my_epoch: ClusterStartupEpoch,
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
                warn!("failed to listen on address {bind_address}: {e}");
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
                start_connection(address, my_index, my_epoch)
                    .await
                    .map(move |stream| ConnectionEstablished {
                        peer_index: i,
                        stream,
                    })
            }
            .boxed(),
        );
    }

    futs.push({
        let f = async {
            await_connection(&listener, my_index, my_epoch)
                .await
                .map(|(stream, peer_index)| ConnectionEstablished { peer_index, stream })
        }
        .boxed();
        f
    });

    while results.iter().filter(|maybe| maybe.is_some()).count() != n_peers {
        let ConnectionEstablished { peer_index, stream } = futs
            .next()
            .await
            .expect("we should always at least have a listener task")?;

        let from_listener = match my_index.cmp(&peer_index) {
            Ordering::Less => true,
            Ordering::Equal => panic!("someone claimed to be us"),
            Ordering::Greater => false,
        };

        if from_listener {
            futs.push({
                let f = async {
                    await_connection(&listener, my_index, my_epoch)
                        .await
                        .map(|(stream, peer_index)| ConnectionEstablished { peer_index, stream })
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

#[derive(Debug)]
/// This task can never successfully boot, since
/// a peer has seen a higher epoch from `environmentd`.
pub struct EpochMismatch {
    /// The epoch we know about
    mine: ClusterStartupEpoch,
    /// The higher epoch from our peer
    peer: ClusterStartupEpoch,
}

impl Display for EpochMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let EpochMismatch { mine, peer } = self;
        write!(f, "epoch mismatch: ours was {mine}; the peer's was {peer}")
    }
}

impl std::error::Error for EpochMismatch {}

async fn start_connection(
    address: String,
    my_index: u64,
    my_epoch: ClusterStartupEpoch,
) -> Result<Stream, anyhow::Error> {
    loop {
        info!(
            process = my_index,
            "Attempting to connect to process at {address}"
        );

        match Stream::connect(&address).await {
            Ok(mut s) => {
                if let Stream::Tcp(tcp) = &s {
                    tcp.set_nodelay(true)?;
                }
                use tokio::io::AsyncWriteExt;

                s.write_all(&my_index.to_be_bytes()).await?;

                s.write_all(&my_epoch.to_bytes()).await?;

                let mut buffer = [0u8; 16];
                use tokio::io::AsyncReadExt;
                s.read_exact(&mut buffer).await?;
                let peer_epoch = ClusterStartupEpoch::from_bytes(buffer);
                debug!("start: received peer epoch {peer_epoch}");

                match my_epoch.cmp(&peer_epoch) {
                    Ordering::Less => {
                        return Err(EpochMismatch {
                            mine: my_epoch,
                            peer: peer_epoch,
                        }
                        .into());
                    }
                    Ordering::Greater => {
                        warn!(
                            process = my_index,
                            "peer at address {address} gave older epoch ({peer_epoch}) than ours ({my_epoch})"
                        );
                    }
                    Ordering::Equal => return Ok(s),
                }
            }
            Err(err) => {
                info!(
                    process = my_index,
                    "error connecting to process at {address}: {err}; will retry"
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn await_connection(
    listener: &Listener,
    my_index: u64, // only for logging
    my_epoch: ClusterStartupEpoch,
) -> Result<(Stream, u64), anyhow::Error> {
    loop {
        info!(process = my_index, "awaiting connection from peer");
        let mut s = listener.accept().await?.0;
        info!(process = my_index, "accepted connection from peer");
        if let Stream::Tcp(tcp) = &s {
            tcp.set_nodelay(true)?;
        }

        let mut buffer = [0u8; 16];
        use tokio::io::AsyncReadExt;

        s.read_exact(&mut buffer[0..8]).await?;
        let peer_index = u64::from_be_bytes((&buffer[0..8]).try_into().unwrap());
        debug!("await: received peer index {peer_index}");

        s.read_exact(&mut buffer).await?;
        let peer_epoch = ClusterStartupEpoch::from_bytes(buffer);
        debug!("await: received peer epoch {peer_epoch}");

        use tokio::io::AsyncWriteExt;
        s.write_all(&my_epoch.to_bytes()[..]).await?;

        match my_epoch.cmp(&peer_epoch) {
            Ordering::Less => {
                return Err(EpochMismatch {
                    mine: my_epoch,
                    peer: peer_epoch,
                }
                .into());
            }
            Ordering::Greater => {
                warn!(
                    process = my_index,
                    "peer {peer_index} gave older epoch ({peer_epoch}) than ours ({my_epoch})"
                );
            }
            Ordering::Equal => return Ok((s, peer_index)),
        }
    }
}
