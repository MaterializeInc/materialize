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
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use mz_cluster_client::client::ClusterStartupEpoch;
use mz_ore::cast::CastFrom;
use mz_ore::netio::{Listener, SocketAddr, Stream};
use timely::communication::allocator::zero_copy::initialize::initialize_networking_from_sockets;
use timely::communication::allocator::GenericBuilder;
use tracing::{debug, info, warn};

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
    let bind_address = match my_address.parse()? {
        SocketAddr::Inet(mut addr) => {
            addr.set_ip("0.0.0.0".parse().unwrap());
            SocketAddr::Inet(addr)
        }
        SocketAddr::Turmoil(mut addr) => {
            addr.set_ip("0.0.0.0".parse().unwrap());
            SocketAddr::Turmoil(addr)
        }
        addr => addr,
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

#[cfg(test)]
mod turmoil_tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::num::NonZeroI64;
    use std::rc::Rc;
    use std::sync::Once;

    use rand::rngs::SmallRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use tokio::sync::mpsc;

    use super::*;

    #[derive(Clone, Debug)]
    struct Process {
        name: String,
        command_tx: mpsc::UnboundedSender<Command>,
    }

    impl Process {
        async fn connect(&self, addresses: Vec<String>, epoch: ClusterStartupEpoch) {
            info!("connecting to process {} at epoch {}", self.name, epoch);
            self.command_tx.send(Command { addresses, epoch }).unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[derive(Clone)]
    struct Command {
        addresses: Vec<String>,
        epoch: ClusterStartupEpoch,
    }

    struct Response {
        process: u64,
        result: Result<Vec<Option<Stream>>, anyhow::Error>,
        epoch: ClusterStartupEpoch,
    }

    /// Run a chaos test for `create_sockets`.
    ///
    /// This test spawns a number of processes and makes them attempt to create connections among
    /// each other, simulates multiple client reconnects in the process.
    #[test]
    fn create_sockets_chaos() {
        const NUM_PROCESSES: u64 = 3;
        const NUM_RECONNECTS: u64 = 3;

        configure_tracing();

        let seed = std::env::var("SEED")
            .ok()
            .and_then(|x| x.parse().ok())
            .unwrap_or_else(|| rand::random());

        info!("initializing rng with seed {seed}");
        let mut rng = SmallRng::seed_from_u64(seed);

        let mut sim = turmoil::Builder::new()
            .enable_random_order()
            .build_with_rng(Box::new(rng.clone()));

        let (response_tx, mut response_rx) = mpsc::unbounded_channel();

        let mut processes = Vec::new();
        for index in 0..NUM_PROCESSES {
            let name = format!("process-{index}");
            let (command_tx, command_rx) = mpsc::unbounded_channel();
            let command_rx = Rc::new(RefCell::new(command_rx));
            let response_tx = response_tx.clone();

            processes.push(Process {
                name: name.clone(),
                command_tx,
            });

            sim.host(&name[..], move || {
                let command_rx = Rc::clone(&command_rx);
                let response_tx = response_tx.clone();

                async move {
                    let mut command_rx = command_rx.borrow_mut();

                    let Some(mut command) = command_rx.recv().await else {
                        return Err("no initial command received".to_string().into());
                    };

                    loop {
                        let Command { addresses, epoch } = command;
                        info!("creating sockets with epoch: {}", epoch);

                        // Run `create_sockets` with the given arguments until it returns or gets
                        // canceled by a new client connection.
                        let result = tokio::select! {
                            result = create_sockets(addresses, index, epoch) => result,
                            new_command = command_rx.recv() => match new_command {
                                Some(cmd) => {
                                    command = cmd;
                                    continue;
                                }
                                None => break,
                            }
                        };

                        match &result {
                            Ok(_) => info!("sockets successfully created"),
                            Err(e) => info!("socket creation failed: {e}"),
                        };

                        let _ = response_tx.send(Response {
                            process: index,
                            result,
                            epoch: command.epoch,
                        });

                        match command_rx.recv().await {
                            Some(cmd) => command = cmd,
                            None => break,
                        }
                    }

                    info!("client disconnected; shutting down");
                    Ok(())
                }
            });
        }

        let addresses: Vec<_> = processes
            .iter()
            .map(|p| {
                let ip = sim.lookup(&p.name[..]);
                format!("turmoil://{ip}:7777")
            })
            .collect();

        sim.client("client", async move {
            let mut processes2 = processes.clone();
            processes2.shuffle(&mut rng);

            let mut epoch = ClusterStartupEpoch::new(NonZeroI64::new(1).unwrap(), 1);
            let target_epoch =
                ClusterStartupEpoch::new(NonZeroI64::new(1).unwrap(), NUM_RECONNECTS);

            // Bump one process to the target epoch immediately, to ensure socket creation will
            // only succeed at that epoch.
            let proc = processes2.pop().unwrap();
            proc.connect(addresses.clone(), target_epoch).await;

            while epoch < target_epoch {
                // Reconnect to a random subset of processes, in random order.
                let amount = rng.gen_range(0..processes2.len());
                let mut chosen: Vec<_> = processes2.choose_multiple(&mut rng, amount).collect();
                chosen.shuffle(&mut rng);

                for proc in &chosen {
                    proc.connect(addresses.clone(), epoch).await;
                }

                epoch.bump_replica();
            }

            // Send the target epoch to all processes.
            for proc in &processes2 {
                proc.connect(addresses.clone(), epoch).await;
            }

            // Processes should either end up connected at the target epoch or produce an error. If
            // any process produces an error, we retry. Eventually we expect to end up with all
            // processes connected.
            let mut epoch = target_epoch;
            loop {
                // Keep the sockets around while waiting for processes, to ensure dropping the
                // doesn't cause connection failures.
                let mut sockets = BTreeMap::new();
                while sockets.len() < usize::cast_from(NUM_PROCESSES) {
                    let resp = response_rx.recv().await.unwrap();

                    if resp.epoch != epoch {
                        assert!(resp.epoch < epoch);
                        continue;
                    }

                    if let Ok(s) = resp.result {
                        sockets.insert(resp.process, s);
                    } else {
                        break;
                    }
                }

                if sockets.len() == usize::cast_from(NUM_PROCESSES) {
                    break;
                }

                epoch.bump_replica();
                info!("some processes returned errors; retrying at epoch {epoch}");

                for proc in &processes {
                    proc.connect(addresses.clone(), epoch).await;
                }
            }

            Ok(())
        });

        sim.run().unwrap();
    }

    /// Fuzz test for `create_sockets`.
    ///
    /// Ignored by default because the test never terminates unless it fails.
    #[test]
    #[ignore]
    fn fuzz_create_sockets() {
        loop {
            create_sockets_chaos();
        }
    }

    /// Configure tracing for the turmoil tests.
    ///
    /// Log events are written to stdout and include the logical time of the simulation.
    fn configure_tracing() {
        use tracing::level_filters::LevelFilter;
        use tracing_subscriber::fmt::time::FormatTime;

        #[derive(Clone)]
        struct SimElapsedTime;

        impl FormatTime for SimElapsedTime {
            fn format_time(
                &self,
                w: &mut tracing_subscriber::fmt::format::Writer<'_>,
            ) -> std::fmt::Result {
                tracing_subscriber::fmt::time().format_time(w)?;
                if let Some(sim_elapsed) = turmoil::sim_elapsed() {
                    write!(w, " [{:?}]", sim_elapsed)?;
                }
                Ok(())
            }
        }

        static INIT_TRACING: Once = Once::new();
        INIT_TRACING.call_once(|| {
            let env_filter = tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy();
            let subscriber = tracing_subscriber::fmt()
                .with_test_writer()
                .with_env_filter(env_filter)
                .with_timer(SimElapsedTime)
                .finish();

            tracing::subscriber::set_global_default(subscriber).unwrap();
        });
    }
}
