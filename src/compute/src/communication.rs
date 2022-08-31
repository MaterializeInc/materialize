// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to spin up communication mesh for a compute replica.
//! Most of this logic was taken from Timely's communication::networking
//! module, with the crucial difference that we
//! actually verify that we can receive data on a connection before considering it
//! established, and retry otherwise. This allows us to cope with the phenomenon
//! of spuriously accepted connections that has been observed in Kubernetes with linkerd.

use std::any::Any;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use timely::communication::allocator::zero_copy::initialize::initialize_networking_from_sockets;
use timely::communication::allocator::GenericBuilder;
use tracing::{info, trace, warn};

use crate::server::CommunicationConfig;

/// Creates communication mesh from cluster config
pub fn initialize_networking(
    config: CommunicationConfig,
) -> Result<(Vec<GenericBuilder>, Box<dyn Any + Send>), String> {
    let CommunicationConfig {
        threads,
        process,
        addresses,
    } = config;
    let sockets_result = create_sockets(addresses, process);
    match sockets_result.and_then(|sockets| {
        initialize_networking_from_sockets(sockets, process, threads, Box::new(|_| None))
    }) {
        Ok((stuff, guard)) => Ok((
            stuff.into_iter().map(GenericBuilder::ZeroCopy).collect(),
            Box::new(guard),
        )),
        Err(err) => Err(format!("failed to initialize networking: {err}")),
    }
}

/// Creates socket connections from a list of host addresses.
///
/// The item at index i in the resulting vec, is a Some(TcpSocket) to process i, except
/// for item `my_index` which is None (no socket to self).
fn create_sockets(
    addresses: Vec<String>,
    my_index: usize,
) -> Result<Vec<Option<TcpStream>>, io::Error> {
    let hosts1 = Arc::new(addresses);
    let hosts2 = Arc::clone(&hosts1);

    let start_task = thread::spawn(move || start_connections(hosts1, my_index));
    let await_task = thread::spawn(move || await_connections(hosts2, my_index));

    let mut results = start_task.join().unwrap()?;
    results.push(None);
    let to_extend = await_task.join().unwrap()?;
    results.extend(to_extend.into_iter());

    for r in &mut results {
        if let Some(sock) = r.as_mut() {
            sock.set_nonblocking(false)
                .expect("set_nonblocking(false) call failed");
        }
    }

    info!(worker = my_index, "initialization complete");

    Ok(results)
}

/// Garbage-collect broken connections.
///
/// Detect if any connection has gone away, by doing a short peek.
/// Assuming the sockets are nonblocking, this should immediately either return data,
/// or fail with EAGAIN or EWOULDBLOCK.
///
/// If an EOF or an error other than those two is signaled, the connection is broken
/// and should be set to `None`.
///
/// `first_idx` is used only for error messages; it does not impact behavior otherwise.
fn gc_broken_connections<'a, I>(conns: I, first_idx: usize)
where
    I: IntoIterator<Item = &'a mut Option<TcpStream>>,
{
    let mut buf = [0];
    for (i, maybe_conn) in conns.into_iter().enumerate() {
        trace!("peeking {} to detect whether it's broken", first_idx + i);
        if let Some(conn) = maybe_conn {
            let closed = match conn.peek(&mut buf) {
                Ok(0) => true, // EOF
                Ok(_) => false,
                Err(err) => err.kind() != std::io::ErrorKind::WouldBlock,
            };
            if closed {
                warn!(
                    "Connection {} has gone down; we will try to establish it again.",
                    i + first_idx
                );
                *maybe_conn = None;
            } else {
                trace!("Peek OK")
            }
        }
    }
}

/// Result contains connections [0, my_index - 1].
fn start_connections(
    addresses: Arc<Vec<String>>,
    my_index: usize,
) -> Result<Vec<Option<TcpStream>>, io::Error> {
    let addresses: Vec<_> = addresses.iter().take(my_index).cloned().collect();
    let mut results: Vec<_> = (0..my_index).map(|_| None).collect();

    // We do not want to provide opportunities for the startup
    // sequence to hang waiting for one of its peers, not noticing that another has gone down;
    // empirically, we have found it difficult to reason
    // about whether this results in crash loops and global deadlocks.
    // Thus, in between connection attempts, check whether a previously-established connection has
    // gone away; if so, we clear it and retry it again later.
    while let Some(i) = results.iter().position(|r| r.is_none()) {
        match TcpStream::connect(&addresses[i]) {
            Ok(mut s) => {
                s.set_nodelay(true).expect("set_nodelay call failed");

                s.write_all(&my_index.to_ne_bytes())
                    .expect("failed to send worker index");

                // This is necessary for `gc_broken_connections`; it will be unset
                // before actually trying to use the sockets.
                s.set_nonblocking(true)
                    .expect("set_nonblocking(true) call failed");

                info!(worker = my_index, "Connected to process {i}");
                results[i] = Some(s);
            }
            Err(err) => {
                info!(
                    worker = my_index,
                    "error connecting to process {i}: {err}; will retry"
                );
                sleep(Duration::from_secs(1));
            }
        }
        // If a peer failed, it's better that we detect it now than spin up Timely
        // and immediately crash.
        gc_broken_connections(&mut results, 0);
    }

    Ok(results)
}

/// Result contains connections [my_index + 1, addresses.len() - 1].
fn await_connections(
    addresses: Arc<Vec<String>>,
    my_index: usize,
) -> Result<Vec<Option<TcpStream>>, io::Error> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index - 1))
        .map(|_| None)
        .collect();
    let listener = TcpListener::bind(&addresses[my_index][..])?;

    while results.iter().any(|r| r.is_none()) {
        let mut stream = listener.accept()?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");
        let mut buffer = [0u8; 8];
        stream.read_exact(&mut buffer)?;
        let identifier: usize = u64::from_ne_bytes((&buffer[..]).try_into().unwrap())
            .try_into()
            .expect("Materialize must run on a 64-bit system");
        // This is necessary for `gc_broken_connections`; it will be unset
        // before actually trying to use the sockets.
        stream
            .set_nonblocking(true)
            .expect("set_nonblocking(true) call failed");
        assert!(identifier >= (my_index + 1));
        assert!(identifier < addresses.len());
        if results[identifier - my_index - 1].is_some() {
            warn!(worker = my_index, "New incarnation of peer {identifier}");
        }
        results[identifier - my_index - 1] = Some(stream);
        info!(worker = my_index, "connection from process {}", identifier);

        // If a peer failed, it's better that we detect it now than spin up Timely
        // and immediately crash.
        gc_broken_connections(&mut results, my_index + 1);
    }

    Ok(results)
}
