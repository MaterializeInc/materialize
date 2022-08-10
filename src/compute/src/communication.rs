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
use tracing::info;

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

    info!(worker = my_index, "initialization complete");

    Ok(results)
}

/// Result contains connections [0, my_index - 1].
fn start_connections(
    addresses: Arc<Vec<String>>,
    my_index: usize,
) -> Result<Vec<Option<TcpStream>>, io::Error> {
    let results = addresses
        .iter()
        .take(my_index)
        .enumerate()
        .map(|(index, address)| loop {
            match TcpStream::connect(address) {
                Ok(mut stream) => {
                    stream.set_nodelay(true).expect("set_nodelay call failed");
                    stream
                        .write_all(&my_index.to_ne_bytes())
                        .expect("failed to send worker index");
                    info!(worker = my_index, "connection to worker {}", index);
                    break Some(stream);
                }
                Err(error) => {
                    info!(
                        worker = my_index,
                        "error connecting to worker {index}: {error}; retrying"
                    );
                    sleep(Duration::from_secs(1));
                }
            }
        })
        .collect();

    Ok(results)
}

/// Result contains connections [my_index + 1, addresses.len() - 1].
pub fn await_connections(
    addresses: Arc<Vec<String>>,
    my_index: usize,
) -> Result<Vec<Option<TcpStream>>, io::Error> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index - 1))
        .map(|_| None)
        .collect();
    let listener = TcpListener::bind(&addresses[my_index][..])?;

    for _ in (my_index + 1)..addresses.len() {
        let mut stream = listener.accept()?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");
        let mut buffer = [0u8; 8];
        stream.read_exact(&mut buffer)?;
        let identifier = u64::from_ne_bytes((&buffer[..]).try_into().unwrap());
        results[identifier as usize - my_index - 1] = Some(stream);
        info!(worker = my_index, "connection from worker {}", identifier);
    }

    Ok(results)
}
