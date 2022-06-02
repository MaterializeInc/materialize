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
use std::io::{self, Write};
use std::io::{Read, Result as IoResult};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use timely::communication::allocator::zero_copy::initialize::initialize_networking_from_sockets;
use timely::communication::allocator::GenericBuilder;
use timely::communication::{initialize_from, WorkerGuards};
use timely::worker::Worker;
use timely::{CommunicationConfig, Config};

// This constant is sent along immediately after establishing a TCP stream, so
// that it is easy to sniff out Timely traffic when it is multiplexed with
// other traffic on the same port.
const HANDSHAKE_MAGIC: u64 = 0xc2f1fb770118add9;

// This constant is sent upon receiving a connection. The process establishing
// the connection must receive it in order to consider the connection to be
// successfully established.
const ACK_MAGIC: u64 = 0x68656c6f77726c64;

/// Fork of timely::execute::execute that calls into our fork of create_sockets
pub fn execute_acking_connections<T, F>(
    mut config: Config,
    func: F,
) -> Result<WorkerGuards<T>, String>
where
    T: Send + 'static,
    F: Fn(&mut Worker<timely::communication::Allocator>) -> T + Send + Sync + 'static,
{
    if let CommunicationConfig::Cluster { ref mut log_fn, .. } = config.communication {
        *log_fn = Box::new(|events_setup| {
            let mut result = None;
            if let Ok(addr) = ::std::env::var("TIMELY_COMM_LOG_ADDR") {
                use timely::dataflow::operators::capture::EventWriterCore;
                use timely::logging::BatchLogger;

                eprintln!("enabled COMM logging to {}", addr);

                if let Ok(stream) = TcpStream::connect(&addr) {
                    let writer = EventWriterCore::new(stream);
                    let mut logger = BatchLogger::new(writer);
                    result = Some(timely::logging_core::Logger::new(
                        ::std::time::Instant::now(),
                        ::std::time::Duration::default(),
                        events_setup,
                        move |time, data| logger.publish_batch(time, data),
                    ));
                } else {
                    panic!("Could not connect to communication log address: {:?}", addr);
                }
            }
            result
        });
    }

    let (allocators, other) = match config.communication {
        timely::CommunicationConfig::Cluster {
            threads,
            process,
            addresses,
            report,
            log_fn,
        } => {
            let sockets_result = create_sockets(addresses, process, report);
            let result: Result<(Vec<GenericBuilder>, Box<dyn Any + Send>), String> =
                match sockets_result.and_then(|sockets| {
                    initialize_networking_from_sockets(sockets, process, threads, log_fn)
                }) {
                    Ok((stuff, guard)) => Ok((
                        stuff
                            .into_iter()
                            .map(|x| GenericBuilder::ZeroCopy(x))
                            .collect(),
                        Box::new(guard),
                    )),
                    Err(err) => Err(format!("failed to initialize networking: {}", err)),
                };
            result
        }
        _ => config.communication.try_build(),
    }?;

    let worker_config = config.worker;
    initialize_from(allocators, other, move |allocator| {
        let mut worker = Worker::new(worker_config.clone(), allocator);

        // If an environment variable is set, use it as the default timely logging.
        if let Ok(addr) = ::std::env::var("TIMELY_WORKER_LOG_ADDR") {
            use timely::dataflow::operators::capture::EventWriterCore;
            use timely::logging::{BatchLogger, TimelyEvent};

            if let Ok(stream) = TcpStream::connect(&addr) {
                let writer = EventWriterCore::new(stream);
                let mut logger = BatchLogger::new(writer);
                worker
                    .log_register()
                    .insert::<TimelyEvent, _>("timely", move |time, data| {
                        logger.publish_batch(time, data)
                    });
            } else {
                panic!("Could not connect logging stream to: {:?}", addr);
            }
        }

        let result = func(&mut worker);
        while worker.has_dataflows() {
            worker.step_or_park(None);
        }
        result
    })
}

/// Creates socket connections from a list of host addresses.
///
/// The item at index i in the resulting vec, is a Some(TcpSocket) to process i, except
/// for item `my_index` which is None (no socket to self).
fn create_sockets(
    addresses: Vec<String>,
    my_index: usize,
    noisy: bool,
) -> IoResult<Vec<Option<TcpStream>>> {
    let hosts1 = Arc::new(addresses);
    let hosts2 = hosts1.clone();

    let start_task = thread::spawn(move || start_connections(hosts1, my_index, noisy));
    let await_task = thread::spawn(move || await_connections(hosts2, my_index, noisy));

    let mut results = start_task.join().unwrap()?;
    results.push(None);
    let to_extend = await_task.join().unwrap()?;
    results.extend(to_extend.into_iter());

    if noisy {
        println!("worker {}:\tinitialization complete", my_index)
    }

    Ok(results)
}

/// Result contains connections [0, my_index - 1].
fn start_connections(
    addresses: Arc<Vec<String>>,
    my_index: usize,
    noisy: bool,
) -> IoResult<Vec<Option<TcpStream>>> {
    let results = addresses.iter().take(my_index).enumerate().map(|(index, address)| {
        loop {
            match TcpStream::connect(address) {
                Ok(mut stream) => {
                    stream.set_nodelay(true).expect("set_nodelay call failed");
                    stream.write_all(HANDSHAKE_MAGIC.to_ne_bytes().as_slice()).expect("failed to send handshake magic");
                    stream.write_all(my_index.to_ne_bytes().as_slice()).expect("failed to send worker index");
                    let mut buffer = [0u8; 8];
                    let _ = stream.read_exact(&mut buffer);
                    let peer_ack = u64::from_ne_bytes(buffer);
                    if peer_ack != ACK_MAGIC {
                        println!("worker {my_index} failed computed handshake with worker {index}; retrying");
                        sleep(Duration::from_secs(1));
                        continue;
                    };
                    if noisy { println!("worker {}:\tconnection to worker {}", my_index, index); }
                    break Some(stream);
                },
                Err(error) => {
                    println!("worker {my_index}:\terror connecting to worker {index}: {error}; retrying");
                    sleep(Duration::from_secs(1));
                },
            }
        }
    }).collect();

    Ok(results)
}

/// Result contains connections [my_index + 1, addresses.len() - 1].
pub fn await_connections(
    addresses: Arc<Vec<String>>,
    my_index: usize,
    noisy: bool,
) -> IoResult<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index - 1))
        .map(|_| None)
        .collect();
    let listener = TcpListener::bind(&addresses[my_index][..])?;

    for _ in (my_index + 1)..addresses.len() {
        let mut stream = listener.accept()?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");
        let mut buffer = [0u8; 16];
        stream.read_exact(&mut buffer)?;
        let magic = u64::from_ne_bytes((&buffer[0..8]).try_into().unwrap());
        if magic != HANDSHAKE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "received incorrect timely handshake",
            ));
        }
        let identifier = u64::from_ne_bytes((&buffer[8..16]).try_into().unwrap());
        stream
            .write_all(ACK_MAGIC.to_ne_bytes().as_slice())
            .expect("failed to send ack magic");
        results[identifier as usize - my_index - 1] = Some(stream);
        if noisy {
            println!(
                "worker {}:\tconnection from worker {}",
                my_index, identifier
            );
        }
    }

    Ok(results)
}
