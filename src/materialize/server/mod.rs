// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Main materialized server.

use futures::Future;
use log::error;
use std::boxed::Box;
use std::error::Error as StdError;
use std::net::SocketAddr;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

use crate::dataflow;
use crate::glue::*;
use crate::pgwire;
use crate::queue;
use ore::collections::CollectionExt;
use ore::future::FutureExt;
use ore::netio;
use ore::netio::SniffingStream;

mod http;

pub enum QueueConfig {
    Transient,
}

pub enum DataflowResultsConfig {
    Local,
    Remote,
}

pub struct Config {
    queue: QueueConfig,
    num_timely_workers: usize,
    dataflow_results: DataflowResultsConfig,
    timely_configuration: timely::Configuration,
}

impl Config {
    /// Constructs a materialize configuration from a timely dataflow configuration.
    pub fn from_timely(timely_configuration: timely::Configuration) -> Self {
        let num_timely_workers = match &timely_configuration {
            timely::Configuration::Thread => 1,
            timely::Configuration::Process(n) => *n,
            timely::Configuration::Cluster {
                threads, addresses, ..
            } => threads * addresses.len(),
        };

        Self {
            queue: QueueConfig::Transient,
            num_timely_workers,
            dataflow_results: DataflowResultsConfig::Remote,
            timely_configuration,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            queue: QueueConfig::Transient,
            num_timely_workers: 4,
            dataflow_results: DataflowResultsConfig::Remote,
            timely_configuration: timely::Configuration::Process(4),
        }
    }
}

fn handle_connection(
    tcp_stream: TcpStream,
    sql_command_sender: UnboundedSender<(SqlCommand, CommandMeta)>,
    sql_response_mux: SqlResponseMux,
    dataflow_results_mux: DataflowResultsMux,
    num_timely_workers: usize,
) -> impl Future<Item = (), Error = ()> {
    // Sniff out what protocol we've received. Choosing how many bytes to sniff
    // is a delicate business. Read too many bytes and you'll stall out
    // protocols with small handshakes, like pgwire. Read too few bytes and
    // you won't be able to tell what protocol you have. For now, eight bytes
    // is the magic number, but this may need to change if we learn to speak
    // new protocols.
    let ss = SniffingStream::new(tcp_stream);
    netio::read_exact_or_eof(ss, [0; 8])
        .from_err()
        .and_then(move |(ss, buf, nread)| {
            let buf = &buf[..nread];
            if pgwire::match_handshake(buf) {
                pgwire::serve(
                    ss.into_sniffed(),
                    sql_command_sender,
                    sql_response_mux,
                    dataflow_results_mux,
                    num_timely_workers,
                )
                .either_a()
            } else if http::match_handshake(buf) {
                http::handle_connection(ss.into_sniffed(), dataflow_results_mux).either_b()
            } else {
                reject_connection(ss.into_sniffed()).from_err().either_c()
            }
        })
        .map_err(|err| error!("error handling request: {}", err))
}

fn reject_connection<A: AsyncWrite>(a: A) -> impl Future<Item = (), Error = io::Error> {
    io::write_all(a, "unknown protocol\n").discard()
}

/// Start the materialized server.
pub fn serve(config: Config) -> Result<(), Box<dyn StdError>> {
    // Construct shared channels for SQL command and result exchange, and dataflow command and result exchange.
    let (sql_command_sender, sql_command_receiver) =
        crate::glue::unbounded::<(SqlCommand, CommandMeta)>();
    let sql_response_mux = SqlResponseMux::default();
    let (dataflow_command_sender, dataflow_command_receiver) =
        crate::glue::unbounded::<(DataflowCommand, CommandMeta)>();
    let dataflow_results_mux = DataflowResultsMux::default();

    // Initialize timely dataflow computation.
    let is_primary = match &config.timely_configuration {
        timely::Configuration::Thread => true,
        timely::Configuration::Process(_) => true,
        timely::Configuration::Cluster { process, .. } => process == &0,
    };
    let dataflow_results_handler = match config.dataflow_results {
        DataflowResultsConfig::Local => {
            dataflow::DataflowResultsHandler::Local(dataflow_results_mux.clone())
        }
        DataflowResultsConfig::Remote => dataflow::DataflowResultsHandler::Remote,
    };
    let dd_workers = dataflow::serve(
        dataflow_command_receiver,
        dataflow_results_handler,
        config.timely_configuration,
    )?;

    // queue and sql planner
    match &config.queue {
        QueueConfig::Transient => {
            let worker0_thread = dd_workers.guards().into_first().thread();
            queue::transient::serve(
                sql_command_receiver,
                sql_response_mux.clone(),
                dataflow_command_sender,
                worker0_thread.clone(),
            );
        }
    }

    // pgwire / http server
    if is_primary {
        let listen_addr: SocketAddr = "127.0.0.1:6875".parse()?;
        let listener = TcpListener::bind(&listen_addr)?;
        let num_timely_workers = config.num_timely_workers;
        let start = future::lazy(move || {
            let server = listener
                .incoming()
                .for_each(move |stream| {
                    tokio::spawn(handle_connection(
                        stream,
                        sql_command_sender.clone(),
                        sql_response_mux.clone(),
                        dataflow_results_mux.clone(),
                        num_timely_workers,
                    ));
                    Ok(())
                })
                .map_err(|err| error!("error accepting connection: {}", err));
            tokio::spawn(server);

            Ok(())
        });
        tokio::run(start);
        println!("materialized listening on {}...", listen_addr);
    }
    Ok(())
}
