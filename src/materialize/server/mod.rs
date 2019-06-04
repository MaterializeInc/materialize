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

use crate::clock::Clock;
use crate::dataflow;
use crate::glue::*;
use crate::pgwire;
use crate::queue;
use ore::future::FutureExt;
use ore::netio;
use ore::netio::SniffingStream;

mod http;

pub enum QueueConfig {
    Transient,
}

pub enum PeekResultsConfig {
    Local,
    Remote,
}

pub struct Config {
    queue: QueueConfig,
    num_timely_workers: usize,
    peek_results: PeekResultsConfig,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            queue: QueueConfig::Transient,
            num_timely_workers: 4,
            peek_results: PeekResultsConfig::Remote,
        }
    }
}

fn handle_connection(
    tcp_stream: TcpStream,
    sql_command_sender: UnboundedSender<(SqlCommand, CommandMeta)>,
    sql_response_mux: SqlResponseMux,
    peek_results_mux: PeekResultsMux,
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
                    peek_results_mux,
                    num_timely_workers,
                )
                .either_a()
            } else if http::match_handshake(buf) {
                http::handle_connection(ss.into_sniffed(), peek_results_mux).either_b()
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
    let clock = Clock::default();

    let (sql_command_sender, sql_command_receiver) = unbounded::<(SqlCommand, CommandMeta)>();
    let sql_response_mux = SqlResponseMux::default();
    let (dataflow_command_senders, dataflow_command_receivers) = (0..config.num_timely_workers)
        .map(|_| unbounded::<(DataflowCommand, CommandMeta)>())
        .unzip();
    let peek_results_mux = PeekResultsMux::default();

    // timely dataflow
    let peek_results_handler = match config.peek_results {
        PeekResultsConfig::Local => dataflow::PeekResultsHandler::Local(peek_results_mux.clone()),
        PeekResultsConfig::Remote => dataflow::PeekResultsHandler::Remote,
    };
    let dd_workers = dataflow::serve(
        dataflow_command_receivers,
        peek_results_handler,
        clock.clone(),
        config.num_timely_workers,
    )?;

    let threads = dd_workers
        .guards()
        .iter()
        .map(|jh| jh.thread().clone())
        .collect::<Vec<_>>();

    // queue and sql planner
    match &config.queue {
        QueueConfig::Transient => {
            queue::transient::serve(
                sql_command_receiver,
                sql_response_mux.clone(),
                dataflow_command_senders,
                threads,
                clock.clone(),
            );
        }
    }

    // pgwire / http server
    let listen_addr: SocketAddr = "127.0.0.1:6875".parse()?;
    let listener = TcpListener::bind(&listen_addr)?;
    let start = future::lazy(move || {
        let server = listener
            .incoming()
            .for_each(move |stream| {
                tokio::spawn(handle_connection(
                    stream,
                    sql_command_sender.clone(),
                    sql_response_mux.clone(),
                    peek_results_mux.clone(),
                    config.num_timely_workers,
                ));
                Ok(())
            })
            .map_err(|err| error!("error accepting connection: {}", err));
        tokio::spawn(server);

        Ok(())
    });
    tokio::run(start);
    println!("materialized listening on {}...", listen_addr);

    Ok(())
}
