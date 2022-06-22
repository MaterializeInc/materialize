// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A simple CLI log viewer for Timely/Differential dataflow programs.

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use timely::dataflow::operators::capture::EventReader;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::operators::Inspect;
use timely::logging::{TimelyEvent, WorkerIdentifier};

use mz_ore::cli::{self, CliConfig};

/// Views Timely logs from a running dataflow server.
///
/// Listens for incoming log connections from a Timely/Differential program running
/// with NUM-TIMELY-WORKERS workers. Point that program at this one by running it
/// with the environment variable TIMELY_WORKER_LOG_ADDR set to ADDR:PORT.
#[derive(clap::Parser)]
struct Args {
    /// Address to listen on.
    #[clap(short = 'l', long, value_name = "ADDR", default_value = "127.0.0.1")]
    listen_addr: String,
    /// Port to listen on.
    #[clap(short = 'p', long, value_name = "PORT", default_value = "51371")]
    port: u16,
    /// The number of Timely workers that the dataflow server is running with.
    #[clap(value_name = "NUM-TIMELY-WORKERS")]
    num_timely_workers: usize,
    /// Arguments to use to configure this program's Timely computation.
    #[clap(last = true)]
    timely_args: Vec<String>,
}

fn main() {
    let args: Args = cli::parse_args(CliConfig::default());

    let listener =
        TcpListener::bind((&*args.listen_addr, args.port)).expect("binding tcp listener");
    println!(
        "listening on {}:{} for {} worker(s)...",
        args.listen_addr, args.port, args.num_timely_workers
    );

    let sockets = Arc::new(Mutex::new(
        (0..args.num_timely_workers)
            .map(|_| {
                let l = listener
                    .incoming()
                    .next()
                    .expect("listener hungup")
                    .expect("error accepting connection");
                Some(l)
            })
            .collect::<Vec<_>>(),
    ));

    timely::execute_from_args(args.timely_args.into_iter(), move |worker| {
        let index = worker.index();
        let peers = worker.peers();
        let replayers = Arc::clone(&sockets)
            .lock()
            .expect("sockets lock poisoned")
            .iter_mut()
            .enumerate()
            .filter(|(i, _)| *i % peers == index)
            .map(move |(_, s)| s.take().unwrap())
            .map(EventReader::<Duration, (Duration, WorkerIdentifier, TimelyEvent), _>::new)
            .collect::<Vec<_>>();
        worker.dataflow::<Duration, _, _>(|scope| {
            replayers
                .replay_into(scope)
                .inspect(|x| println!("{:?}", x));
        });
    })
    .expect("timely computation failed");
}
