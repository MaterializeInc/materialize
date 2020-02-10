// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A simple CLI log viewer for Timely/Differential dataflow programs.

use timely::dataflow::operators::capture::EventReader;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::operators::Inspect;
use timely::logging::{TimelyEvent, WorkerIdentifier};

use std::env;
use std::net::TcpListener;
use std::process;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const USAGE: &str = r#"usage: dataflow-logview NUM-TIMELY-WORKERS
       dataflow-logview [-p PORT] [-l ADDR] NUM-TIMELY-WORKERS

Listens for incoming log connections from a Timely/Differential program running
with NUM-TIMELY-WORKERS workers. Point that program at this one by running it
with the environment variable TIMELY_WORKER_LOG_ADDR set to ADDR:PORT."#;

fn main() {
    let args: Vec<_> = env::args().collect();

    let mut opts = getopts::Options::new();
    opts.optflag("h", "help", "show this usage information");
    opts.optopt(
        "l",
        "listen-addr",
        "address to listen on (default 127.0.0.1)",
        "ADDR",
    );
    opts.optopt("p", "port", "port to listen on (default 51371)", "PORT");
    let mut popts = match opts.parse(&args[1..]) {
        Ok(popts) => popts,
        Err(err) => {
            eprintln!("{}", err);
            process::exit(1);
        }
    };

    if popts.opt_present("h") || popts.free.is_empty() {
        eprint!("{}", opts.usage(USAGE));
        process::exit(1);
    }
    let source_peers_str = popts.free.remove(0);

    let addr = popts.opt_str("l").unwrap_or_else(|| "127.0.0.1".into());
    let port: u16 = match popts.opt_get_default("p", 51371) {
        Ok(port) => port,
        Err(err) => {
            eprintln!("unable to parse --port: {}", err);
            process::exit(1);
        }
    };
    let source_peers: usize = match source_peers_str.parse() {
        Ok(port) => port,
        Err(err) => {
            eprintln!(
                "unable to parse source peers: {}: {}",
                err, source_peers_str
            );
            process::exit(1);
        }
    };

    let listener = TcpListener::bind((&*addr, port)).expect("binding tcp listener");
    println!(
        "listening on {}:{} for {} worker(s)...",
        addr, port, source_peers
    );

    let sockets = Arc::new(Mutex::new(
        (0..source_peers)
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

    timely::execute_from_args(popts.free.into_iter(), move |worker| {
        let index = worker.index();
        let peers = worker.peers();
        let replayers = sockets
            .clone()
            .lock()
            .expect("sockets lock poisoned")
            .iter_mut()
            .enumerate()
            .filter(|(i, _)| *i % peers == index)
            .map(move |(_, s)| s.take().unwrap())
            .map(|r| EventReader::<Duration, (Duration, WorkerIdentifier, TimelyEvent), _>::new(r))
            .collect::<Vec<_>>();
        worker.dataflow::<Duration, _, _>(|scope| {
            replayers
                .replay_into(scope)
                .inspect(|x| println!("{:?}", x));
        });
    })
    .expect("timely computation failed");
}
