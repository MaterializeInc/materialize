use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread::sleep;
use std::time::Duration;

use clap::Parser;

#[derive(Debug, Parser)]
#[clap(name = "gus")]
struct Cli {
    #[clap(long)]
    place: String,
    #[clap(long)]
    wait_time_ms: u64,
    #[clap(long)]
    cause_failure: bool,
}

fn main() {
    let args = Cli::parse();
    let wait_time = Duration::from_millis(args.wait_time_ms);

    let runtime = tokio::runtime::Runtime::new().unwrap();

    let _guard = runtime.enter();

    let fut = reqwest::ClientBuilder::new()
        // ensure we get a timeout
        .timeout(Duration::from_millis(args.wait_time_ms / 2))
        .build()
        .unwrap()
        .get(args.place)
        .send();
    let mut fut = Box::pin(fut);

    let waker = futures::task::noop_waker_ref();
    let mut ctx = Context::from_waker(waker);

    let ret = loop {
        println!("polling...");
        let ret = Pin::new(&mut fut).poll(&mut ctx);

        if let Poll::Ready(ret) = ret {
            break ret;
        }

        if args.cause_failure {
            sleep(wait_time);
        }
    };

    match ret {
        Ok(inner) => {
            println!("connected: {:?}", inner);
        }
        Err(e) => {
            // In practice we will see:
            // $ cargo run -- --wait-time-ms 1000 --place "http://google.com" --cause-failure
            // polling...
            // polling...
            // errored: error sending request for url (http://google.com/): operation timed out
            //
            //
            // What's unclear to me is IF we are hitting this problem, then what actual timeout are
            // we hitting? In the customer issue, we saw some timeouts of UP to and maybe exceeding
            // 30 second:
            // ```
            // 2022-12-15T18:04:09.990975Z  INFO mz_persist_client::internal::machine: external operation fetch_batch::get failed, retrying in 32ms: indeterminate: s3 get meta err: timeout: error trying to connect: HTTP connect timeout occurred after 30s
            // ```
            //
            // This means we either starved polling for 30+ seconds, or we starved it for JUST
            // enough time (maybe a few seonds) multiple times, so that after a total of 30 second worth of
            // polling, the request died. The important thing to note here is that _mis-polling
            // (by being starved) a future can 100% cause issues that SEEM like a problem talking
            // to an upstream service, but are not_.
            //
            // This is not a new problem, and has been documented here
            // <https://github.com/rust-lang/futures-rs/issues/2387>, in the context of
            // `FuturesUnordered` starving and timing out db connections.
            println!("errored: {}", e);
        }
    }
}
