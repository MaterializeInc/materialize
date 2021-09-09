use dataflow::{Command, Response};

fn main() {
    // idk?
    let runtime = tokio::runtime::Runtime::new().expect("creating tokio runtime failed");
    let _enter_tokio_guard = std::mem::forget(runtime.enter());

    let workers = 4;
    let worker_config = timely::WorkerConfig::default();

    // Channels to communicate commands to the dataflow.
    let mut command_send = Vec::new();
    let mut command_recv = Vec::new();
    for _ in 0..workers {
        let (send, recv) = crossbeam_channel::unbounded();
        command_send.push(send);
        command_recv.push(recv);
    }

    // Channels to communicate response from the dataflow.
    let (response_send, mut response_recv) = tokio::sync::mpsc::unbounded_channel();

    // Required configuration to start a dataflow instance.
    let dataflow_config = dataflow::Config {
        command_receivers: command_recv,
        timely_worker: worker_config,
        experimental_mode: true,
        now: || 0,
        metrics_registry: ore::metrics::MetricsRegistry::new(),
        persist: None,
        feedback_tx: response_send,
    };

    let _worker_guards = dataflow::serve(dataflow_config);

    // Now do various things with command_send and response_recv.
    broadcast(dataflow::Command::Shutdown, &command_send);

    while let Some(response) = response_recv.blocking_recv() {
        println!("RESPONSE: {:?}", response);
    }
}

fn broadcast(command: dataflow::Command, sends: &[crossbeam_channel::Sender<Command>]) {
    for channel in sends.iter() {
        channel.send(command.clone()).unwrap();
    }
}
