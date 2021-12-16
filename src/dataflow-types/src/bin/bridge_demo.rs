#[derive(clap::Parser)]
struct Args {
    /// The address on which to listen for a connection from the coordinator.
    #[clap(
        long,
        env = "REPLICA_CONNECT_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6877"
    )]
    replica_addr: String,
    /// The address on which to listen for a connection from the coordinator.
    #[clap(
        long,
        env = "DATAFLOW_CONNECT_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6878"
    )]
    dataflow_addr: String,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run(mz_ore::cli::parse_args()).await {
        eprintln!("bridge_demo: {:#}", err);
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    use tokio::net::TcpStream;

    let mut replica =
        mz_dataflow_types::client::tcp::framed_server(TcpStream::connect(args.replica_addr).await?);
    let mut dataflow = mz_dataflow_types::client::tcp::framed_client(
        TcpStream::connect(args.dataflow_addr).await?,
    );

    loop {
        use futures::sink::SinkExt;
        use futures::stream::TryStreamExt;

        tokio::select! {
            from_d = dataflow.try_next() => {
                let from_d: Option<mz_dataflow_types::client::Response> = from_d?;
                let from_d = from_d.expect("Dataflow connection terminated");
                replica.send(from_d).await?;
            }
            from_r = replica.try_next() => {
                let from_r: Option<mz_dataflow_types::client::Command> = from_r?;
                if let Some(from_r) = from_r {
                    dataflow.send(from_r).await?;
                } else {
                    return Ok(());
                }
            }
        }
    }
}
