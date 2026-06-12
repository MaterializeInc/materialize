//! Hosts the persist PubSub server and a `PersistClientCache` wired to it.
//! This is the mechanism's persist access layer; it embeds no workload.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::PersistClient;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PersistGrpcPubSubServer;
use mz_persist_types::PersistLocation;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

/// Hosts persist infrastructure for the driver: a PubSub server plus a client
/// cache configured to use it. `clusterd` connects to `pubsub_url()`.
pub struct PersistHost {
    cache: Arc<PersistClientCache>,
    pubsub_port: u16,
    location: PersistLocation,
}

impl PersistHost {
    /// Starts a PubSub server on an ephemeral localhost port (for unit tests).
    pub async fn start(location: PersistLocation) -> anyhow::Result<Self> {
        Self::start_on(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            location,
        )
        .await
    }

    /// Starts a PubSub server bound to `bind` and builds a cache wired to it,
    /// using the given blob/consensus location for all shards. mzcompose passes
    /// a fixed routable bind (e.g. `0.0.0.0:6879`) so clusterd can be configured
    /// with the driver's PubSub URL at its own startup.
    pub async fn start_on(bind: SocketAddr, location: PersistLocation) -> anyhow::Result<Self> {
        let registry = MetricsRegistry::new();
        let persist_cfg = PersistConfig::new_default_configs(
            &mz_build_info::build_info!(),
            mz_ore::now::SYSTEM_TIME.clone(),
        );

        let server = PersistGrpcPubSubServer::new(&persist_cfg, &registry);
        let conn = server.new_same_process_connection();

        let listener = TcpListener::bind(bind).await?;
        let pubsub_port = listener.local_addr()?.port();
        mz_ore::task::spawn(|| "persist_pubsub_server", async move {
            server
                .serve_with_stream(TcpListenerStream::new(listener))
                .await
                .expect("pubsub server");
        });

        let cache = PersistClientCache::new(persist_cfg, &registry, |_cfg, _metrics| conn);
        Ok(PersistHost {
            cache: Arc::new(cache),
            pubsub_port,
            location,
        })
    }

    /// The port the PubSub server is listening on.
    ///
    /// Prefer this for the container/mzcompose case, where `clusterd` reaches
    /// the driver by service hostname: the composition builds the URL from the
    /// driver's service name plus this port.
    pub fn pubsub_port(&self) -> u16 {
        self.pubsub_port
    }

    /// A loopback PubSub URL, valid only for same-host use (local iteration,
    /// unit tests). For cross-container use, build the URL from the driver's
    /// routable hostname and [`PersistHost::pubsub_port`] instead — this method
    /// always returns `127.0.0.1` regardless of the bind address.
    pub fn pubsub_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.pubsub_port)
    }

    /// The shared blob/consensus location to embed in shard metadata.
    pub fn location(&self) -> &PersistLocation {
        &self.location
    }

    /// Opens a persist client against the hosted location.
    pub async fn client(&self) -> anyhow::Result<PersistClient> {
        Ok(self.cache.open(self.location.clone()).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns a file-backed location plus the `TempDir` guard, which the
    /// caller must keep alive for the duration of the test so the blob
    /// directory is not cleaned up while in use.
    fn file_location() -> (PersistLocation, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let blob = format!("file://{}", dir.path().display());
        let consensus = std::env::var("COCKROACH_URL").unwrap_or_else(|_| {
            "postgres://root@127.0.0.1:26257?options=--search_path=mz_driver".to_string()
        });
        let location = PersistLocation {
            blob_uri: blob.parse().expect("blob uri"),
            consensus_uri: consensus.parse().expect("consensus uri"),
        };
        (location, dir)
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn host_starts_and_opens_client() {
        if std::env::var("COCKROACH_URL").is_err() {
            return; // requires a running CockroachDB; skip otherwise
        }
        let (location, _dir) = file_location();
        let host = PersistHost::start(location).await.expect("host");
        assert!(host.pubsub_url().starts_with("http://127.0.0.1:"));

        // The PubSub server must actually be listening on the reported port.
        tokio::net::TcpStream::connect(("127.0.0.1", host.pubsub_port()))
            .await
            .expect("pubsub server listening");

        let _client = host.client().await.expect("client");
    }
}
