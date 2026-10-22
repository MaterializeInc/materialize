//! THROWAWAY spike host for a hosted sink.
//!
//! Reads a materialized view's persist shard (snapshot, then listen),
//! consolidates each frontier step, encodes it as Arrow IPC, and hands it to a
//! sink guest (WASM component or the same code natively). The guest commits a
//! frontier `f` in the target; the host then downgrades its own critical since
//! hold to `f - 1`, the earliest time a restart needs to read.

mod guest;

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use arrow::ipc::writer::StreamWriter;
use clap::Parser;
use differential_dataflow::consolidation::consolidate_updates;
use mz_arrow_util::builder::ArrowBuilder;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::critical::{CriticalReaderId, Opaque, SinceHandle};
use mz_persist_client::read::ListenEvent;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{Diagnostics, PersistLocation, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, RelationDesc, Row, SqlScalarType, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;

use crate::guest::GuestThread;

#[derive(Parser)]
struct Args {
    #[clap(long)]
    blob: String,
    #[clap(long)]
    consensus: String,
    #[clap(long)]
    shard: String,
    /// Columns as `name:type[!]`, comma separated; `!` means NOT NULL.
    /// Types: int8, int4, text, bool. The first column is the upsert key.
    #[clap(long)]
    columns: String,
    /// File holding this sink's CriticalReaderId; created on first use.
    #[clap(long)]
    reader_id_file: PathBuf,
    /// Valkey `ip:port`; the only address the guest may connect to.
    #[clap(long)]
    valkey: std::net::SocketAddr,
    #[clap(long, default_value = "{s1}")]
    prefix: String,
    /// WASM component to run. Without it, the guest runs natively.
    #[clap(long)]
    wasm: Option<PathBuf>,
    #[clap(long, default_value = "50000")]
    chunk_rows: usize,
    /// Wall-clock bound for one guest call.
    #[clap(long, default_value = "30000")]
    call_deadline_ms: u64,
    /// Retention cap: the hold is never kept further than this behind now.
    #[clap(long, default_value = "600000")]
    max_lag_ms: u64,
    #[clap(long, default_value = "67108864")]
    guest_memory_bytes: usize,
    /// Minimum interval between since downgrades.
    #[clap(long, default_value = "250")]
    downgrade_interval_ms: u64,
    /// Exit after committing the first frontier at or beyond this time.
    #[clap(long)]
    exit_at: Option<u64>,
    /// Exit right after the snapshot is committed.
    #[clap(long)]
    exit_after_snapshot: bool,
    /// Extra guest config JSON fields, merged into the generated config.
    #[clap(long, default_value = "{}")]
    guest_extra: String,
}

fn parse_desc(spec: &str) -> anyhow::Result<RelationDesc> {
    let mut b = RelationDesc::builder();
    for col in spec.split(',') {
        let (name, ty) = col.split_once(':').context("name:type")?;
        let (ty, nullable) = match ty.strip_suffix('!') {
            Some(t) => (t, false),
            None => (ty, true),
        };
        let st = match ty {
            "int8" => SqlScalarType::Int64,
            "int4" => SqlScalarType::Int32,
            "text" => SqlScalarType::String,
            "bool" => SqlScalarType::Bool,
            t => bail!("unsupported type {t}"),
        };
        b = b.with_column(name, st.nullable(nullable));
    }
    Ok(b.finish())
}

/// Process CPU seconds (user + system) from /proc/self/stat.
fn cpu_secs() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let rest = s.rsplit_once(')').map(|x| x.1).unwrap_or("");
    let f: Vec<&str> = rest.split_whitespace().collect();
    let ticks = |i: usize| f.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    // Fields after the comm: state is 0, utime is 11, stime is 12.
    (ticks(11) + ticks(12)) / 100.0
}

fn rss_mib() -> f64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: f64 = s
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0);
    pages * 4096.0 / (1024.0 * 1024.0)
}

struct Encoder {
    desc: RelationDesc,
}

impl Encoder {
    fn new(desc: &RelationDesc) -> Self {
        let mut b = RelationDesc::builder();
        for (name, ty) in desc.iter() {
            b = b.with_column(name.clone(), ty.clone());
        }
        let desc = b
            .with_column("mz_time", SqlScalarType::UInt64.nullable(false))
            .with_column("mz_diff", SqlScalarType::Int64.nullable(false))
            .finish();
        Encoder { desc }
    }

    fn encode(&self, updates: &[(Row, Timestamp, i64)]) -> anyhow::Result<Vec<u8>> {
        let mut builder = ArrowBuilder::new(&self.desc, updates.len(), updates.len() * 32)?;
        let mut row = Row::default();
        for (r, t, d) in updates {
            let mut p = row.packer();
            p.extend(r.iter());
            p.push(Datum::UInt64(u64::from(*t)));
            p.push(Datum::Int64(*d));
            builder.add_row(&row)?;
        }
        let batch = builder.to_record_batch()?;
        let mut out = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut out, &batch.schema())?;
            w.write(&batch)?;
            w.finish()?;
        }
        Ok(out)
    }
}

struct Hold {
    handle: SinceHandle<SourceData, (), Timestamp, StorageDiff>,
    last: Instant,
    interval: Duration,
}

impl Hold {
    /// Downgrades the hold to `to`, rate limited unless `force`.
    async fn downgrade(&mut self, to: Timestamp, force: bool) -> anyhow::Result<()> {
        if !force && self.last.elapsed() < self.interval {
            return Ok(());
        }
        if self.handle.since().less_than(&to) {
            let opaque = self.handle.opaque().clone();
            self.handle
                .compare_and_downgrade_since(&opaque, (&opaque, &Antichain::from_elem(to)))
                .await
                .map_err(|o| anyhow!("since handle fenced, opaque {o:?}"))?;
        }
        self.last = Instant::now();
        Ok(())
    }

    fn since(&self) -> Option<Timestamp> {
        self.handle.since().as_option().copied()
    }
}

struct Stats {
    start: Instant,
    cpu0: f64,
    updates: u64,
    commits: u64,
    last_print: Instant,
}

impl Stats {
    fn print(&mut self, phase: &str, frontier: u64, force: bool) {
        if !force && self.last_print.elapsed() < Duration::from_secs(5) {
            return;
        }
        self.last_print = Instant::now();
        let wall = self.start.elapsed().as_secs_f64();
        let cpu = cpu_secs() - self.cpu0;
        let now = (SYSTEM_TIME)();
        let lag = now.saturating_sub(frontier);
        println!(
            "STATS phase={phase} wall_s={wall:.2} cpu_s={cpu:.2} updates={} commits={} upd_per_s={:.0} cpu_us_per_upd={:.3} lag_ms={lag} rss_mib={:.1}",
            self.updates,
            self.commits,
            self.updates as f64 / wall.max(1e-9),
            cpu * 1e6 / (self.updates.max(1) as f64),
            rss_mib(),
        );
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let desc = parse_desc(&args.columns)?;
    let shard = ShardId::from_str(&args.shard).map_err(|e| anyhow!(e))?;

    let reader_id = match std::fs::read_to_string(&args.reader_id_file) {
        Ok(s) => CriticalReaderId::from_str(s.trim()).map_err(|e| anyhow!(e))?,
        Err(_) => {
            let id = CriticalReaderId::new();
            // Recorded before registering, so a crash cannot lose the hold.
            std::fs::write(&args.reader_id_file, id.to_string())?;
            id
        }
    };

    let client = PersistClientCache::new(
        PersistConfig::new_default_configs(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone()),
        &MetricsRegistry::new(),
        |_, _| PubSubClientConnection::noop(),
    )
    .open(PersistLocation {
        blob_uri: SensitiveUrl::from_str(&args.blob)?,
        consensus_uri: SensitiveUrl::from_str(&args.consensus)?,
    })
    .await?;
    let diag = Diagnostics::from_purpose("wasm sink spike");
    let handle = client
        .open_critical_since::<SourceData, (), Timestamp, StorageDiff>(
            shard,
            reader_id,
            Opaque::encode(&0i64),
            diag.clone(),
        )
        .await?;
    let mut hold = Hold {
        handle,
        last: Instant::now() - Duration::from_secs(3600),
        interval: Duration::from_millis(args.downgrade_interval_ms),
    };
    let mut read = client
        .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
            shard,
            Arc::new(desc.clone()),
            Arc::new(UnitSchema),
            diag,
            false,
        )
        .await?;

    let deadline = Duration::from_millis(args.call_deadline_ms);
    let guest = match &args.wasm {
        Some(path) => {
            let engine = guest::engine()?;
            let t = Instant::now();
            let component = guest::load_component(&engine, path)?;
            println!("component compiled in {:?}", t.elapsed());
            let allowed = args.valkey;
            let limits = guest::WasmLimits {
                memory_bytes: args.guest_memory_bytes,
                call_ticks: args.call_deadline_ms * guest::TICKS_PER_SEC / 1000,
            };
            GuestThread::spawn(
                move || {
                    Ok(Box::new(guest::Wasm::new(&engine, &component, allowed, limits)?)
                        as Box<dyn guest::Guest>)
                },
                deadline,
            )
        }
        None => GuestThread::spawn(|| Ok(Box::new(guest::Native::new()) as _), deadline),
    };

    let mut config: serde_json::Value = serde_json::from_str(&args.guest_extra)?;
    config["addr"] = args.valkey.to_string().into();
    config["prefix"] = args.prefix.clone().into();

    let mut stats = Stats {
        start: Instant::now(),
        cpu0: cpu_secs(),
        updates: 0,
        commits: 0,
        last_print: Instant::now(),
    };
    let max_lag = args.max_lag_ms;

    // Any failure past this point first enforces the retention cap.
    let result = async {
        let resume = guest.open(config.to_string()).await?;
        let since = hold.since().context("shard since is empty")?;
        println!("open: resume={resume:?} hold_since={since} shard_since={:?}", read.since());
        let encoder = Encoder::new(&desc);

        let as_of = match resume {
            Some(f) if f > 0 && Timestamp::from(f - 1) >= since => {
                println!("resuming at as_of={}", f - 1);
                Timestamp::from(f - 1)
            }
            _ => {
                println!("fresh snapshot (resume={resume:?}, since={since})");
                guest.reset().await?;
                let as_of = read
                    .since()
                    .as_option()
                    .copied()
                    .context("empty since")?
                    .max(since);
                let snap_start = Instant::now();
                let mut cursor = read
                    .snapshot_cursor(Antichain::from_elem(as_of), |_| true)
                    .await
                    .map_err(|s| anyhow!("snapshot since {s:?}"))?;
                let mut chunk = Vec::with_capacity(args.chunk_rows);
                while let Some(iter) = cursor.next().await {
                    for ((k, _v), _t, d) in iter {
                        let row = k.0.map_err(|e| anyhow!("error in shard: {e:?}"))?;
                        chunk.push((row, as_of, d));
                        if chunk.len() >= args.chunk_rows {
                            stats.updates += chunk.len() as u64;
                            guest.write(encoder.encode(&chunk)?, None).await?;
                            chunk.clear();
                            stats.print("snapshot", u64::from(as_of), false);
                        }
                    }
                }
                stats.updates += chunk.len() as u64;
                let commit = u64::from(as_of) + 1;
                let bytes = if chunk.is_empty() {
                    Vec::new()
                } else {
                    encoder.encode(&chunk)?
                };
                guest.write(bytes, Some(commit)).await?;
                stats.commits += 1;
                hold.downgrade(as_of, true).await?;
                println!(
                    "SNAPSHOT_DONE rows={} secs={:.2}",
                    stats.updates,
                    snap_start.elapsed().as_secs_f64()
                );
                stats.print("snapshot", commit, true);
                if args.exit_after_snapshot {
                    return Ok(());
                }
                as_of
            }
        };

        let mut listen = read
            .listen(Antichain::from_elem(as_of))
            .await
            .map_err(|s| anyhow!("listen since {s:?}"))?;
        let mut frontier = u64::from(as_of) + 1;
        let mut buf: Vec<(Row, Timestamp, i64)> = Vec::new();
        loop {
            for ev in listen.fetch_next().await {
                match ev {
                    ListenEvent::Updates(us) => {
                        for ((k, _v), t, d) in us {
                            let row = k.0.map_err(|e| anyhow!("error in shard: {e:?}"))?;
                            buf.push((row, t, d));
                        }
                    }
                    ListenEvent::Progress(p) => {
                        let Some(p) = p.as_option().copied() else {
                            println!("shard closed");
                            return Ok(());
                        };
                        let p = u64::from(p);
                        if p <= frontier {
                            continue;
                        }
                        consolidate_updates(&mut buf);
                        // Sort by time, then row. A row's encoding starts with
                        // its first column, so rows of one key are contiguous.
                        buf.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
                        stats.updates += buf.len() as u64;
                        let chunks: Vec<_> = buf.chunks(args.chunk_rows).collect();
                        let n = chunks.len();
                        if n == 0 {
                            guest.write(Vec::new(), Some(p)).await?;
                        }
                        for (i, c) in chunks.into_iter().enumerate() {
                            let commit = (i + 1 == n).then_some(p);
                            guest.write(encoder.encode(c)?, commit).await?;
                        }
                        buf.clear();
                        stats.commits += 1;
                        frontier = p;
                        hold.downgrade(Timestamp::from(p - 1), false).await?;
                        stats.print("listen", p, false);
                        if args.exit_at.is_some_and(|e| p >= e) {
                            stats.print("listen", p, true);
                            println!("EXIT_AT frontier={p}");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
    .await;

    if let Err(e) = &result {
        eprintln!("sink failed: {e:#}");
        let now = (mz_ore::now::SYSTEM_TIME)();
        let floor = now.saturating_sub(max_lag);
        if let Some(since) = hold.since() {
            if u64::from(since) < floor {
                println!("RETENTION_CAP releasing hold from {since} to {floor}");
                hold.downgrade(Timestamp::from(floor), true).await?;
            }
        }
    }
    result
}
