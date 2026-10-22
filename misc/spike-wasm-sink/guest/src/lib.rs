//! THROWAWAY spike guest: upserts a relation keyed on its first column into
//! Valkey hashes `<prefix>:k:<key>`, with the committed frontier in
//! `<prefix>:progress`. Compiles natively (baseline) and as a wasm32-wasip2
//! component (`wit/sink.wit`).

pub mod resp;

use arrow_array::{Array, Int64Array, RecordBatch, UInt64Array};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_ipc::reader::StreamReader;
use resp::{Conn, Reply};

#[derive(serde::Deserialize)]
struct Config {
    /// Resolved `ip:port`. The host resolves names, the guest has no DNS.
    addr: String,
    /// Key prefix, a hash tag such as `{s1}` so all keys share a slot.
    prefix: String,
    /// Sandbox probe run in `open`: `spin`, `connect:<ip:port>`,
    /// `read:<path>`, or `alloc:<bytes>`.
    #[serde(default)]
    probe: Option<String>,
}

fn probe(p: &str) -> Result<(), String> {
    if p == "spin" {
        let mut x: u64 = 0;
        loop {
            x = std::hint::black_box(x.wrapping_add(1));
        }
    } else if let Some(a) = p.strip_prefix("connect:") {
        std::net::TcpStream::connect(a)
            .map(|_| ())
            .map_err(|e| format!("probe connect {a} failed: {e}"))
    } else if let Some(f) = p.strip_prefix("read:") {
        std::fs::read(f)
            .map(|_| ())
            .map_err(|e| format!("probe read {f} failed: {e}"))
            .and(Err(format!("probe read {f} SUCCEEDED")))
    } else if let Some(n) = p.strip_prefix("alloc:") {
        let n: usize = n.parse().map_err(|e| format!("{e}"))?;
        let v = std::hint::black_box(vec![1u8; n]);
        Err(format!("probe alloc {} SUCCEEDED", v.len()))
    } else {
        Err(format!("unknown probe {p}"))
    }
}

/// Commands queued before a transaction is committed mid-batch.
const FLUSH_EVERY: usize = 20_000;

pub struct ValkeySink {
    conn: Conn,
    prefix: String,
    epoch_key: String,
    /// The epoch this instance claimed with `INCR` on open. Every transaction
    /// WATCHes the epoch key, so an older instance's writes abort once a newer
    /// instance has opened.
    epoch: Vec<u8>,
    in_tx: bool,
    pub stats: Stats,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct Stats {
    pub rows: u64,
    pub commands: u64,
    pub commits: u64,
}

impl ValkeySink {
    pub fn open(config: &str) -> Result<(Self, Option<u64>), String> {
        let cfg: Config = serde_json::from_str(config).map_err(|e| e.to_string())?;
        if let Some(p) = &cfg.probe {
            probe(p)?;
        }
        let mut conn = Conn::connect(&cfg.addr)?;
        let epoch_key = format!("{}:epoch", cfg.prefix);
        let epoch = match conn.call(&["INCR", epoch_key.as_str()])? {
            Reply::Int(e) => e.to_string().into_bytes(),
            r => return Err(format!("unexpected INCR reply {r:?}")),
        };
        let progress = format!("{}:progress", cfg.prefix);
        let resume = match conn.call(&["GET", progress.as_str()])? {
            Reply::Bulk(None) => None,
            Reply::Bulk(Some(b)) => Some(
                String::from_utf8_lossy(&b)
                    .parse::<u64>()
                    .map_err(|e| format!("bad progress: {e}"))?,
            ),
            r => return Err(format!("unexpected GET reply {r:?}")),
        };
        Ok((
            ValkeySink {
                conn,
                prefix: cfg.prefix,
                epoch_key,
                epoch,
                in_tx: false,
                stats: Stats::default(),
            },
            resume,
        ))
    }

    /// Queues a command inside the current fenced transaction, starting one
    /// if needed.
    fn push<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Result<(), String> {
        if !self.in_tx {
            self.conn.call(&["WATCH", self.epoch_key.as_str()])?;
            let current = match self.conn.call(&["GET", self.epoch_key.as_str()])? {
                Reply::Bulk(Some(b)) => b,
                r => return Err(format!("fenced: epoch key {r:?}")),
            };
            if current != self.epoch {
                return Err(format!(
                    "fenced: epoch is {}, ours is {}",
                    String::from_utf8_lossy(&current),
                    String::from_utf8_lossy(&self.epoch)
                ));
            }
            self.conn.queue(&["MULTI"]);
            self.in_tx = true;
        }
        self.conn.queue(args);
        Ok(())
    }

    /// Commits the current transaction, if any.
    fn commit_tx(&mut self) -> Result<(), String> {
        if !self.in_tx {
            return Ok(());
        }
        self.conn.queue(&["EXEC"]);
        self.stats.commands += self.conn.queued() as u64;
        self.in_tx = false;
        let replies = self.conn.flush()?;
        match replies.last() {
            Some(Reply::Array(Some(rs))) => {
                if let Some(e) = rs.iter().find(|r| matches!(r, Reply::Error(_))) {
                    return Err(format!("EXEC error {e:?}"));
                }
                Ok(())
            }
            Some(Reply::Array(None)) => Err("fenced: EXEC aborted, epoch changed".into()),
            r => Err(format!("unexpected EXEC reply {r:?}")),
        }
    }

    pub fn reset(&mut self) -> Result<(), String> {
        let pattern = format!("{}:*", self.prefix);
        let mut cursor = b"0".to_vec();
        loop {
            let r = self.conn.call(&[
                b"SCAN".as_slice(),
                &cursor,
                b"MATCH",
                pattern.as_bytes(),
                b"COUNT",
                b"10000",
            ])?;
            let Reply::Array(Some(mut parts)) = r else {
                return Err(format!("bad SCAN reply {r:?}"));
            };
            let keys = parts.pop();
            let Some(Reply::Bulk(Some(next))) = parts.pop() else {
                return Err("bad SCAN cursor".into());
            };
            if let Some(Reply::Array(Some(keys))) = keys {
                let mut args: Vec<Vec<u8>> = vec![b"UNLINK".to_vec()];
                for k in keys {
                    if let Reply::Bulk(Some(k)) = k {
                        if k != self.epoch_key.as_bytes() {
                            args.push(k);
                        }
                    }
                }
                if args.len() > 1 {
                    self.push(&args)?;
                    self.commit_tx()?;
                }
            }
            if next == b"0" {
                return Ok(());
            }
            cursor = next;
        }
    }

    pub fn write(&mut self, batch: &[u8], commit: Option<u64>) -> Result<(), String> {
        let batches = decode(batch)?;
        for b in &batches {
            self.apply(b)?;
        }
        if let Some(f) = commit {
            let progress = format!("{}:progress", self.prefix);
            self.push(&["SET", progress.as_str(), f.to_string().as_str()])?;
            self.commit_tx()?;
            self.stats.commits += 1;
        } else {
            self.commit_tx()?;
        }
        Ok(())
    }

    /// Queues commands for one batch sorted by `(mz_time, row)`. Rows of one
    /// key at one time are contiguous because the key is the first column.
    fn apply(&mut self, b: &RecordBatch) -> Result<(), String> {
        let n = b.num_columns();
        let ncols = n - 2;
        let times = b
            .column(n - 2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or("mz_time not u64")?;
        let diffs = b
            .column(n - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("mz_diff not i64")?;
        let opts = FormatOptions::default();
        let fmts: Vec<ArrayFormatter> = (0..ncols)
            .map(|i| ArrayFormatter::try_new(b.column(i).as_ref(), &opts))
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        let names: Vec<String> = b.schema().fields()[..ncols]
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let rows = b.num_rows();
        self.stats.rows += rows as u64;
        let mut i = 0;
        while i < rows {
            let t = times.value(i);
            let key = fmts[0].value(i).to_string();
            let mut j = i;
            let mut positive = None;
            while j < rows && times.value(j) == t && fmts[0].value(j).to_string() == key {
                if diffs.value(j) > 0 {
                    positive = Some(j);
                }
                j += 1;
            }
            let k = format!("{}:k:{}", self.prefix, key);
            self.push(&[b"DEL".as_slice(), k.as_bytes()])?;
            if let Some(p) = positive {
                let mut args: Vec<Vec<u8>> = vec![b"HSET".to_vec(), k.into_bytes()];
                for c in 1..ncols {
                    if b.column(c).is_valid(p) {
                        args.push(names[c].clone().into_bytes());
                        args.push(fmts[c].value(p).to_string().into_bytes());
                    }
                }
                if args.len() > 2 {
                    self.push(&args)?;
                }
            }
            i = j;
            if self.conn.queued() >= FLUSH_EVERY {
                self.commit_tx()?;
            }
        }
        Ok(())
    }
}

fn decode(batch: &[u8]) -> Result<Vec<RecordBatch>, String> {
    if batch.is_empty() {
        return Ok(Vec::new());
    }
    let reader = StreamReader::try_new(batch, None).map_err(|e| e.to_string())?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

#[cfg(target_family = "wasm")]
mod component {
    use std::cell::RefCell;

    wit_bindgen::generate!({ path: "wit", world: "sink" });

    thread_local! {
        static SINK: RefCell<Option<super::ValkeySink>> = const { RefCell::new(None) };
    }

    struct Component;

    impl Component {
        fn with<R>(f: impl FnOnce(&mut super::ValkeySink) -> Result<R, String>) -> Result<R, String> {
            SINK.with(|s| f(s.borrow_mut().as_mut().ok_or("sink not open")?))
        }
    }

    impl Guest for Component {
        fn open_sink(config: String) -> Result<Option<u64>, String> {
            let (sink, resume) = super::ValkeySink::open(&config)?;
            log(&format!("opened, resume={resume:?}"));
            SINK.with(|s| *s.borrow_mut() = Some(sink));
            Ok(resume)
        }
        fn reset_sink() -> Result<(), String> {
            Self::with(|s| s.reset())
        }
        fn write_batch(batch: Vec<u8>, commit: Option<u64>) -> Result<(), String> {
            Self::with(|s| s.write(&batch, commit))
        }
    }

    export!(Component);
}
