//! THROWAWAY: sink guests (WASM component or native baseline), each driven on
//! a dedicated OS thread so a slow or stuck guest never blocks the host's
//! async runtime.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, anyhow};
use tokio::sync::oneshot;
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::sockets::SocketAddrUse;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

/// wasmtime 50 has its own error type.
fn w<T>(r: wasmtime::Result<T>) -> anyhow::Result<T> {
    r.map_err(|e| anyhow!("{e:?}"))
}

pub trait Guest {
    fn open(&mut self, config: &str) -> Result<Option<u64>, String>;
    fn reset(&mut self) -> Result<(), String>;
    fn write(&mut self, batch: &[u8], commit: Option<u64>) -> Result<(), String>;
}

/// Accepts every call without doing work; isolates host cost.
pub struct Null;

impl Guest for Null {
    fn open(&mut self, _config: &str) -> Result<Option<u64>, String> {
        Ok(None)
    }
    fn reset(&mut self) -> Result<(), String> {
        Ok(())
    }
    fn write(&mut self, _batch: &[u8], _commit: Option<u64>) -> Result<(), String> {
        Ok(())
    }
}

pub struct Native(Option<wasm_sink_guest::ValkeySink>);

impl Native {
    pub fn new() -> Self {
        Native(None)
    }
}

impl Guest for Native {
    fn open(&mut self, config: &str) -> Result<Option<u64>, String> {
        let (s, resume) = wasm_sink_guest::ValkeySink::open(config)?;
        self.0 = Some(s);
        Ok(resume)
    }
    fn reset(&mut self) -> Result<(), String> {
        self.0.as_mut().ok_or("not open")?.reset()
    }
    fn write(&mut self, batch: &[u8], commit: Option<u64>) -> Result<(), String> {
        self.0.as_mut().ok_or("not open")?.write(batch, commit)
    }
}

wasmtime::component::bindgen!({
    path: "../../misc/spike-wasm-sink/guest/wit",
    world: "sink",
});

struct State {
    wasi: WasiCtx,
    table: ResourceTable,
    limits: StoreLimits,
}

impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl SinkImports for State {
    fn log(&mut self, msg: String) {
        eprintln!("guest: {msg}");
    }
}

/// Epoch ticks per second; the engine's epoch advances every 10ms.
pub const TICKS_PER_SEC: u64 = 100;

pub struct WasmLimits {
    pub memory_bytes: usize,
    /// Per-call deadline in epoch ticks.
    pub call_ticks: u64,
    /// Per-call fuel budget as `(base, per input byte)`; `None` disables fuel.
    pub fuel: Option<(u64, u64)>,
}

pub struct Wasm {
    store: Store<State>,
    sink: Sink,
    call_ticks: u64,
    fuel: Option<(u64, u64)>,
    fuel_used: u64,
    calls: u64,
}

impl Wasm {
    /// Compiles the component and instantiates it with network access limited
    /// to TCP connects to `allowed`.
    pub fn new(
        engine: &Engine,
        component: &Component,
        allowed: SocketAddr,
        limits: WasmLimits,
    ) -> anyhow::Result<Self> {
        let mut linker: Linker<State> = Linker::new(engine);
        w(wasmtime_wasi::p2::add_to_linker_sync(&mut linker))?;
        w(Sink::add_to_linker::<_, HasSelf<_>>(&mut linker, |s| s))?;

        let mut b = WasiCtxBuilder::new();
        b.allow_tcp(true)
            .allow_udp(false)
            .allow_ip_name_lookup(false)
            .socket_addr_check(move |addr, use_| {
                // NOTE: wasi-sockets binds the client socket to an unspecified
                // address with an ephemeral port before connecting.
                let ok = match use_ {
                    SocketAddrUse::TcpConnect => addr == allowed,
                    SocketAddrUse::TcpBind => addr.ip().is_unspecified() && addr.port() == 0,
                    _ => false,
                };
                if !ok {
                    eprintln!("sandbox: denied {use_:?} {addr}");
                }
                Box::pin(async move { ok })
            });
        let state = State {
            wasi: b.build(),
            table: ResourceTable::new(),
            limits: StoreLimitsBuilder::new()
                .memory_size(limits.memory_bytes)
                .trap_on_grow_failure(true)
                .build(),
        };
        let mut store = Store::new(engine, state);
        store.limiter(|s| &mut s.limits);
        store.set_epoch_deadline(limits.call_ticks);
        store.epoch_deadline_trap();
        if limits.fuel.is_some() {
            // Instantiation runs guest code and consumes fuel too.
            w(store.set_fuel(u64::MAX / 2))?;
        }
        let sink =w(Sink::instantiate(&mut store, component, &linker))?;
        Ok(Wasm {
            store,
            sink,
            call_ticks: limits.call_ticks,
            fuel: limits.fuel,
            fuel_used: 0,
            calls: 0,
        })
    }

    fn arm(&mut self, input_bytes: usize) -> u64 {
        self.store.set_epoch_deadline(self.call_ticks);
        match self.fuel {
            Some((base, per_byte)) => {
                let budget = base + per_byte * u64::try_from(input_bytes).unwrap();
                self.store.set_fuel(budget).expect("fuel enabled");
                budget
            }
            None => 0,
        }
    }

    fn disarm(&mut self, budget: u64, input_bytes: usize) {
        if self.fuel.is_some() {
            let used = budget - self.store.get_fuel().unwrap_or(0);
            self.fuel_used += used;
            self.calls += 1;
            if self.calls % 200 == 0 {
                eprintln!(
                    "fuel: calls={} total={} last_call={} last_bytes={}",
                    self.calls, self.fuel_used, used, input_bytes
                );
            }
        }
    }
}

fn flatten<T>(r: wasmtime::Result<Result<T, String>>) -> Result<T, String> {
    match r {
        Ok(r) => r,
        Err(trap) => Err(format!("trap: {trap:?}")),
    }
}

impl Guest for Wasm {
    fn open(&mut self, config: &str) -> Result<Option<u64>, String> {
        let b = self.arm(config.len());
        let r = flatten(self.sink.call_open_sink(&mut self.store, config));
        self.disarm(b, config.len());
        r
    }
    fn reset(&mut self) -> Result<(), String> {
        let b = self.arm(0);
        let r = flatten(self.sink.call_reset_sink(&mut self.store));
        self.disarm(b, 0);
        r
    }
    fn write(&mut self, batch: &[u8], commit: Option<u64>) -> Result<(), String> {
        let b = self.arm(batch.len());
        let r = flatten(self.sink.call_write_batch(&mut self.store, batch, commit));
        self.disarm(b, batch.len());
        r
    }
}

pub fn engine(fuel: bool) -> anyhow::Result<Engine> {
    let mut cfg = Config::new();
    cfg.epoch_interruption(true);
    cfg.consume_fuel(fuel);
    let engine = w(Engine::new(&cfg))?;
    let e = engine.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_millis(1000 / TICKS_PER_SEC));
            e.increment_epoch();
        }
    });
    Ok(engine)
}

pub fn load_component(engine: &Engine, path: &Path) -> anyhow::Result<Component> {
    w(Component::from_file(engine, path)).with_context(|| format!("loading {}", path.display()))
}

enum Req {
    Open(String, oneshot::Sender<Result<Option<u64>, String>>),
    Reset(oneshot::Sender<Result<(), String>>),
    Write(Vec<u8>, Option<u64>, oneshot::Sender<Result<(), String>>),
}

/// A guest running on its own thread. Calls fail after `deadline`; the thread
/// is then abandoned because a call blocked in a host socket operation cannot
/// be interrupted.
pub struct GuestThread {
    tx: std::sync::mpsc::Sender<Req>,
    deadline: Duration,
    pub poisoned: Arc<AtomicBool>,
}

impl GuestThread {
    pub fn spawn<F>(make: F, deadline: Duration) -> Self
    where
        F: FnOnce() -> anyhow::Result<Box<dyn Guest>> + Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::channel::<Req>();
        let poisoned = Arc::new(AtomicBool::new(false));
        let p = Arc::clone(&poisoned);
        std::thread::Builder::new()
            .name("sink-guest".into())
            .spawn(move || {
                let mut g = match make() {
                    Ok(g) => g,
                    Err(e) => {
                        eprintln!("guest init failed: {e:#}");
                        p.store(true, Ordering::SeqCst);
                        return;
                    }
                };
                while let Ok(req) = rx.recv() {
                    match req {
                        Req::Open(c, r) => {
                            let _ = r.send(g.open(&c));
                        }
                        Req::Reset(r) => {
                            let _ = r.send(g.reset());
                        }
                        Req::Write(b, c, r) => {
                            let _ = r.send(g.write(&b, c));
                        }
                    }
                }
            })
            .expect("spawn guest thread");
        GuestThread {
            tx,
            deadline,
            poisoned,
        }
    }

    async fn call<T>(
        &self,
        req: impl FnOnce(oneshot::Sender<Result<T, String>>) -> Req,
    ) -> anyhow::Result<T> {
        let (rtx, rrx) = oneshot::channel();
        self.tx
            .send(req(rtx))
            .map_err(|_| anyhow!("guest thread gone"))?;
        match tokio::time::timeout(self.deadline, rrx).await {
            Err(_) => Err(anyhow!("guest call exceeded deadline {:?}", self.deadline)),
            Ok(Err(_)) => Err(anyhow!("guest thread dropped call")),
            Ok(Ok(r)) => r.map_err(|e| anyhow!("guest error: {e}")),
        }
    }

    pub async fn open(&self, config: String) -> anyhow::Result<Option<u64>> {
        self.call(|r| Req::Open(config, r)).await
    }
    pub async fn reset(&self) -> anyhow::Result<()> {
        self.call(Req::Reset).await
    }
    pub async fn write(&self, batch: Vec<u8>, commit: Option<u64>) -> anyhow::Result<()> {
        self.call(|r| Req::Write(batch, commit, r)).await
    }
}
