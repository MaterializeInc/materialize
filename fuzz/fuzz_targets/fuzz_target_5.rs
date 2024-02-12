#![no_main]

use libfuzzer_sys::fuzz_target;

use std::cell::RefCell;
use std::fmt;
use std::fs::File;
use std::io::{self, Write, Stdout, Stderr};
use std::path::PathBuf;
use std::process::ExitCode;
use std::collections::BTreeMap;

use chrono::Utc;
use tokio::runtime::Runtime;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig, KeyValueArg};
use mz_ore::metrics::MetricsRegistry;
use mz_sqllogictest::runner::{self, Outcomes, RunConfig, Runner, WriteFmt, run_string};
use mz_sqllogictest::util;
use mz_tracing::CloneableEnvFilter;
use walkdir::WalkDir;

struct State {
    stdout: OutputStream<Stdout>,
    stderr: OutputStream<Stderr>,
    config: Option<RunConfig<'static>>,
    runner: Option<Runner<'static>>
}

async fn init() {
    let tracing_args = TracingCliArgs {
        ..Default::default()
    };
    let (tracing_handle, _tracing_guard) = tracing_args
        .configure_tracing(
            StaticTracingConfig {
                service_name: "sqllogictest",
                build_info: mz_environmentd::BUILD_INFO,
            },
            MetricsRegistry::new(),
        )
        .await
        .unwrap();

    STATE.with(|state| async {
        let mut state = state.borrow();
        state.config = Some(RunConfig {
            stdout: &state.stdout,
            stderr: &state.stderr,
            verbosity: 3,
            postgres_url: "postgres://root@localhost:26257".to_string(),
            no_fail: true,
            fail_fast: false,
            auto_index_tables: false,
            auto_index_selects: false,
            auto_transactions: false,
            enable_table_keys: false,
            orchestrator_process_wrapper: None,
            tracing: tracing_args.clone(),
            tracing_handle,
            system_parameter_defaults: BTreeMap::new(),
            persist_dir: tempfile::tempdir().unwrap(),
            replicas: 1,
        });
        state.runner = Some(Runner::start(&state.config.unwrap()).await.unwrap());
    });
}

thread_local!(static STATE: RefCell<State> = RefCell::new(State {
    stdout: OutputStream::new(io::stdout(), false),
    stderr: OutputStream::new(io::stderr(), false),
    config: None,
    runner: None,
}));

async fn run(s: String) {
    STATE.with(|mut state| async {
        run_string(&state.borrow().runner, "", &*s).await;
    });
}

struct OutputStream<W> {
    inner: RefCell<W>,
    need_timestamp: RefCell<bool>,
    timestamps: bool,
}

impl<W> OutputStream<W>
where
    W: Write,
{
    fn new(inner: W, timestamps: bool) -> OutputStream<W> {
        OutputStream {
            inner: RefCell::new(inner),
            need_timestamp: RefCell::new(true),
            timestamps,
        }
    }

    fn emit_str(&self, s: &str) {
        self.inner.borrow_mut().write_all(s.as_bytes()).unwrap();
    }
}

impl<W> WriteFmt for OutputStream<W>
where
    W: Write,
{
    fn write_fmt(&self, fmt: fmt::Arguments<'_>) {
        let s = format!("{}", fmt);
        if self.timestamps {
            // We need to prefix every line in `s` with the current timestamp.

            let timestamp = Utc::now();
            let timestamp_str = timestamp.format("%Y-%m-%d %H:%M:%S.%f %Z");

            // If the last character we outputted was a newline, then output a
            // timestamp prefix at the start of this line.
            if self.need_timestamp.replace(false) {
                self.emit_str(&format!("[{}] ", timestamp_str));
            }

            // Emit `s`, installing a timestamp at the start of every line
            // except the last.
            let (s, last_was_timestamp) = match s.strip_suffix('\n') {
                None => (&*s, false),
                Some(s) => (s, true),
            };
            self.emit_str(&s.replace('\n', &format!("\n[{}] ", timestamp_str)));

            // If the line ended with a newline, output the newline but *not*
            // the timestamp prefix. We want the timestamp to reflect the moment
            // the *next* character is output. So instead we just remember that
            // the last character we output was a newline.
            if last_was_timestamp {
                *self.need_timestamp.borrow_mut() = true;
                self.emit_str("\n");
            }
        } else {
            self.emit_str(&s)
        }
    }
}

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let st = s.to_string();
        rt.block_on(run(st));
    }
});
