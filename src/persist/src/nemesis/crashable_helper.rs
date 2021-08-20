// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for writing unit tests with unclean interruption.
//!
//! The #[ignore] test attribute is abused to run an implementation of [Server]
//! in another process without needing a second binary. A [Client] can then talk
//! to this server, stop it uncleanly using SIGKILL, and start it again at will.
//! To make this work, a special unit test needs to live somewhere in the crate,
//! so we have somewhere to hook the server startup to. It must look exactly
//! like the following (including being named "child_process_hack"):
//!
//! ```no_run
//! #[test]
//! #[ignore]
//! fn child_process_hack() {
//!     crashable_helper::run_child_process_hack::<impl of Server>();
//! }
//! ```
//!
//! At the moment, there can only be one of these per crate, but this is
//! fixable.

use std::ffi::OsString;
use std::io::{BufRead, BufReader, Write};
use std::marker::PhantomData;
use std::process::{Child, ChildStderr, ChildStdin, Command, Stdio};
use std::{env, io, process, thread};

use ore::test::init_logging;
use serde::de::DeserializeOwned;
use serde::Serialize;

const CHILD_PROCESS_HACK: &'static str = "child_process_hack";
const CHILD_PROCESS_ARG_PREFIX: &'static str = "crashable_helper_arg_";

struct ClientChild {
    child: Child,
    stdin: ChildStdin,
    stderr: BufReader<ChildStderr>,
}

pub struct Client<S: Server> {
    program: OsString,
    args: Vec<OsString>,
    child: Option<ClientChild>,
    _phantom: PhantomData<S>,
}

impl<S: Server> Client<S> {
    pub fn boot<A: AsRef<str>, I: IntoIterator<Item = A>>(args: I) -> Result<Self, String> {
        init_logging();
        let program = env::args_os().next().unwrap();
        let mut child_args = vec![
            "--nocapture".into(),
            "--ignored".into(),
            "--".into(),
            "child_process_hack".into(),
        ];
        for arg in args {
            child_args.push(format!("{}{}", CHILD_PROCESS_ARG_PREFIX, arg.as_ref()).into());
        }
        let mut ret = Client {
            program,
            args: child_args,
            child: None,
            _phantom: PhantomData,
        };
        ret.start()?;
        Ok(ret)
    }

    pub fn run(&mut self, req: &S::Req) -> Result<S::Res, String> {
        let child = self.child.as_mut().ok_or("runtime unavailable")?;
        let req_str = serde_json::to_string(req).map_err(|err| err.to_string())?;
        child
            .stdin
            .write_all(&req_str.as_bytes())
            .map_err(|err| err.to_string())?;
        child
            .stdin
            .write_all("\n".as_bytes())
            .map_err(|err| err.to_string())?;
        let mut res = String::new();
        child
            .stderr
            .read_line(&mut res)
            .map_err(|err| err.to_string())?;
        let res = res.trim_end();
        let res: S::Res = serde_json::from_str(&res)
            .map_err(|err| format!("invalid request [{}]: {}", res, err))?;
        Ok(res)
    }

    pub fn start(&mut self) -> Result<(), String> {
        if self.child.is_some() {
            self.sigkill();
            debug_assert!(self.child.is_none());
        }
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| err.to_string())?;
        let stdin = child.stdin.take().ok_or("stdin unavailable")?;
        let stdout = child.stdout.take().ok_or("stdout unavailable")?;
        let stderr = child.stderr.take().ok_or("stderr unavailable")?;

        // Capture stdout and replay it through println! to keep the
        // cargo test output on the parent clean.
        thread::spawn(move || {
            let stdout = BufReader::new(stdout);
            for line in stdout.lines() {
                match line {
                    Err(_) => break,
                    Ok(line) => {
                        // Sniff things that look like log lines and pass them
                        // through unchanged, otherwise, print with a prefix.
                        if line.starts_with("2021-") {
                            println!("{}", line);
                        } else if line.is_empty() || line == "running 1 test" {
                            // Artifact of running a rust test.
                        } else {
                            println!("child stdout: {}", line);
                        }
                    }
                }
            }
        });

        // Communicate responses back from the child process using stderr
        // because rust test execution output and ore::test::init_logging() both
        // write to stdout.
        self.child = Some(ClientChild {
            child,
            stdin,
            stderr: BufReader::new(stderr),
        });
        Ok(())
    }

    pub fn sigkill(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Err(err) = child.child.kill() {
                log::debug!("failed to clean up child process: {}", err)
            }
        }
    }
}

impl<S: Server> Drop for Client<S> {
    fn drop(&mut self) {
        self.sigkill()
    }
}

pub fn run_child_process_hack<S: Server>() {
    let in_child = env::args().any(|x| x == CHILD_PROCESS_HACK);
    if !in_child {
        return;
    }
    init_logging();
    let args = env::args()
        .flat_map(|arg| {
            if arg.starts_with(CHILD_PROCESS_ARG_PREFIX) {
                Some(arg.replace(CHILD_PROCESS_ARG_PREFIX, ""))
            } else {
                None
            }
        })
        .collect();
    ChildProcessServer::<S>::create_and_run(args);
}

// The client and server communicate with each other over the child process's
// stdin and stderr, so implementations of both `start` and `run` must avoid
// printing to stderr.
pub trait Server: Sized {
    type Req: Serialize + DeserializeOwned;
    type Res: Serialize + DeserializeOwned;

    fn start(args: Vec<String>) -> Result<Self, String>;
    fn run(&mut self, req: Self::Req) -> Self::Res;
}

struct ChildProcessServer<S: Server> {
    server: S,
}

impl<S: Server> ChildProcessServer<S> {
    pub fn create_and_run(args: Vec<String>) {
        log::debug!("server starting with args: {}", args.join(" "));
        let server = S::start(args).unwrap_or_else(|err| {
            log::error!("failure during server create: {}", err);
            process::exit(1);
        });
        let mut server = ChildProcessServer { server };
        if let Err(err) = server.run() {
            log::error!("failure during server run: {}", err);
            process::exit(1);
        }
    }

    fn run(&mut self) -> Result<(), String> {
        let (stdin, stderr) = (io::stdin(), io::stderr());
        let stdin = stdin.lock().lines();
        let mut stderr = stderr.lock();
        for req in stdin {
            let req = req.map_err(|err| err.to_string())?;
            log::debug!("server got  req: {}", req);
            let req: S::Req = serde_json::from_str(&req).map_err(|err| err.to_string())?;
            let res = self.server.run(req);
            let res = serde_json::to_string(&res).map_err(|err| err.to_string())?;
            log::debug!("server send res: {}", res);
            stderr
                .write_all(res.as_bytes())
                .map_err(|err| err.to_string())?;
            stderr
                .write_all("\n".as_bytes())
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }
}
