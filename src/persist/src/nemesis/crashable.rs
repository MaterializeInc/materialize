// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use ore::metrics::MetricsRegistry;

use crate::error::Error;
use crate::file::{FileBlob, FileLog};
use crate::indexed::runtime;
use crate::nemesis::direct::Direct;
use crate::nemesis::{crashable_helper, Input, Req, ReqId, Res, ResError, Runtime, Step};
use crate::unreliable::{UnreliableBlob, UnreliableLog};

pub struct CrashableRuntime {
    client: crashable_helper::Client<CrashableRuntimeServer>,
}

impl CrashableRuntime {
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Result<Self, Error> {
        let args = &[format!("--base_dir={}", base_dir.as_ref().display())];
        let client = crashable_helper::Client::boot(args)?;
        Ok(CrashableRuntime { client })
    }

    fn run_req(&mut self, req: &Req) -> Result<Res, ResError> {
        let res = match req {
            Req::Start => {
                // TODO: Need to keep track of unreliability..
                self.client.start()?;
                Res::Start(Ok(()))
            }
            Req::Stop => {
                self.client.sigkill();
                Res::Stop(Ok(()))
            }
            _ => self.client.run(req)?,
        };
        Ok(res)
    }
}

impl Runtime for CrashableRuntime {
    fn run(&mut self, input: Input) -> Step {
        let req_id = input.req_id;
        let res = self.run_req(&input.req);
        let res = res.unwrap_or_else(|err| input.req.into_error_res(err));
        Step { req_id, res }
    }

    fn finish(mut self) {
        self.client.sigkill();
    }
}

pub struct CrashableRuntimeServer {
    direct: Direct,
}

impl crashable_helper::Server for CrashableRuntimeServer {
    type Req = Req;
    type Res = Res;

    fn start(args: Vec<String>) -> Result<Self, String> {
        for arg in args.iter() {
            if arg.starts_with("--base_dir=") {
                let base_dir = PathBuf::from(arg.replace("--base_dir=", ""));
                let direct = Direct::new(move |unreliable| {
                    let (log_dir, blob_dir) = (base_dir.join("log"), base_dir.join("blob"));
                    let log = FileLog::new(log_dir, ("reentrance0", "process_file").into())?;
                    let log = UnreliableLog::from_handle(log, unreliable.clone());
                    let blob = FileBlob::new(blob_dir, ("reentrance0", "process_file").into())?;
                    let blob = UnreliableBlob::from_handle(blob, unreliable);
                    runtime::start(log, blob, &MetricsRegistry::new())
                })
                .map_err(|err| err.to_string())?;
                return Ok(CrashableRuntimeServer { direct });
            }
        }
        return Err(format!("missing --base_dir from args: {}", args.join(" ")));
    }

    fn run(&mut self, req: Self::Req) -> Self::Res {
        self.direct
            .run(Input {
                req_id: ReqId(0),
                req: req,
            })
            .res
    }
}

#[cfg(test)]
mod tests {
    use crate::nemesis;
    use crate::nemesis::generator::GeneratorConfig;

    use super::*;

    #[test]
    fn crashable_file() -> Result<(), Error> {
        let base_dir = tempfile::tempdir().expect("tempdir creation failed");
        let runtime = CrashableRuntime::new(&base_dir)?;
        let mut config = GeneratorConfig::default();
        // TODO: CrashableRuntime doesn't quite do the right thing for these yet.
        config.storage_available = 0;
        config.storage_unavailable = 0;
        nemesis::run(10, config, runtime);
        Ok(())
    }

    // NB: This test has to be named exactly "child_process_hack" for the
    // killable_test stuff to work.
    #[test]
    #[ignore]
    fn crashable_helper_hack() {
        crashable_helper::run_child_process_hack::<CrashableRuntimeServer>();
    }
}
