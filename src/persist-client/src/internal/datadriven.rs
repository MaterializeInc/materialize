// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Common abstractions for persist datadriven tests.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use differential_dataflow::trace::Description;
use timely::progress::Antichain;
use tokio::sync::Mutex;

use crate::internal::paths::PartialBatchKey;
use crate::internal::state::{HollowBatch, HollowBatchPart};

/// A [datadriven::TestCase] wrapper with helpers for parsing.
pub struct DirectiveArgs<'a> {
    pub args: &'a HashMap<String, Vec<String>>,
    pub input: &'a str,
}

impl<'a> DirectiveArgs<'a> {
    #[track_caller]
    pub fn optional_str(&self, name: &str) -> Option<&'a str> {
        self.args.get(name).map(|vals| {
            if vals.is_empty() {
                return "";
            }
            if vals.len() != 1 {
                panic!("unexpected values for {}: {:?}", name, vals);
            }
            vals[0].as_ref()
        })
    }

    #[track_caller]
    pub fn optional<T: FromStr>(&self, name: &str) -> Option<T> {
        self.optional_str(name).map(|x| {
            x.parse::<T>()
                .unwrap_or_else(|_| panic!("invalid {}: {}", name, x))
        })
    }

    #[track_caller]
    pub fn optional_antichain(&self, name: &str) -> Option<Antichain<u64>> {
        self.optional_str(name).map(Self::parse_antichain)
    }

    #[track_caller]
    pub fn expect_str(&self, name: &str) -> &str {
        self.optional_str(name)
            .unwrap_or_else(|| panic!("missing {}", name))
    }

    #[track_caller]
    pub fn expect<T: FromStr>(&self, name: &str) -> T {
        self.optional(name)
            .unwrap_or_else(|| panic!("missing {}", name))
    }

    #[track_caller]
    pub fn expect_antichain(&self, name: &str) -> Antichain<u64> {
        self.optional_antichain(name)
            .unwrap_or_else(|| panic!("missing {}", name))
    }

    #[track_caller]
    pub fn parse_update(x: &str) -> Option<((String, ()), u64, i64)> {
        let x = x.trim();
        if x.is_empty() {
            return None;
        }
        let parts = x.split(' ').collect::<Vec<_>>();
        if parts.len() != 3 {
            panic!("unexpected update: {}", x);
        }
        let (key, ts, diff) = (parts[0], parts[1], parts[2]);
        let ts = ts.parse::<u64>().expect("invalid ts");
        let diff = diff.parse::<i64>().expect("invalid diff");
        Some(((key.to_owned(), ()), ts, diff))
    }

    #[track_caller]
    pub fn parse_hollow_batch(x: &str) -> HollowBatch<u64> {
        let parts = x.trim().split(' ').collect::<Vec<_>>();
        assert!(
            parts.len() >= 2,
            "usage: [<lower>][<upper>][<since>] <len> <key0> <key1> ... <keyN>"
        );
        let (desc, len, keys) = (parts[0], parts[1], &parts[2..]);
        let desc = Self::parse_desc(desc);
        let len = len.parse().expect("invalid len");
        HollowBatch {
            desc,
            len,
            parts: keys
                .iter()
                .map(|x| HollowBatchPart {
                    key: PartialBatchKey((*x).to_owned()),
                    encoded_size_bytes: 0,
                })
                .collect(),
            runs: vec![],
        }
    }

    #[track_caller]
    pub fn parse_desc(x: &str) -> Description<u64> {
        let parts = x
            .strip_prefix('[')
            .expect("usage: [<lower>][<upper>][<since>]")
            .strip_suffix(']')
            .expect("usage: [<lower>][<upper>][<since>]")
            .split("][")
            .collect::<Vec<_>>();
        assert!(parts.len() == 3, "usage: [<lower>][<upper>][<since>]");
        let (lower, upper, since) = (&parts[0], &parts[1], &parts[2]);
        Description::new(
            Self::parse_antichain(lower),
            Self::parse_antichain(upper),
            Self::parse_antichain(since),
        )
    }

    #[track_caller]
    pub fn parse_antichain(x: &str) -> Antichain<u64> {
        if x.is_empty() {
            Antichain::new()
        } else {
            Antichain::from_elem(x.parse().expect("invalid ts"))
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn trace() {
        use crate::internal::trace::datadriven as trace_dd;

        datadriven::walk("tests/trace", |f| {
            let mut state = trace_dd::TraceState::default();
            f.run(move |tc| -> String {
                let args = DirectiveArgs {
                    args: &tc.args,
                    input: &tc.input,
                };
                let res = match tc.directive.as_str() {
                    "apply-merge-res" => trace_dd::apply_merge_res(&mut state, args),
                    "downgrade-since" => trace_dd::downgrade_since(&mut state, args),
                    "push-batch" => trace_dd::insert(&mut state, args),
                    "since-upper" => trace_dd::since_upper(&mut state, args),
                    "spine-batches" => trace_dd::batches(&mut state, args),
                    "take-merge-reqs" => trace_dd::take_merge_req(&mut state, args),
                    _ => panic!("unknown directive {:?}", tc),
                };
                match res {
                    Ok(x) if x.is_empty() => "<empty>\n".into(),
                    Ok(x) => x,
                    Err(err) => format!("error: {:?}\n", err),
                }
            })
        });
    }

    #[tokio::test]
    async fn machine() {
        use crate::internal::machine::datadriven as machine_dd;

        ::datadriven::walk_async("tests/machine", |mut f| {
            let initial_state_fut = machine_dd::MachineState::new();
            async move {
                let state = Arc::new(Mutex::new(initial_state_fut.await));
                f.run_async(move |tc| {
                    let state = Arc::clone(&state);
                    async move {
                        let args = DirectiveArgs {
                            args: &tc.args,
                            input: &tc.input,
                        };
                        let mut state = state.lock().await;
                        let res = match tc.directive.as_str() {
                            "apply-merge-res" => {
                                machine_dd::apply_merge_res(&mut state, args).await
                            }
                            "blob-scan-batches" => {
                                machine_dd::blob_scan_batches(&mut state, args).await
                            }
                            "compact" => machine_dd::compact(&mut state, args).await,
                            "compare-and-append" => {
                                machine_dd::compare_and_append(&mut state, args).await
                            }
                            "consensus-scan" => machine_dd::consensus_scan(&mut state, args).await,
                            "downgrade-since" => {
                                machine_dd::downgrade_since(&mut state, args).await
                            }
                            "expire-reader" => machine_dd::expire_reader(&mut state, args).await,
                            "expire-writer" => machine_dd::expire_writer(&mut state, args).await,
                            "fetch-batch" => machine_dd::fetch_batch(&mut state, args).await,
                            "gc" => machine_dd::gc(&mut state, args).await,
                            "heartbeat-reader" => {
                                machine_dd::heartbeat_reader(&mut state, args).await
                            }
                            "heartbeat-writer" => {
                                machine_dd::heartbeat_writer(&mut state, args).await
                            }
                            "listen-through" => machine_dd::listen_through(&mut state, args).await,
                            "perform-maintenance" => {
                                machine_dd::perform_maintenance(&mut state, args).await
                            }
                            "register-listen" => {
                                machine_dd::register_listen(&mut state, args).await
                            }
                            "register-reader" => {
                                machine_dd::register_reader(&mut state, args).await
                            }
                            "register-writer" => {
                                machine_dd::register_writer(&mut state, args).await
                            }
                            "set-batch-parts-size" => {
                                machine_dd::set_batch_parts_size(&mut state, args).await
                            }
                            "snapshot" => machine_dd::snapshot(&mut state, args).await,
                            "truncate-batch-desc" => {
                                machine_dd::truncate_batch_desc(&mut state, args).await
                            }
                            "write-batch" => machine_dd::write_batch(&mut state, args).await,
                            _ => panic!("unknown directive {:?}", tc),
                        };
                        match res {
                            Ok(x) if x.is_empty() => "<empty>\n".into(),
                            Ok(x) => x,
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                })
                .await;
                f
            }
        })
        .await;
    }
}
