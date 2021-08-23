// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;
use std::{cmp, env, process};

use ore::metrics::MetricsRegistry;
use persist::file::{FileBlob, FileLog};
use persist::indexed::runtime::{self, RuntimeClient, StreamReadHandle};
use persist::operators::source::PersistedSource;
use persist::operators::stream::Persist;
use persist::storage::LockInfo;
use persist::Data;
use timely::dataflow::operators::generic::{operator, source};
use timely::dataflow::operators::{Concat, Inspect, Map, ToStream};
use timely::dataflow::{Scope, Stream};

fn construct_persistent_kafka_upsert_source<G: Scope<Timestamp = u64>>(
    scope: &mut G,
    persist: RuntimeClient,
    name_base: &str,
) -> Result<
    (
        Stream<G, ((String, String), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    ),
    Box<dyn Error>,
> {
    let (_ts_write, ts_read) =
        persist.create_or_load::<KafkaOffset, ()>(&format!("{}_ts", name_base))?;
    let (out_write, out_read) =
        persist.create_or_load::<String, String>(&format!("{}_out", name_base))?;
    let start_ts = cmp::min(sealed_ts(&ts_read)?, sealed_ts(&out_read)?);

    // Reload upsert state.
    // - TODO: Make this a third stream
    // - TODO: Do this as of start_ts
    // - TODO: Instead of the same one of these per worker, make them data
    //   parallel in the same way the computation is
    // - TODO: This needs to be respectful of ts and diff
    // - TODO: Don't use read_to_end_flattened
    let mut prev_value_by_key = HashMap::new();
    for ((k, v), _, _) in out_read.snapshot()?.read_to_end_flattened()? {
        prev_value_by_key.insert(k, v);
    }

    // Compute start offset.
    // - TODO: Don't use read_to_end_flattened
    // - TODO: Is this even actually how to find the right start offset?
    let mut start_offset = KafkaOffset(0);
    for ((offset, _), ts, _) in ts_read.snapshot()?.read_to_end_flattened()? {
        if ts > start_ts {
            continue;
        }
        start_offset = cmp::max(start_offset, offset);
    }

    let (out_ok_prev, out_ok_err) = scope.persisted_source(&out_read);

    let (out_ok_new, out_err_new) = fake_kafka(scope, start_offset)
        .map(|(kv, ts, diff)| {
            // TODO: Reclocking
            (kv, ts.0, diff)
        })
        .persist((out_write, out_read));

    let ok_stream = out_ok_new.concat(&out_ok_prev);
    let err_stream = out_err_new.concat(&out_ok_err);
    Ok((ok_stream, err_stream))
}

// HACK: This should be a method on StreamReadHandle that actually queries the
// runtime.
fn sealed_ts<K: Data, V: Data>(read: &StreamReadHandle<K, V>) -> Result<u64, Box<dyn Error>> {
    let mut sealed = 0;
    for (_, ts, _) in read.snapshot()?.read_to_end_flattened()? {
        sealed = cmp::max(sealed, ts);
    }
    Ok(sealed)
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Default)]
struct KafkaOffset(u64);

fn fake_kafka<G: Scope<Timestamp = u64>>(
    scope: &mut G,
    starting_offset: KafkaOffset,
) -> Stream<G, ((String, String), KafkaOffset, isize)> {
    source(scope, "fake_kafka", |mut cap, info| {
        let mut offset = starting_offset.0;
        let activator = scope.activator_for(&info.address[..]);
        move |output| {
            cap.downgrade(&offset);
            let kv = (format!("k{}", offset % 10), format!("v{}", offset));
            output.session(&cap).give((kv, KafkaOffset(offset), 1));
            offset += 1;
            activator.activate_after(Duration::from_secs(1));
        }
    })
}

fn run(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    if args.len() != 2 {
        Err(format!("usage: {} <persist_dir>", &args[0]))?;
    }
    let base_dir = PathBuf::from(&args[1]);
    let persist = {
        let lock_info = LockInfo::new("kafka_upsert".into(), "nonce".into())?;
        let log = FileLog::new(base_dir.join("log"), lock_info.clone())?;
        let blob = FileBlob::new(base_dir.join("blob"), lock_info)?;
        runtime::start(log, blob, &MetricsRegistry::new())?
    };

    timely::execute_directly(|worker| {
        worker.dataflow(|scope| {
            let (ok_stream, err_stream) =
                construct_persistent_kafka_upsert_source(scope, persist, "persistent_kafka_1")
                    .unwrap_or_else(|err| {
                        let ok_stream = operator::empty(scope);
                        let err_stream = vec![(err.to_string(), 0, 1)].to_stream(scope);
                        (ok_stream, err_stream)
                    });
            ok_stream.inspect(|d| println!("ok: {:?}", d));
            err_stream.inspect(|d| println!("err: {:?}", d));
        })
    });

    Ok(())
}

fn main() {
    if let Err(err) = run(env::args().collect()) {
        eprintln!("error: {}", err);
        process::exit(1);
    }
}

mod kafka_offset_impls {
    use std::convert::TryFrom;

    use persist::Codec;

    use crate::KafkaOffset;

    impl Codec for KafkaOffset {
        fn codec_name() -> &'static str {
            "KafkaOffset"
        }

        fn size_hint(&self) -> usize {
            8
        }

        fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
            buf.extend(&self.0.to_le_bytes())
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(KafkaOffset(u64::from_le_bytes(
                <[u8; 8]>::try_from(buf).map_err(|err| err.to_string())?,
            )))
        }
    }
}
