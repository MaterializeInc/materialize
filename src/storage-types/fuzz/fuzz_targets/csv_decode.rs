// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `CsvDecoderState` decodes untrusted CSV bytes (a source object's
//! contents) into `Row`s. This is the `FORMAT CSV` source decoder — first to
//! touch external data — and was previously unfuzzed. A panic reachable from the
//! bytes is a source-ingestion availability bug; decoding must only ever return
//! `Ok`/`Err`.
//!
//! The first two bytes pick the decoder config (column count 1..=4, the field
//! delimiter, and whether the first row is a validated header); the rest is fed
//! as the CSV stream, draining it through `decode` exactly as the storage decode
//! operator does. This exercises the buffer-growth (`OutputFull`/`OutputEndsFull`),
//! column-count-mismatch, invalid-UTF-8, and header-validation paths.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_storage_types::sources::encoding::{ColumnSpec, CsvDecoderState, CsvEncoding};

fuzz_target!(|data: &[u8]| {
    // First two bytes are the config; the remainder is the CSV stream.
    if data.len() < 2 {
        return;
    }
    let (cfg, mut chunk) = data.split_at(2);
    let n_cols = usize::from(cfg[0] % 4) + 1;
    // Usually the standard comma, but sometimes an arbitrary delimiter byte
    // (which csv_core accepts) to reach unusual framing.
    let delimiter = if cfg[1] & 1 == 0 { b',' } else { cfg[1] };
    let columns = if cfg[1] & 2 != 0 {
        ColumnSpec::Header {
            names: (0..n_cols).map(|i| format!("c{i}")).collect(),
        }
    } else {
        ColumnSpec::Count(n_cols)
    };

    let mut state = CsvDecoderState::new(CsvEncoding { columns, delimiter });

    // Drain the stream a record at a time, like the decode operator. Each call
    // returns one decoded record (or an error for a malformed one, having
    // consumed it) until the input is exhausted (`Ok(None)`). The progress guard
    // is belt-and-suspenders against a non-advancing call.
    loop {
        let before = chunk.len();
        match state.decode(&mut chunk) {
            Ok(None) => break,
            Ok(Some(_)) | Err(_) => {
                if chunk.len() == before {
                    break;
                }
            }
        }
    }
});
