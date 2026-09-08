// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `CsvDecoderState` decodes untrusted CSV bytes (a source object's
//! contents) into `Row`s. This is the `FORMAT CSV` source decoder, the first to
//! touch external data. A panic reachable from the bytes is a source-ingestion
//! availability bug. Decoding must only ever return `Ok`/`Err`.
//!
//! The first two bytes pick the decoder config (column count 1..=4, the field
//! delimiter, and whether the first row is a validated header). The rest is fed
//! as a CSV object. Once its ordinary input is consumed, the target makes the
//! empty-input EOF call and resets the decoder as the storage decode operator
//! does. It then reuses the state for a second object. This exercises the
//! buffer-growth (`OutputFull`/`OutputEndsFull`), column-count-mismatch,
//! invalid-UTF-8, header-validation, EOF-finalization, and object-reset paths.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_storage_types::sources::encoding::{ColumnSpec, CsvDecoderState, CsvEncoding};

fn decode_object(state: &mut CsvDecoderState, mut chunk: &[u8]) {
    // Drain ordinary input a record at a time. Leave the empty slice for the
    // explicit EOF call below.
    while !chunk.is_empty() {
        let before = chunk.len();
        match state.decode(&mut chunk) {
            Ok(None) => break,
            Ok(Some(_)) | Err(_) => {
                if chunk.len() == before {
                    return;
                }
            }
        }
    }

    if chunk.is_empty() {
        // csv_core needs a separate empty-input call to emit or reject a final
        // record without a line terminator.
        let _ = state.decode(&mut chunk);
        state.reset_for_new_object();
    }
}

fuzz_target!(|data: &[u8]| {
    // First two bytes are the config, the remainder is the CSV stream.
    if data.len() < 2 {
        return;
    }
    let (cfg, input) = data.split_at(2);
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
    decode_object(&mut state, input);
    decode_object(&mut state, &input[input.len() / 2..]);
});
