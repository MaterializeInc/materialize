// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Basic unit tests for sources.

use mz_repr::{Datum, Row};
use mz_storage::source::testscript::ScriptCommand;
use mz_storage::types::sources::{
    encoding::{DataEncoding, DataEncodingInner, SourceDataEncoding},
    KeyEnvelope, NoneEnvelope, SourceData, SourceEnvelope,
};

mod setup;

#[test]
fn test_basic() -> Result<(), anyhow::Error> {
    let script = vec![ScriptCommand::Emit {
        value: "gus".to_string(),
        key: None,
        offset: 0,
    }];

    setup::assert_source_results_in(
        script,
        SourceDataEncoding::Single(DataEncoding {
            force_nullable_columns: false,
            inner: DataEncodingInner::Bytes,
        }),
        SourceEnvelope::None(NoneEnvelope {
            key_envelope: KeyEnvelope::None,
            key_arity: 0,
        }),
        vec![SourceData(Ok(Row::pack([Datum::Bytes(b"gus")])))],
    )
}

#[test]
fn test_basic_failing() {
    let script = vec![ScriptCommand::Emit {
        value: "gus".to_string(),
        key: None,
        offset: 0,
    }];

    assert!(setup::assert_source_results_in(
        script,
        SourceDataEncoding::Single(DataEncoding {
            force_nullable_columns: false,
            inner: DataEncodingInner::Bytes,
        }),
        SourceEnvelope::None(NoneEnvelope {
            key_envelope: KeyEnvelope::None,
            key_arity: 0,
        }),
        vec![SourceData(Ok(Row::pack([Datum::Bytes(b"gus2")])))],
    )
    .is_err())
}
