// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Basic unit tests for sources.

use std::collections::BTreeMap;

use mz_storage::source::testscript::ScriptCommand;
use mz_storage_types::sources::encoding::SourceDataEncoding;
use mz_storage_types::sources::SourceEnvelope;

mod setup;

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rocksdb_create_default_env` on OS `linux`
fn test_datadriven() {
    datadriven::walk("tests/datadriven", |f| {
        let mut sources: BTreeMap<
            String,
            (Vec<ScriptCommand>, SourceDataEncoding, SourceEnvelope),
        > = BTreeMap::new();

        // Note we unwrap and panic liberally here as we
        // expect tests to be properly written.
        f.run(move |tc| -> String {
            match tc.directive.as_str() {
                "register-source" => {
                    // we just use the serde json representations.
                    let source: serde_json::Value = serde_json::from_str(&tc.input).unwrap();
                    let source = source.as_object().unwrap();
                    sources.insert(
                        tc.args["name"][0].clone(),
                        (
                            serde_json::from_value(source["script"].clone()).unwrap(),
                            serde_json::from_value(source["encoding"].clone()).unwrap(),
                            serde_json::from_value(source["envelope"].clone()).unwrap(),
                        ),
                    );

                    "<empty>\n".to_string()
                }
                "run-source" => {
                    let (script, encoding, envelope) = sources[&tc.args["name"][0]].clone();

                    // We just use the `Debug` representation here.
                    // REWRITE=true makes this reasonable!
                    format!(
                        "{:#?}\n",
                        setup::run_script_source(
                            script,
                            encoding,
                            envelope,
                            tc.args["expected_len"][0].parse().unwrap(),
                        )
                        .unwrap()
                    )
                }
                _ => panic!("unknown directive {:?}", tc),
            }
        })
    });
}
