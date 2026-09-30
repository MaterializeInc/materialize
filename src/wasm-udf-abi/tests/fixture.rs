// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Read;

use mz_wasm_udf_abi::{AbiError, ModuleInfo, ScalarSignature, UdfType};

fn fixture() -> Vec<u8> {
    let gz: &[u8] = include_bytes!("../../wasm-udf/tests/fixtures/udfs.wasm.gz");
    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(gz)
        .read_to_end(&mut bytes)
        .expect("valid gzip");
    bytes
}

#[mz_ore::test]
fn parses_fixture_module() {
    let info = ModuleInfo::parse(&fixture()).expect("fixture validates");
    assert_eq!(info.abi_version.0, 3);
    assert!(
        info.functions.contains("gcd(int32,int32)->int32"),
        "functions: {:?}",
        info.functions
    );
    assert!(
        info.functions
            .contains("jaro_winkler(string,string)->float64")
    );
    assert!(
        info.imports
            .iter()
            .all(|i| i.module == mz_wasm_udf_abi::wasi::MODULE)
    );
    let determinized: Vec<_> = info
        .determinized_imports()
        .map(|(i, _)| i.name.as_str())
        .collect();
    assert!(
        determinized.contains(&"random_get"),
        "HashMap seeding imports random_get: {determinized:?}"
    );
}

#[mz_ore::test]
fn finds_exports_by_signature() {
    let info = ModuleInfo::parse(&fixture()).unwrap();
    let gcd = ScalarSignature {
        name: "gcd".into(),
        args: vec![UdfType::Int32, UdfType::Int32],
        ret: UdfType::Int32,
    };
    assert_eq!(info.scalar_export(&gcd).unwrap(), gcd.export_name());

    let wrong = ScalarSignature {
        ret: UdfType::Int64,
        ..gcd
    };
    match info.scalar_export(&wrong) {
        Err(AbiError::NoSuchFunction { available, .. }) => {
            assert!(available.contains(&"gcd(int32,int32)->int32".to_string()))
        }
        other => panic!("expected NoSuchFunction, got {other:?}"),
    }
}
