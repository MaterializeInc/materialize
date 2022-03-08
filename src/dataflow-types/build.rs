// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

fn main() {
    let mut cfg = prost_build::Config::new();
    cfg.type_attribute(".", "#[derive(Eq, serde::Serialize, serde::Deserialize)]");
    cfg.compile_protos(&["postgres_source.proto"], &["src"])
        .unwrap();
}
