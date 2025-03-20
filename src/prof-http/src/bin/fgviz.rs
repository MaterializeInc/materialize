// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use askama::Template;
use mz_build_info::build_info;
use mz_prof_http::FlamegraphTemplate;

fn main() {
    let bi = build_info!();
    let mzfg = std::env::args()
        .nth(1)
        .map(|path| {
            let bytes = std::fs::read(path).expect("Failed to read supplied file");
            String::from_utf8(bytes).expect("Supplied file was not utf-8")
        })
        .unwrap_or_else(|| "".into());
    let rendered = FlamegraphTemplate {
        version: &bi.human_version(None),
        title: "Flamegraph Visualizer",
        mzfg: &mzfg,
    }
    .render()
    .expect("template rendering cannot fail");
    print!("{}", rendered);
}
