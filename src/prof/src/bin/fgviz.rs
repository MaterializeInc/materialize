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
use mz_prof::http::FlamegraphTemplate;

fn main() {
    let bi = build_info!();
    let rendered = FlamegraphTemplate {
        version: &bi.human_version(),
        title: "Flamegraph Visualizer",
        mzfg: "",
    }
    .render()
    .expect("template rendering cannot fail");
    print!("{}", rendered);
}
