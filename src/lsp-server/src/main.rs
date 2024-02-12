// Copyright (c) 2023 Eyal Kalderon
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the tower-lsp project. The original source
// code was retrieved on 10/02/2023 from:
//
//     https://github.com/ebkalderon/tower-lsp/blob/cc4c858/examples/stdio.rs
//
// The original source code is subject to the terms of the <APACHE|MIT> license, a copy
// of which can be found in the LICENSE file at the root of this repository.

use mz_lsp_server::backend::{Backend, Completions, DEFAULT_FORMATTING_WIDTH};
use mz_ore::collections::HashMap;
use tokio::sync::Mutex;
use tower_lsp::{LspService, Server};

#[tokio::main]
async fn main() {
    let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());

    let (service, socket) = LspService::new(|client| Backend {
        client,
        parse_results: Mutex::new(HashMap::new()),
        formatting_width: DEFAULT_FORMATTING_WIDTH.into(),
        schema: Mutex::new(None),
        content: Mutex::new(HashMap::new()),
        completions: Mutex::new(Completions {
            select: Vec::new(),
            from: Vec::new(),
        }),
    });
    Server::new(stdin, stdout, socket).serve(service).await;
}
