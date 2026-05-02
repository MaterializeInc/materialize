// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * Entry point for Console.
 */

// We host our own copy of Inter because the fontsource version is very out of date
import "~/font/Inter.var.woff2";
import "~/font/inter.css";
import "@fontsource/roboto-mono";
// Initializes Sentry error reporting and tracing
import "~/sentry";
import "jotai-devtools/styles.css";

import React from "react";
import { createRoot } from "react-dom/client";

import { App } from "~/platform/App";

import { addChunkLoadErrorListener } from "./utils/chunkLoadErrorHandler";

const rootEl = document.createElement("div");
document.body.appendChild(rootEl);
const root = createRoot(rootEl);
root.render(<App />);

addChunkLoadErrorListener();
