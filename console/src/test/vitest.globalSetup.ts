// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const dirname = path.dirname(fileURLToPath(import.meta.url));
export const LOG_DIR = path.resolve(dirname, "../../log");
export const TEST_LOG_PATH = path.join(LOG_DIR, "./test.log");

export async function setup() {
  rmSync(LOG_DIR, { force: true, recursive: true });
  mkdirSync(LOG_DIR);
}
