// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { v4 as uuidv4 } from "uuid";

export type HistoryId = string;

/**
 * Returns a v4 uuid. This function exists so we can easily mock historyIds in test.
 */
export function createHistoryId() {
  return uuidv4();
}
