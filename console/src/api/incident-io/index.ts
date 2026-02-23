// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { StatusSummary } from "./types";

export const STATUSPAGE_URL =
  "https://statuspage.incident.io/materialize/api/v1/summary";

export async function fetchSummary(requestOptions?: RequestInit) {
  const response = await fetch(STATUSPAGE_URL, requestOptions);

  if (!response.ok) {
    throw new Error(await response.json());
  }

  return response.json() as Promise<StatusSummary>;
}
