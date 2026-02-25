#!/usr/bin/env node

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

const PASSWORD = process.env.E2E_TEST_PASSWORD;
if (!PASSWORD) process.exit(0);

const CLOUD_HOST = process.env.CLOUD_HOST || "cloud.materialize.com";
const STACK = CLOUD_HOST.includes("staging") ? "staging" : "production";
const stackSuffix = STACK === "production" ? "" : `.${STACK}`;
const FRONTEGG_URL = `https://admin${stackSuffix}.cloud.materialize.com`;
const REGIONS = [
  `https://api.us-east-1.aws${stackSuffix}.cloud.materialize.com`,
  `https://api.eu-west-1.aws${stackSuffix}.cloud.materialize.com`,
];
const NUM_WORKERS = 5;

async function disableRegionsForUser(email) {
  let accessToken;
  try {
    const res = await fetch(
      `${FRONTEGG_URL}/identity/resources/auth/v1/user`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ email, password: PASSWORD }),
        signal: AbortSignal.timeout(10_000),
      },
    );
    if (!res.ok) return;
    ({ accessToken } = await res.json());
  } catch {
    return;
  }

  await Promise.allSettled(
    REGIONS.map((regionUrl) =>
      fetch(`${regionUrl}/api/region?hardDelete=true`, {
        method: "DELETE",
        headers: {
          authorization: `Bearer ${accessToken}`,
          "content-type": "application/json",
        },
        signal: AbortSignal.timeout(65_000),
      }),
    ),
  );
}

const workers = [];
for (let i = 0; i < NUM_WORKERS; i++) {
  const email = `infra+cloud-integration-tests-${STACK}-console-${i}@materialize.io`;
  workers.push(disableRegionsForUser(email));
}
await Promise.allSettled(workers);
