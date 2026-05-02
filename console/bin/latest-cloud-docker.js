#!/usr/bin/env node

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { exec } from "node:child_process";
import { promisify } from "node:util";

const run = promisify(exec);

async function getCommitShas() {
  const { stdout } = await run(
    `git log --first-parent main --pretty=format:"%H"`,
  );
  return stdout.split("\n");
}

async function checkDockerTag(tag) {
  const encodedToken = Buffer.from(process.env.GITHUB_GHCR_TOKEN).toString(
    "base64",
  );
  const response = await fetch(
    `https://ghcr.io/v2/materializeinc/cloud/manifests/${tag}`,
    {
      headers: {
        authorization: `Bearer ${encodedToken}`,
      },
    },
  );
  return response.status === 200;
}

// Gets the most recent commit on main that has an associated docker image.
// Expects to be run from the cloud git repo.
async function getLatestDockerTag() {
  const shas = await getCommitShas();
  for (const sha of shas) {
    if (await checkDockerTag(sha)) {
      console.error(sha, "is the latest tag");
      console.log(sha);
      return sha;
    } else {
      console.error(sha, "not found");
    }
  }
  console.error("No published release found on Docker hub");
  process.exit(1);
}

getLatestDockerTag();
