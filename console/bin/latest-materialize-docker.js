#!/usr/bin/env node

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { rsort } from "semver";

async function getTags() {
  const response = await fetch(
    "https://api.github.com/repos/MaterializeInc/materialize/tags?per_page=10&page=1&sort=created",
    {
      headers: {
        authorization: `token ${process.env.GITHUB_TOKEN}`,
      },
    },
  );
  if (response.status !== 200) {
    throw new Error(
      `Failed to fetch materialize tags from github: ${response.status} ${await response.text()}`,
    );
  }
  const tags = await response.json();
  return tags.map((tag) => tag.name);
}

async function checkDockerHubTag(tag) {
  const response = await fetch(
    `https://hub.docker.com/v2/namespaces/materialize/repositories/materialized/tags/${tag}`,
    { method: "head" },
  );
  return response.status === 200;
}

async function getLatestDockerTag() {
  const tags = await getTags();
  for (const tag of rsort(tags)) {
    if (await checkDockerHubTag(tag)) {
      console.error(tag, "is the latest tag");
      console.log(tag);
      return tag;
    } else {
      console.error(tag, "not found");
    }
  }
  console.error("No published release found on Docker hub");
  process.exit(1);
}

getLatestDockerTag();
