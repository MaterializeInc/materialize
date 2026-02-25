#!/usr/bin/env node

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { argv, stdout } from "node:process";

// Vercel has a hard limit on subdomain length. If exceeded, the alias will fail to assign.
// https://vercel.com/guides/why-is-my-vercel-deployment-url-being-shortened
const MAX_SUBDOMAIN_LENGTH = 63;

function toKebabCase(input) {
  return input
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, "-") // Replace all non-alphanumeric characters with hyphens
    .replace(/\s+/g, "-") // Replace all spaces with hyphens
    .replace(/-+/g, "-") // Drop consecutive hyphens
    .replace(/^-+|-+$/g, ""); // Remove leading and trailing hyphens
}

function getPreviewUrl(identifier) {
  const sanitized = toKebabCase(identifier);
  let subdomain = `console-git-${sanitized}`;
  if (subdomain.length > MAX_SUBDOMAIN_LENGTH) {
    console.warn(
      `Subdomain component '${subdomain}' is too long. Truncating. This may result in reassignment of an existing alias.`,
    );
    subdomain = subdomain
      .substring(0, MAX_SUBDOMAIN_LENGTH)
      .replace(/-+$/g, "");
  }
  const url = `${subdomain}.preview.console.materialize.com`;
  return url;
}

const previewUrl = getPreviewUrl(argv[2]);
stdout.write(previewUrl + "\n");
