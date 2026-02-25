#!/usr/bin/env node

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Add CSP headers to Vercel.json
 *
 * We need to preprocess our Vercel configuration to include CSP headers for
 * known production environments. This enables us to define the Sentry
 * environment in the `report-uri`. Since `vercel build` first loads the
 * config, this script must be executed before the build process, outside of
 * the `vercel build` command (i.e., running it inside yarn's `build` alias
 * will not work).
 */

/*

Sample input:

```json
{
  "headers": [
    {
      "source": "/(.*)",
      "headers": [
        // Headers to apply to all non-asset files
        {
          "key": "Cache-Control",
          "value": "public, max-age=0, s-maxage=3600, must-revalidate"
        },
      ]
    },
    {
      "source": "/(.*).(.*).:ext(js|css|png|jpg|jpeg|gif|svg|woff|woff2)",
      "headers": [
        // Headers to apply to all asset files
        {
          "key": "Cache-Control",
          "value": "public, max-age=60, s-maxage=3600, immutable"
        },
      ]
    }
  ]
}
```

Sample output (note the addition of a 'Content-Security-Policy' header in the
`"/(.*)"` base/fall-through rule, and a dupe of that with a `has` matcher for
prod):

```json
{
  "headers": [
    {
      "source": "/(.*)",
      "headers": [
        // Headers to apply to all non-asset files
        {
          "key": "Cache-Control",
          "value": "public, max-age=0, s-maxage=3600, must-revalidate"
        },
        {
          "key": "Content-Security-Policy",
          "value": "base-uri 'self'; ..." // Fall-through policy trimmed for brevity
        }
      ]
    },
    {
      "source": "/(.*)",
      "headers": [
        // Headers to apply to all non-asset files
        {
          "key": "Cache-Control",
          "value": "public, max-age=0, s-maxage=3600, must-revalidate"
        },
        {
          "key": "Content-Security-Policy",
          "value": "base-uri 'self'; ..." // Prod policy trimmed for brevity
        }
      ]
      "has": [
        {
          "type": "host",
          "value": "console.materialize.com"
        }
      ]
    },
    {
      "source": "/(.*).(.*).:ext(js|css|png|jpg|jpeg|gif|svg|woff|woff2)",
      "headers": [
        // Headers to apply to all asset files
        {
          "key": "Cache-Control",
          "value": "public, max-age=60, s-maxage=3600, immutable"
        },
      ]
    }
  ]
}
```
*/

import { writeFileSync } from "node:fs";
import { parseArgs } from "node:util";

import { buildCsp } from "../contentSecurityPolicy.js";
import vercelJson from "../vercel.json" with { type: "json" };

const HEADER_GROUP_SOURCE_PATTERN = "/(.*)";

const CSP_ENV_MAPPINGS = {
  production: ["console.materialize.com"],
  noReport: ["loadtest.console.materialize.com"],
};

function getArgs() {
  const { values } = parseArgs({
    options: {
      "sentry-release": { type: "string" },
    },
  });
  return {
    sentryRelease: values["sentry-release"],
  };
}

function stringify(obj) {
  return JSON.stringify(obj, null, 4);
}

/**
 * @param {string} sentryRelease
 */
function writeCsp(sentryRelease) {
  const outJson = structuredClone(vercelJson);
  outJson.headers = outJson.headers.filter(
    (headerGroup) =>
      headerGroup.source !== HEADER_GROUP_SOURCE_PATTERN ||
      headerGroup.has === undefined,
  );
  // Find the offset of the existing hardcoded headers for all non-asset content
  const headerGroupIdx = outJson.headers.findIndex(
    (group) => group.source === HEADER_GROUP_SOURCE_PATTERN,
  );
  if (headerGroupIdx < 0) throw new Error("No header group found");
  // Get the hardcoded header object
  const headerGroup = outJson.headers[headerGroupIdx];
  // If a CSP is already configured (e.g., when re-running this script in dev),
  // drop the CSP header.
  headerGroup.headers = headerGroup.headers.filter(
    (header) => header.key !== "Content-Security-Policy",
  );
  // Create a copy of this for use in additional blocks
  const headerGroupBase = structuredClone(headerGroup);
  // Modify the existing fall-through header group to include a staging CSP
  // header.
  headerGroup.headers.push({
    key: "Content-Security-Policy",
    // For envs that we can't explicitly bind a hostname for (e.g., previews,
    // impersonation), specify a fallback sentry configuration.
    value: buildCsp(sentryRelease, "staging"),
  });
  for (const [env, domains] of Object.entries(CSP_ENV_MAPPINGS)) {
    // We don't want reporting for some envs
    const sentryEnvironment = env === "noReport" ? undefined : env;
    for (const domain of domains) {
      const group = structuredClone(headerGroupBase);
      group.has = [{ type: "host", value: domain }];
      group.headers.push({
        key: "Content-Security-Policy",
        value: buildCsp(sentryRelease, sentryEnvironment),
      });
      // Insert the CSP group *after* the fall-through CSP group since Vercel
      // evaluates configured matchers from the bottom up.
      outJson.headers.splice(headerGroupIdx + 1, 0, group);
    }
  }
  writeFileSync("vercel.json", stringify(outJson));
}

const { sentryRelease } = getArgs();
writeCsp(sentryRelease);
