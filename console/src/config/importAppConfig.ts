// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { type SelfManagedAuthMode } from "./AppConfig";

const DEFAULT_APP_CONFIG = {
  auth: {
    mode: "None" as const,
  },
};

async function fetchAppConfigJson() {
  try {
    // app-config.json is how Orchestratord can propagate configuration changes to the client. Depending on the user's configuration, it'll rewrite
    // app-config.json. This is much simpler opposed to alternatives like creating an HTTP endpoint.
    return await (await fetch("/app-config/app-config.json")).json();
  } catch (error) {
    console.error("Failed to fetch app config", error);
    // If the fetch fails, fallback to the default app config that Cloud uses. This usually fails in Teleport impoersonation due to its reverse proxy not
    // properly routing the path to the app-config.json file.
    return DEFAULT_APP_CONFIG;
  }
}

const appConfigJson = await fetchAppConfigJson();

export function importAppConfig(): {
  auth: {
    mode: SelfManagedAuthMode;
  };
} {
  if (process.env.NODE_ENV === "test") {
    return DEFAULT_APP_CONFIG;
  }

  return appConfigJson as {
    auth: {
      mode: SelfManagedAuthMode;
    };
  };
}
