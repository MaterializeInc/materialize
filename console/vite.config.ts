// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sentryVitePlugin } from "@sentry/vite-plugin";
import react from "@vitejs/plugin-react";
import browserslistToEsbuild from "browserslist-to-esbuild";
import { defineConfig } from "vite";
import { analyzer } from "vite-bundle-analyzer";
import { createHtmlPlugin } from "vite-plugin-html";
import svgr from "vite-plugin-svgr";
import wasm from "vite-plugin-wasm";
import tsconfigPaths from "vite-tsconfig-paths";

import { svgrTemplate } from "./svgrTemplate";

const isProd = process.env.NODE_ENV === "production";
const consoleDeploymentMode = process.env.CONSOLE_DEPLOYMENT_MODE ?? "mz-cloud";
const seperateSourceMaps = Boolean(process.env.SOURCE_MAPS);

function requireEnv(name: string | string[]) {
  if (typeof name === "string") {
    const value = process.env[name];
    if (!value) {
      throw new Error(`${name} environment variable must be defined`);
    }
    return value;
  } else {
    const values = name.map((n) => process.env[n]);
    const defined = values.filter((v) => v) as string[];
    if (defined.length === 0) {
      throw new Error(`One of ${name} environment variables must be defined`);
    }
    return defined[0];
  }
}

function getSentryRelease() {
  if (isProd && consoleDeploymentMode !== "flexible-deployment")
    return requireEnv(["SENTRY_RELEASE", "VERCEL_GIT_COMMIT_SHA"]);

  return process.env.SENTRY_RELEASE;
}

function buildDefinitions() {
  if (isProd) {
    return {
      __BASENAME__: JSON.stringify(process.env.BASENAME || ""),
      __CONSOLE_DEPLOYMENT_MODE__: JSON.stringify(consoleDeploymentMode),
      __DEFAULT_STACK__: JSON.stringify(
        process.env.DEFAULT_STACK || "production",
      ),
      __FORCE_OVERRIDE_STACK__: JSON.stringify(
        process.env.FORCE_OVERRIDE_STACK,
      ),
      __IMPERSONATION_HOSTNAME__: JSON.stringify(
        process.env.IMPERSONATION_HOSTNAME,
      ),
      __MZ_CONSOLE_IMAGE_TAG__: JSON.stringify(
        process.env.MZ_CONSOLE_IMAGE_TAG,
      ),
      __SENTRY_ENABLED__: JSON.stringify(process.env.SENTRY_ENABLED),
      __SENTRY_RELEASE__: JSON.stringify(getSentryRelease()),
    };
  }
  return {
    __BASENAME__: JSON.stringify(process.env.BASENAME || ""),
    __CONSOLE_DEPLOYMENT_MODE__: JSON.stringify(consoleDeploymentMode),
    __DEFAULT_STACK__: JSON.stringify(process.env.DEFAULT_STACK || "staging"),
    __FORCE_OVERRIDE_STACK__: JSON.stringify(process.env.FORCE_OVERRIDE_STACK),
    __IMPERSONATION_HOSTNAME__: JSON.stringify(
      process.env.IMPERSONATION_HOSTNAME,
    ),
    __MZ_CONSOLE_IMAGE_TAG__: JSON.stringify(process.env.MZ_CONSOLE_IMAGE_TAG),
    __SENTRY_ENABLED__: JSON.stringify(process.env.SENTRY_ENABLED || false),
    __SENTRY_RELEASE__: JSON.stringify(process.env.SENTRY_RELEASE || null),
  };
}

const plugins = [
  wasm(),
  createHtmlPlugin({
    minify: true,
    // these paths are relative to the project root
    entry: "/src/index.tsx",
  }),
  svgr({
    svgrOptions: {
      typescript: true,
      template: svgrTemplate,
    },
    esbuildOptions: {
      loader: "tsx",
    },
    // A minimatch pattern, or array of patterns, which specifies the files in the build the plugin should include.
    include: "**/*.svg?react",
  }),
  tsconfigPaths(),
  react({
    babel: {
      plugins: ["@emotion"],
      presets: ["jotai/babel/preset"],
    },
  }),
];

if (seperateSourceMaps) {
  plugins.push(
    sentryVitePlugin({
      org: "materializeinc",
      project: "console",
      release: {
        name: getSentryRelease(),
      },
      authToken: process.env.SENTRY_AUTH_TOKEN,
    }),
  );
}

if (process.env.BUNDLE_ANALYZE) {
  plugins.push(analyzer());
}

if (isProd) {
  plugins.push();
}

const devServerProxyPort = process.env.DEV_SERVER_PROXY_PORT ?? 6876;

export default defineConfig({
  build: {
    // Converts browserslist format to explicit esbuild browser ranges
    target: browserslistToEsbuild(),
    sourcemap: seperateSourceMaps,
  },
  define: buildDefinitions(),
  server: {
    host: "local.dev.materialize.com",
    port: 3000,
    /**
     * Proxy any requests from :3000 to environmentd/balancerd ports to avoid CORs issues.
     * We assume that any local instance of Materialize is exposed on localhost:6876.
     */
    proxy:
      process.env.DEV_SERVER_WITH_TLS_PROXY === "true"
        ? {
            "/api/": {
              target: `https://127.0.0.1:${devServerProxyPort}`,
              secure: false,
            },
            "/api/experimental/": {
              target: `wss://127.0.0.1:${devServerProxyPort}`,
              secure: false,
              ws: true,
            },
          }
        : {
            "/api/": {
              target: `http://127.0.0.1:${devServerProxyPort}`,
            },
            "/api/experimental/": {
              target: `ws://127.0.0.1:${devServerProxyPort}`,
              ws: true,
            },
          },
  },
  plugins,
  base: process.env.BASENAME ? `${process.env.BASENAME}/` : "/",
});
