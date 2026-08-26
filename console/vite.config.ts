// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { readFileSync } from "node:fs";

import { sentryVitePlugin } from "@sentry/vite-plugin";
import react from "@vitejs/plugin-react";
import browserslistToEsbuild from "browserslist-to-esbuild";
import { defineConfig, type ProxyOptions } from "vite";
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

const devServerTls =
  process.env.DEV_SERVER_TLS_CERT && process.env.DEV_SERVER_TLS_KEY
    ? {
        cert: readFileSync(process.env.DEV_SERVER_TLS_CERT),
        key: readFileSync(process.env.DEV_SERVER_TLS_KEY),
      }
    : undefined;

/**
 * Proxies Ory's self-service API through the dev server when ORY_KRATOS_URL is
 * set, keeping the SDK same-origin so Kratos needs no CORS entry per laptop.
 * Point VITE_ORY_SDK_URL at the dev server itself.
 *
 * Kratos scopes its CSRF and session cookies to its own registrable domain and
 * marks them Secure, and an OIDC round trip returns through Kratos's own host.
 * DEV_SERVER_HOST must therefore name a host under that same domain, served
 * over TLS, or the browser drops those cookies partway through the flow.
 */
const oryTarget: ProxyOptions = {
  target: process.env.ORY_KRATOS_URL,
  changeOrigin: true,
};

const oryProxy: Record<string, ProxyOptions> = process.env.ORY_KRATOS_URL
  ? { "/self-service/": oryTarget, "/sessions/": oryTarget }
  : {};

export default defineConfig({
  build: {
    // Converts browserslist format to explicit esbuild browser ranges
    target: browserslistToEsbuild(),
    sourcemap: seperateSourceMaps,
  },
  define: buildDefinitions(),
  server: {
    host: process.env.DEV_SERVER_HOST ?? "local.dev.materialize.com",
    https: devServerTls,
    port: 3000,
    /**
     * Proxy any requests from :3000 to environmentd/balancerd ports to avoid CORs issues.
     * We assume that any local instance of Materialize is exposed on localhost:6876.
     */
    proxy:
      process.env.DEV_SERVER_WITH_TLS_PROXY === "true"
        ? {
            ...oryProxy,
            "/api/": {
              target: `https://127.0.0.1:${devServerProxyPort}`,
              secure: false,
            },
            "/api/experimental/": {
              target: `wss://127.0.0.1:${devServerProxyPort}`,
              secure: false,
              ws: true,
            },
            // OAuth discovery for MCP clients pointed at /api/mcp.
            "/.well-known/oauth-protected-resource": {
              target: `https://127.0.0.1:${devServerProxyPort}`,
              secure: false,
            },
          }
        : {
            ...oryProxy,
            "/api/": {
              target: `http://127.0.0.1:${devServerProxyPort}`,
            },
            "/api/experimental/": {
              target: `ws://127.0.0.1:${devServerProxyPort}`,
              ws: true,
            },
            // OAuth discovery for MCP clients pointed at /api/mcp.
            "/.well-known/oauth-protected-resource": {
              target: `http://127.0.0.1:${devServerProxyPort}`,
            },
          },
  },
  plugins,
  base: process.env.BASENAME ? `${process.env.BASENAME}/` : "/",
});
