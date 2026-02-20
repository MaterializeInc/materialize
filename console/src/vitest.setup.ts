// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import "@testing-library/jest-dom/vitest";
// @ts-ignore
import "intersection-observer";

import { ResizeObserver } from "@juggle/resize-observer";
import * as Sentry from "@sentry/react";
import debug from "debug";
import { MotionGlobalConfig } from "framer-motion";
import React from "react";

import { getStore, resetStore } from "./jotai";
import { resetQueryClient } from "./queryClient";
import {
  defaultRegionId,
  healthyEnvironment,
  setFakeEnvironment,
} from "./test/utils";

const debugHandlers = debug("console:msw");
// for some reason logs seem to get swallowed by vitest without this
debugHandlers.log = console.log.bind(console);

// Mocks

// Mock the facade for the frontegg library
vi.mock("~/external-library-wrappers/frontegg");
vi.mock("~/hooks/useFlags", () => {
  return { useFlags: () => ({}) };
});
vi.mock("~/analytics/segment", () => {
  // No official mock of segment. Methods copied from https://segment.com/docs/connections/spec/
  return {
    segment: {
      identify: vi.fn(),
      page: vi.fn(),
      track: vi.fn(),
      screen: vi.fn(),
      group: vi.fn(),
      alias: vi.fn(),
      load: vi.fn(),
      reset: vi.fn(),
      user: async () => ({
        // partial implementation based on current usage
        id: () => "fd983f6d-4a56-4d21-90dc-d631e15e891c",
        anonymousId: () => "6bfb2eec-761c-4d1a-935d-e2d445e4f79c",
      }),
    },
    useSegment: () => ({
      track: vi.fn(),
    }),
    useSegmentPageTracking: vi.fn(),
  };
});

// CodeMirror breaks under vitest. Hopefully we can fix this at some point, but for now
// we just mock out components that use CodeMirror
// https://github.com/uiwjs/react-codemirror/issues/216
vi.mock("~/components/CommandBlock/ReadOnlyCommandBlock", () => {
  return {
    default: function (props: { value: string }) {
      return React.createElement("div", { className: "cm-line" }, props.value);
    },
  };
});

/**
 * Set a default environment globally. This is not ideal, but we have lots of tests that
 * create query keys in the module scope for convienience, and they break without this.
 */
await setFakeEnvironment(getStore().set, defaultRegionId, healthyEnvironment);

/**
 * We need to import this after we set the environment, because mock server
 * creates MSW handlers that expect a region to be set.
 */
const { default: server } = await import("./api/mocks/server");

beforeAll(async () => {
  // Establish API mocking before all tests.
  if (debugHandlers.enabled) {
    server.events.on("request:start", ({ request }) => {
      debugHandlers(
        `[request] ${request.method}, ${request.url}`,
        request.body,
      );
    });
  }

  Sentry.init({});

  server.listen({ onUnhandledRequest: "error" });
});

beforeEach(async () => {
  MotionGlobalConfig.skipAnimations = true;
  await setFakeEnvironment(getStore().set, defaultRegionId, healthyEnvironment);
  resetQueryClient();
});

afterEach(() => {
  // Reset any request handlers that we may add during the tests,
  // so they don't affect other tests.
  server.resetHandlers();
  // Reset the jotai store for isolation.
  resetStore();
});

// Clean up after the tests are finished.
afterAll(() => {
  server.events.removeAllListeners();

  server.close();
});

// Jsdom doesn't provide a ResizeObserver. Provide a polyfill.
global.ResizeObserver = ResizeObserver;

export {};
