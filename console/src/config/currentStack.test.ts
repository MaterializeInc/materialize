// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getCurrentStack } from "~/config/currentStack";

describe("getCurrentStack", () => {
  beforeEach(() => {
    window.localStorage.removeItem("mz-current-stack");
  });

  it("should return the default if no local storage value is set", () => {
    const stack = getCurrentStack({ hostname: "localhost", isBrowser: true });
    expect(stack).toEqual("test");
  });

  it("should return the local storage value if it's set", () => {
    window.localStorage.setItem("mz-current-stack", "local");
    const stack = getCurrentStack({ hostname: "localhost", isBrowser: true });
    expect(stack).toEqual("local");
  });

  it("should return loadtest if the there is no local storage value and the url is loadtest", () => {
    const stack = getCurrentStack({
      hostname: "loadtest.console.materialize.com",
      isBrowser: true,
    });
    expect(stack).toEqual("loadtest");
  });

  it("should return staging if the there is no local storage value and the url is staging", () => {
    const stack = getCurrentStack({
      hostname: "staging.console.materialize.com",
      isBrowser: true,
    });
    expect(stack).toEqual("staging");
  });

  it("should return staging if the there is no local storage value and it's a preview url", () => {
    const stack = getCurrentStack({
      hostname: "branch-name.preview.console.materialize.com",
      isBrowser: true,
    });
    expect(stack).toEqual("staging");
  });

  it("should return the local storage value if it's set", () => {
    window.localStorage.setItem("mz-current-stack", "local");
    const stack = getCurrentStack({ hostname: "localhost", isBrowser: true });
    expect(stack).toEqual("local");
  });

  describe("in a non-browser environment", () => {
    afterEach(() => {
      vi.unstubAllEnvs();
    });

    it("returns local for localhost and localhost aliases, ignoring defaultStack", () => {
      expect(
        getCurrentStack({
          hostname: "localhost",
          isBrowser: false,
          defaultStack: "ignored",
        }),
      ).toEqual("local");
      expect(
        getCurrentStack({
          hostname: "local.dev.materialize.com",
          isBrowser: false,
          defaultStack: "ignored",
        }),
      ).toEqual("local");
    });

    it("returns production for non-local domains $CLOUD_HOST is the prod domain", () => {
      vi.stubEnv("CLOUD_HOST", "cloud.materialize.com");

      const stack = getCurrentStack({
        hostname: "foo.example.com",
        isBrowser: false,
        defaultStack: "ignored",
      });
      expect(stack).toEqual("production");
    });

    it("returns staging for non-local domains $CLOUD_HOST is not the prod domain", () => {
      vi.stubEnv("CLOUD_HOST", "test.example.com");

      const stack = getCurrentStack({
        hostname: "foo.example.com",
        isBrowser: false,
        defaultStack: "ignored",
      });
      expect(stack).toEqual("staging");
    });
  });
});
