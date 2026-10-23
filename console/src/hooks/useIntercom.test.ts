// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, renderHook } from "@testing-library/react";

import {
  INTERCOM_BOOT_FAST_CHECK_INTERVAL_MS,
  INTERCOM_BOOT_FAST_CHECKS,
  INTERCOM_BOOT_SLOW_CHECK_INTERVAL_MS,
  useIsIntercomIdentified,
} from "./useIntercom";

vi.mock("@intercom/messenger-js-sdk", () => ({
  Intercom: vi.fn(),
  update: vi.fn(),
  show: vi.fn(),
  showNewMessage: vi.fn(),
  onUnreadCountChange: vi.fn(),
}));

vi.mock("~/api/auth", () => ({
  useIntercomJwt: () => ({
    intercomJwt: { jwt: "test-jwt" },
    loading: false,
    error: null,
  }),
}));

type IntercomStub = ((...args: unknown[]) => void) & { booted?: boolean };

const installStub = () => {
  const intercom: IntercomStub = vi.fn<(...args: unknown[]) => void>();
  (window as unknown as { Intercom?: IntercomStub }).Intercom = intercom;
  return intercom;
};

describe("useIsIntercomIdentified", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    delete (window as unknown as { Intercom?: IntercomStub }).Intercom;
  });

  it("notices a Messenger that boots after the fast checks stop", () => {
    const intercom = installStub();
    const { result } = renderHook(() => useIsIntercomIdentified());
    expect(result.current).toBe(false);

    // Past the fast checks, still not booted.
    act(() => {
      vi.advanceTimersByTime(
        INTERCOM_BOOT_FAST_CHECKS * INTERCOM_BOOT_FAST_CHECK_INTERVAL_MS +
          5_000,
      );
    });
    expect(result.current).toBe(false);

    intercom.booted = true;
    act(() => {
      vi.advanceTimersByTime(INTERCOM_BOOT_SLOW_CHECK_INTERVAL_MS);
    });
    expect(result.current).toBe(true);
  });

  it("is true right away when the Messenger has already booted", () => {
    installStub().booted = true;
    const { result } = renderHook(() => useIsIntercomIdentified());
    expect(result.current).toBe(true);
  });
});
