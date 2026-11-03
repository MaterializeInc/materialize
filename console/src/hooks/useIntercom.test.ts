// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { onShow, showNewMessage } from "@intercom/messenger-js-sdk";
import { act, renderHook } from "@testing-library/react";

import {
  INTERCOM_BOOT_FAST_CHECK_INTERVAL_MS,
  INTERCOM_BOOT_FAST_CHECKS,
  INTERCOM_BOOT_SLOW_CHECK_INTERVAL_MS,
  INTERCOM_OPEN_TIMEOUT_MS,
  openIntercomNewConversation,
  resetMessengerVisibilityForTests,
  useIntercom,
  useIsIntercomIdentified,
} from "./useIntercom";

vi.mock("@intercom/messenger-js-sdk", () => ({
  Intercom: vi.fn(),
  update: vi.fn(),
  show: vi.fn(),
  showNewMessage: vi.fn(),
  onShow: vi.fn(),
  onHide: vi.fn(),
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

describe("openIntercomNewConversation", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    resetMessengerVisibilityForTests();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
    delete (window as unknown as { Intercom?: IntercomStub }).Intercom;
  });

  it("falls back when the Messenger never boots", () => {
    installStub();
    const onUnavailable = vi.fn();
    expect(openIntercomNewConversation(onUnavailable)).toBe(false);
    expect(showNewMessage).not.toHaveBeenCalled();
  });

  it("falls back when the booted Messenger never appears", () => {
    installStub().booted = true;
    const onUnavailable = vi.fn();
    expect(openIntercomNewConversation(onUnavailable)).toBe(true);
    expect(showNewMessage).toHaveBeenCalled();

    vi.advanceTimersByTime(INTERCOM_OPEN_TIMEOUT_MS);
    expect(onUnavailable).toHaveBeenCalledTimes(1);
  });

  it("doesn't fall back once the Messenger appears", () => {
    installStub().booted = true;
    const onUnavailable = vi.fn();
    openIntercomNewConversation(onUnavailable);
    const [shown] = vi.mocked(onShow).mock.calls[0] as [() => void];
    shown();

    vi.advanceTimersByTime(INTERCOM_OPEN_TIMEOUT_MS);
    expect(onUnavailable).not.toHaveBeenCalled();
  });

  it("doesn't fall back when the user already opened the Messenger", () => {
    installStub().booted = true;
    // Booting registers the visibility watchers, before any of our buttons
    // are clicked.
    renderHook(() => useIntercom());
    const [shown] = vi.mocked(onShow).mock.calls[0] as [() => void];
    // The user opens the Messenger from the launcher.
    shown();

    // Asking an open Messenger to open doesn't fire onShow again.
    const onUnavailable = vi.fn();
    openIntercomNewConversation(onUnavailable);
    vi.advanceTimersByTime(INTERCOM_OPEN_TIMEOUT_MS);
    expect(onUnavailable).not.toHaveBeenCalled();
  });
});
