// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  boot,
  Intercom,
  onShow,
  showNewMessage,
  shutdown,
  whoami,
} from "@intercom/messenger-js-sdk";
import { act, renderHook } from "@testing-library/react";

import {
  INTERCOM_BOOT_FAST_CHECK_INTERVAL_MS,
  INTERCOM_BOOT_FAST_CHECKS,
  INTERCOM_BOOT_SLOW_CHECK_INTERVAL_MS,
  INTERCOM_OPEN_TIMEOUT_MS,
  isIntercomIdentifiedWith,
  openIntercomNewConversation,
  resetMessengerStateForTests,
  useIntercom,
  useIntercomAnonymous,
  useIsIntercomIdentified,
} from "./useIntercom";

vi.mock("@intercom/messenger-js-sdk", () => ({
  Intercom: vi.fn(),
  boot: vi.fn(),
  shutdown: vi.fn(),
  // The running session's user, while a stubbed Messenger is booted.
  whoami: vi.fn(() =>
    (window as unknown as { Intercom?: { booted?: boolean } }).Intercom?.booted
      ? { user_id: "user@example.com", email: "user@example.com" }
      : undefined,
  ),
  update: vi.fn(),
  show: vi.fn(),
  showNewMessage: vi.fn(),
  onShow: vi.fn(),
  onHide: vi.fn(),
  onUnreadCountChange: vi.fn(),
}));

/** The signed-in user's Intercom JWT, which tests can change. */
const auth = vi.hoisted(() => {
  /** An Intercom JWT for this user, as the global API issues them. */
  const jwtFor = (user: string) =>
    [
      "header",
      globalThis.btoa(JSON.stringify({ user_id: user, email: user })),
      "sig",
    ].join(".");
  // The user the `whoami` mock reports.
  const userJwt = jwtFor("user@example.com");
  return { jwtFor, userJwt, jwt: userJwt };
});

vi.mock("~/api/auth", () => ({
  useIntercomJwt: () => ({
    intercomJwt: { jwt: auth.jwt },
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

/** Boots the Messenger with the signed-in user's JWT, as the app does. */
const identify = () => renderHook(() => useIntercom());

describe("useIsIntercomIdentified", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    resetMessengerStateForTests();
  });

  afterEach(() => {
    vi.useRealTimers();
    delete (window as unknown as { Intercom?: IntercomStub }).Intercom;
  });

  it("notices a Messenger that boots after the fast checks stop", () => {
    const intercom = installStub();
    identify();
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
    identify();
    const { result } = renderHook(() => useIsIntercomIdentified());
    expect(result.current).toBe(true);
  });

  it("isn't fooled by an anonymously booted Messenger", () => {
    // e.g. the sign-in page booted it, or impersonation, which never boots
    // it with a JWT.
    installStub().booted = true;
    const { result } = renderHook(() => useIsIntercomIdentified());
    expect(result.current).toBe(false);

    act(() => {
      identify();
    });
    expect(result.current).toBe(true);
  });
});

const { jwtFor } = auth;

describe("readiness across identities", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    resetMessengerStateForTests();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
    // Back to reporting the stub's session, for the tests that follow.
    vi.mocked(whoami).mockReset();
    auth.jwt = auth.userJwt;
    delete (window as unknown as { Intercom?: IntercomStub }).Intercom;
  });

  it("waits for the next user's session after a switch", () => {
    installStub().booted = true;
    auth.jwt = jwtFor("first@example.com");
    vi.mocked(whoami).mockReturnValue({ user_id: "first@example.com" });
    const { rerender } = identify();
    const { result, rerender: rerenderReadiness } = renderHook(() =>
      useIsIntercomIdentified(),
    );
    expect(result.current).toBe(true);

    // Another user signs in. `booted` stays true through the shutdown, and
    // their boot is still pending.
    vi.mocked(whoami).mockReturnValue(undefined);
    auth.jwt = jwtFor("second@example.com");
    act(() => {
      rerender();
    });
    rerenderReadiness();
    expect(result.current).toBe(false);
    expect(isIntercomIdentifiedWith(auth.jwt)).toBe(false);
    act(() => {
      vi.advanceTimersByTime(INTERCOM_BOOT_FAST_CHECK_INTERVAL_MS * 3);
    });
    expect(result.current).toBe(false);

    // Their session starts.
    vi.mocked(whoami).mockReturnValue({ user_id: "second@example.com" });
    act(() => {
      vi.advanceTimersByTime(INTERCOM_BOOT_FAST_CHECK_INTERVAL_MS);
    });
    expect(result.current).toBe(true);
  });

  it.each([
    ["an unreadable JWT", "not-a-jwt"],
    [
      "a JWT without a user",
      [
        "header",
        window.btoa(JSON.stringify({ organization_id: "org" })),
        "sig",
      ].join("."),
    ],
  ])("doesn't count any session as ready for %s", (_, jwt) => {
    // A previous user's session may still be running.
    installStub().booted = true;
    vi.mocked(whoami).mockReturnValue({ user_id: "first@example.com" });
    auth.jwt = jwt;
    identify();
    const { result } = renderHook(() => useIsIntercomIdentified());
    expect(result.current).toBe(false);
    expect(isIntercomIdentifiedWith(jwt)).toBe(false);
    expect(openIntercomNewConversation()).toBe(false);
  });

  it("doesn't count another user's session as ready", () => {
    // e.g. the shutdown hasn't taken effect yet.
    installStub().booted = true;
    auth.jwt = jwtFor("second@example.com");
    vi.mocked(whoami).mockReturnValue({ user_id: "first@example.com" });
    identify();
    const { result } = renderHook(() => useIsIntercomIdentified());
    expect(result.current).toBe(false);
    expect(openIntercomNewConversation()).toBe(false);
  });
});

describe("Messenger identity", () => {
  beforeEach(() => {
    resetMessengerStateForTests();
    auth.jwt = auth.userJwt;
  });

  afterEach(() => {
    vi.clearAllMocks();
    delete (window as unknown as { Intercom?: IntercomStub }).Intercom;
  });

  /** Simulates the SDK, which installs `window.Intercom` on first boot. */
  const loadOnFirstBoot = () =>
    vi.mocked(Intercom).mockImplementation(() => {
      installStub();
    });

  it("boots fresh for the user instead of updating the anonymous session", () => {
    loadOnFirstBoot();
    renderHook(() => useIntercomAnonymous());
    expect(Intercom).toHaveBeenCalledWith({ app_id: expect.any(String) });
    expect(shutdown).not.toHaveBeenCalled();

    identify();

    expect(shutdown).toHaveBeenCalledTimes(1);
    expect(boot).toHaveBeenCalledWith({
      app_id: expect.any(String),
      intercom_user_jwt: auth.userJwt,
    });
    // The anonymous session ends before the user's starts.
    expect(vi.mocked(shutdown).mock.invocationCallOrder[0]).toBeLessThan(
      vi.mocked(boot).mock.invocationCallOrder[0],
    );
    // Only the first boot loads the Messenger; later ones don't update it.
    expect(Intercom).toHaveBeenCalledTimes(1);
  });

  it("starts a new session when another user signs in", () => {
    loadOnFirstBoot();
    const { rerender } = identify();
    expect(shutdown).not.toHaveBeenCalled();

    auth.jwt = "other-jwt";
    rerender();

    expect(shutdown).toHaveBeenCalledTimes(1);
    expect(boot).toHaveBeenLastCalledWith({
      app_id: expect.any(String),
      intercom_user_jwt: "other-jwt",
    });
  });

  it("doesn't restart the session for the same user", () => {
    loadOnFirstBoot();
    const { rerender } = identify();
    rerender();
    expect(shutdown).not.toHaveBeenCalled();
    expect(boot).not.toHaveBeenCalled();
  });

  it("ends the user's session on sign-out", () => {
    loadOnFirstBoot();
    const { unmount } = identify();

    unmount();

    expect(shutdown).toHaveBeenCalledTimes(1);
    expect(boot).toHaveBeenLastCalledWith({ app_id: expect.any(String) });
    // Nothing can be opened for the signed-out user.
    installStub().booted = true;
    expect(openIntercomNewConversation()).toBe(false);
  });
});

describe("openIntercomNewConversation", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    resetMessengerStateForTests();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
    delete (window as unknown as { Intercom?: IntercomStub }).Intercom;
  });

  it("falls back when the Messenger never boots", () => {
    installStub();
    identify();
    expect(openIntercomNewConversation(vi.fn())).toBe(false);
    expect(showNewMessage).not.toHaveBeenCalled();
  });

  it("doesn't open an anonymously booted Messenger", () => {
    installStub().booted = true;
    expect(openIntercomNewConversation(vi.fn())).toBe(false);
    expect(showNewMessage).not.toHaveBeenCalled();
  });

  it("falls back when the booted Messenger never appears", () => {
    installStub().booted = true;
    identify();
    const onUnavailable = vi.fn();
    expect(openIntercomNewConversation(onUnavailable)).toBe(true);
    expect(showNewMessage).toHaveBeenCalled();

    vi.advanceTimersByTime(INTERCOM_OPEN_TIMEOUT_MS);
    expect(onUnavailable).toHaveBeenCalledTimes(1);
  });

  it("doesn't fall back once the Messenger appears", () => {
    installStub().booted = true;
    identify();
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
