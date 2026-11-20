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
  onHide,
  onShow,
  onUnreadCountChange,
  show,
  showNewMessage,
  shutdown,
  update,
  whoami,
} from "@intercom/messenger-js-sdk";
import { useEffect, useState, useSyncExternalStore } from "react";

import { useIntercomJwt } from "~/api/auth";
const APP_ID = "r8661p0d";

/**
 * The JWT the Messenger was last booted with, or null after an anonymous
 * boot. The widget reports `booted` for an anonymous boot too, so this is
 * what says whether it knows who the user is.
 */
let identifiedJwt: string | null = null;
const identityListeners = new Set<() => void>();

function setIdentifiedJwt(jwt: string | null) {
  identifiedJwt = jwt;
  identityListeners.forEach((listener) => listener());
}

function subscribeToIdentity(listener: () => void) {
  identityListeners.add(listener);
  return () => {
    identityListeners.delete(listener);
  };
}

const getIdentifiedJwt = () => identifiedJwt;

/**
 * Who this page last booted the Messenger for: a user's JWT, null for an
 * anonymous visitor, or undefined before the first boot.
 */
let bootedFor: string | null | undefined;

/**
 * Boots the Messenger for the user with this JWT, or anonymously.
 *
 * Calling `Intercom()` on a running Messenger only updates it, so the session
 * (and its cookie) would carry over: the anonymous visitor's, or a previous
 * user's, conversations and unread count would follow the new identity. So
 * any other identity is shut down first, and the new one boots fresh.
 */
function bootMessenger(jwt: string | null) {
  if (bootedFor === jwt) return;
  const settings = jwt
    ? { app_id: APP_ID, intercom_user_jwt: jwt }
    : { app_id: APP_ID };
  // `window.Intercom` exists once anything has loaded the Messenger.
  if (bootedFor === undefined && !window.Intercom) {
    Intercom(settings);
  } else {
    shutdown();
    boot(settings);
    // A new session: watch its visibility from scratch.
    messengerState.isWatching = false;
    messengerState.isShown = false;
  }
  bootedFor = jwt;
  setIdentifiedJwt(jwt);
  watchMessengerVisibility();
}

export function useIntercomAnonymous() {
  useEffect(() => {
    // Before sign-in, or after sign-out if this page booted it for a user.
    bootMessenger(null);
  }, []);
}

export function useIntercom() {
  const { intercomJwt } = useIntercomJwt();
  const jwt = intercomJwt?.jwt;

  useEffect(() => {
    if (jwt) bootMessenger(jwt);
  }, [jwt]);

  // Signing out unmounts this, so the next person at the browser doesn't get
  // the user's Messenger session.
  useEffect(
    () => () => {
      if (bootedFor) bootMessenger(null);
    },
    [],
  );
}

export function useHideIntercomLauncher(hidden: boolean) {
  useEffect(() => {
    if (!hidden || !window.Intercom) return;
    update({ hide_default_launcher: true });
    return () => update({ hide_default_launcher: false });
  }, [hidden]);
}

/**
 * Whether the Intercom Messenger has booted. `window.Intercom` alone is not
 * enough: the SDK installs a queueing stub as soon as it initializes, so the
 * function exists even when the widget script is blocked or cannot boot
 * (e.g. third-party cookies disabled). The real widget sets `booted` once it
 * is running.
 */
export function isIntercomBooted() {
  if (typeof window === "undefined") return false;
  const intercom = window.Intercom as
    (((...args: unknown[]) => void) & { booted?: boolean }) | undefined;
  return typeof intercom === "function" && intercom.booted === true;
}

/** The user an Intercom JWT is for: its `user_id`, or else its `email`. */
function jwtUser(jwt: string) {
  try {
    const payload = jwt.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    const claims = JSON.parse(window.atob(payload)) as {
      user_id?: unknown;
      email?: unknown;
    };
    const user = claims.user_id ?? claims.email;
    return typeof user === "string" ? user.toLowerCase() : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Whether the running Messenger session is this JWT's user. `booted` can't
 * tell: it stays true through `shutdown()` and while the next boot is pending
 * or failing. `whoami` is empty from a shutdown until a boot finishes, and
 * has no user ID or email for an anonymous visitor.
 */
function isSessionFor(jwt: string) {
  if (!isIntercomBooted()) return false;
  const session = whoami() as
    { user_id?: string | null; email?: string | null } | undefined;
  const ids = [session?.user_id, session?.email]
    .filter((id): id is string => Boolean(id))
    .map((id) => id.toLowerCase());
  // A JWT that doesn't say who it's for can't be matched to the session, so
  // it never counts: the session could be a previous user's.
  const user = jwtUser(jwt);
  return user !== undefined && ids.includes(user);
}

/**
 * Whether the Messenger is running for the user with this JWT, rather than
 * anonymously, for someone else, or not yet. For click handlers, which should
 * check at click time.
 */
export function isIntercomIdentifiedWith(jwt: string | undefined) {
  return Boolean(jwt) && jwt === identifiedJwt && isSessionFor(jwt as string);
}

/** The signed-in user's Intercom JWT, if the console has one. */
export function useCurrentIntercomJwt() {
  const { intercomJwt } = useIntercomJwt();
  return intercomJwt?.jwt;
}

/**
 * Whether the Messenger is booted and identifies the signed-in user. The
 * console boots Intercom anonymously before sign-in, so the Messenger can be
 * running without the user's company, tickets, or outage eligibility.
 *
 * The widget boots asynchronously and exposes no event for it, so this
 * checks every second at first, then keeps checking every
 * `INTERCOM_BOOT_SLOW_CHECK_INTERVAL_MS` in case it boots late (slow network,
 * cookies allowed after a prompt). Callers acting on a click should also
 * check `isIntercomIdentifiedWith()` at that moment rather than rely on this
 * value.
 */
export function useIsIntercomIdentified() {
  const jwt = useCurrentIntercomJwt();
  const bootedJwt = useSyncExternalStore(subscribeToIdentity, getIdentifiedJwt);
  // The JWT whose session was seen running. Readiness is per identity, so a
  // new user's pending or failed boot doesn't count as ready.
  const [readyJwt, setReadyJwt] = useState<string | null>(() =>
    bootedJwt && isSessionFor(bootedJwt) ? bootedJwt : null,
  );
  const isReady =
    bootedJwt !== null && (readyJwt === bootedJwt || isSessionFor(bootedJwt));

  useEffect(() => {
    if (!bootedJwt || isReady) return;
    let attempts = 0;
    let timeout: number;
    const check = () => {
      attempts += 1;
      if (isSessionFor(bootedJwt)) {
        setReadyJwt(bootedJwt);
        return;
      }
      timeout = window.setTimeout(
        check,
        attempts < INTERCOM_BOOT_FAST_CHECKS
          ? INTERCOM_BOOT_FAST_CHECK_INTERVAL_MS
          : INTERCOM_BOOT_SLOW_CHECK_INTERVAL_MS,
      );
    };
    timeout = window.setTimeout(check, INTERCOM_BOOT_FAST_CHECK_INTERVAL_MS);
    return () => window.clearTimeout(timeout);
  }, [bootedJwt, isReady]);

  return Boolean(jwt) && bootedJwt === jwt && isReady;
}

export const INTERCOM_BOOT_FAST_CHECK_INTERVAL_MS = 1_000;
export const INTERCOM_BOOT_FAST_CHECKS = 30;
export const INTERCOM_BOOT_SLOW_CHECK_INTERVAL_MS = 10_000;

/**
 * How long to wait for the Messenger to appear after asking it to open. The
 * SDK can report itself booted while the Messenger's frame fails to load, in
 * which case it never shows.
 */
export const INTERCOM_OPEN_TIMEOUT_MS = 3_000;

const messengerState = { isWatching: false, isShown: false, shownAt: 0 };

/**
 * Tracks whether the Messenger is open. Registered when Intercom boots, so a
 * Messenger the user opened from the launcher counts as open: asking an open
 * Messenger to open again doesn't fire `onShow`.
 */
function watchMessengerVisibility() {
  if (messengerState.isWatching) return;
  messengerState.isWatching = true;
  onShow(() => {
    messengerState.isShown = true;
    messengerState.shownAt = Date.now();
  });
  onHide(() => {
    messengerState.isShown = false;
  });
}

/**
 * Asks the Messenger to open, and calls `onUnavailable` if it doesn't appear
 * within `INTERCOM_OPEN_TIMEOUT_MS`. Returns false when the Messenger hasn't
 * booted for a signed-in user, so callers can fall back right away.
 */
function openMessenger(open: () => void, onUnavailable?: () => void) {
  if (!identifiedJwt || !isSessionFor(identifiedJwt)) return false;
  watchMessengerVisibility();
  const requestedAt = Date.now();
  open();
  if (onUnavailable && !messengerState.isShown) {
    window.setTimeout(() => {
      if (messengerState.shownAt < requestedAt) onUnavailable();
    }, INTERCOM_OPEN_TIMEOUT_MS);
  }
  return true;
}

/**
 * Opens a new Messenger conversation. The Messenger's workflow offers the
 * ticket types the user's organization is eligible for, including "Report
 * Production Outage" for SLA customers. Returns false when the Messenger is
 * not available, so callers can fall back to email; `onUnavailable` covers a
 * Messenger that booted but never appears.
 */
export function openIntercomNewConversation(onUnavailable?: () => void) {
  return openMessenger(() => showNewMessage(""), onUnavailable);
}

/** Opens the Messenger home, where Matty answers questions. */
export function openIntercomMessenger(onUnavailable?: () => void) {
  return openMessenger(show, onUnavailable);
}

/** Resets the Messenger visibility and identity tracking. For tests only. */
export function resetMessengerStateForTests() {
  messengerState.isWatching = false;
  messengerState.isShown = false;
  messengerState.shownAt = 0;
  bootedFor = undefined;
  setIdentifiedJwt(null);
}

/**
 * Number of unread Messenger conversations and ticket replies for the
 * signed-in user. Stays 0 while the Messenger is unavailable.
 */
export function useIntercomUnreadCount() {
  const isIdentified = useIsIntercomIdentified();
  const jwt = useCurrentIntercomJwt();
  const [unreadCount, setUnreadCount] = useState(0);

  // Per identity: a new user's count starts at 0, from their own session.
  useEffect(() => {
    setUnreadCount(0);
    if (!isIdentified) return;
    let isMounted = true;
    // The SDK has no way to unregister the callback, so ignore updates after
    // unmount instead.
    onUnreadCountChange((count: number) => {
      if (isMounted) setUnreadCount(count);
    });
    return () => {
      isMounted = false;
    };
  }, [isIdentified, jwt]);

  return isIdentified ? unreadCount : 0;
}

export default useIntercom;
