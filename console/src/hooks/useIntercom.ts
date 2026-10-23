// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Intercom,
  onUnreadCountChange,
  show,
  showNewMessage,
  update,
} from "@intercom/messenger-js-sdk";
import { useEffect, useState } from "react";

import { useIntercomJwt } from "~/api/auth";
const APP_ID = "r8661p0d";

export function useIntercomAnonymous() {
  useEffect(() => {
    const anonymousConfig = {
      app_id: APP_ID,
    };
    Intercom(anonymousConfig);
  }, []);
}

export function useIntercom() {
  const { intercomJwt } = useIntercomJwt();
  const jwt = intercomJwt?.jwt;

  useEffect(() => {
    if (jwt) {
      Intercom({ app_id: APP_ID, intercom_user_jwt: jwt });
    }
  }, [jwt]);
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

/** Whether the console has an Intercom JWT for the signed-in user. */
export function useHasIntercomJwt() {
  const { intercomJwt } = useIntercomJwt();
  return Boolean(intercomJwt?.jwt);
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
 * check `isIntercomBooted()` at that moment rather than rely on this value.
 */
export function useIsIntercomIdentified() {
  const hasJwt = useHasIntercomJwt();
  const [isBooted, setIsBooted] = useState(isIntercomBooted);

  useEffect(() => {
    if (isBooted) return;
    let attempts = 0;
    let timeout: number;
    const check = () => {
      attempts += 1;
      if (isIntercomBooted()) {
        setIsBooted(true);
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
  }, [isBooted]);

  return hasJwt && isBooted;
}

export const INTERCOM_BOOT_FAST_CHECK_INTERVAL_MS = 1_000;
export const INTERCOM_BOOT_FAST_CHECKS = 30;
export const INTERCOM_BOOT_SLOW_CHECK_INTERVAL_MS = 10_000;

/**
 * Opens a new Messenger conversation. The Messenger's workflow offers the
 * ticket types the user's organization is eligible for, including "Report
 * Production Outage" for SLA customers. Returns false when the Messenger is
 * not available, so callers can fall back to email.
 */
export function openIntercomNewConversation() {
  if (!isIntercomBooted()) return false;
  showNewMessage("");
  return true;
}

/** Opens the Messenger home, where Matty answers questions. */
export function openIntercomMessenger() {
  if (!isIntercomBooted()) return false;
  show();
  return true;
}

/**
 * Number of unread Messenger conversations and ticket replies for the
 * signed-in user. Stays 0 while the Messenger is unavailable.
 */
export function useIntercomUnreadCount() {
  const isIdentified = useIsIntercomIdentified();
  const [unreadCount, setUnreadCount] = useState(0);

  useEffect(() => {
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
  }, [isIdentified]);

  return isIdentified ? unreadCount : 0;
}

export default useIntercom;
