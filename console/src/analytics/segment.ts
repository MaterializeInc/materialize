// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  AnalyticsBrowser,
  Callback,
  EventProperties,
  Options,
  SegmentEvent,
} from "@segment/analytics-next";
import React, { useCallback } from "react";
import { useLocation } from "react-router-dom";

import { useMaybeCurrentOrganizationId } from "~/api/auth";
import { appConfigAtom } from "~/config/store";
import { User } from "~/external-library-wrappers/frontegg";
import { getStore } from "~/jotai";

const appConfig = getStore().get(appConfigAtom);

const segmentApiKey =
  appConfig.mode === "cloud" && appConfig.segmentApiKey !== null
    ? appConfig.segmentApiKey
    : null;

export const segment = segmentApiKey ? new AnalyticsBrowser() : null;

if (segment && segmentApiKey !== null) {
  segment.load(
    {
      writeKey: segmentApiKey,
    },
    {
      integrations: {
        // Use the Materialize-specific Segment proxy, which is less likely to
        // be on public ad blocking lists.
        "Segment.io": {
          apiHost: "api.segment.materialize.com/v1",
        },
      },
      retryQueue: true,
      // Use cookies first, since the work across subdomains. This ensures that logged in
      // users will correctly be track when the visit the docs and marketing site.
      storage: {
        stores: ["cookie", "localStorage", "memory"],
      },
    },
  );
}

/*
 * A React hook that returns a user-aware Segment client.
 *
 * The returned Segment client functions like Segment's standard
 * `AnalyticsBrowser` client, but calls to `track` attach the current
 * organization ID to the event.
 *
 */
export const useSegment = segment
  ? () => {
      const maybeOrganizationId = useMaybeCurrentOrganizationId();

      const track = useCallback(
        (
          eventName: string | SegmentEvent,
          properties?: EventProperties | Callback | undefined,
          options?: Callback | Options | undefined,
          callback?: Callback | undefined,
        ) => {
          if (!maybeOrganizationId || maybeOrganizationId.data === undefined) {
            return;
          }

          segment.track(
            eventName,
            properties,
            {
              groupId: maybeOrganizationId.data,
              ...options,
            },
            callback,
          );
        },
        [maybeOrganizationId],
      );

      return { track };
    }
  : () => ({ track: () => null });

export const useSegmentPageTracking = ({ user }: { user: User }) => {
  const location = useLocation();

  React.useEffect(() => {
    if (!segment) {
      return;
    }
    segment.page(
      undefined, // category
      undefined, // name
      {
        // Include the hash because the Frontegg admin portal uses the hash
        // for routing.
        hash: location.hash,
      },
      {
        groupId: user.tenantId,
      },
    );
  }, [location, user]);

  React.useEffect(() => {
    if (!segment) {
      return;
    }
    segment.identify(user.id);
  }, [user]);
};
