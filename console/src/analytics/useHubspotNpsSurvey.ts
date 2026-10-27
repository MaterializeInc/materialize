// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useEffect, useState } from "react";

import { useCurrentOrganization } from "~/api/auth";
import { useWelcomeDialog } from "~/components/WelcomeDialog/useWelcomeDialog";
import { User } from "~/external-library-wrappers/frontegg";
import { useFlags } from "~/hooks/useFlags";

const HUBSPOT_SCRIPT_SRC = "//js.hs-scripts.com/23399445.js";

const useHubspotNpsSurvey = ({ user }: { user: User }) => {
  const { organization } = useCurrentOrganization();
  const flags = useFlags();
  const isNpsEnabled = flags["enable-nps-3190"] === true;
  // Flag used to trigger the NPS survey for internal users
  const internalNpsOverride =
    flags["nps-survey-internal-testing-3190"] === true;

  const [welcomeDialogSeen] = useWelcomeDialog();

  const [isHubspotTrackingCodeLoaded, setIsHubspotTrackingCodeLoaded] =
    useState(false);

  useEffect(() => {
    function loadHubspotScript() {
      const scriptTag = document.createElement("script");
      scriptTag.src = HUBSPOT_SCRIPT_SRC;
      scriptTag.async = true;
      scriptTag.defer = true;
      scriptTag.onload = () => setIsHubspotTrackingCodeLoaded(true);
      document.body.appendChild(scriptTag);
    }

    const isPayingCustomer =
      organization?.subscription?.type === "capacity" ||
      organization?.subscription?.type === "on-demand";

    if (
      !isHubspotTrackingCodeLoaded &&
      isNpsEnabled &&
      user &&
      (isPayingCustomer || internalNpsOverride) &&
      organization?.blocked === false &&
      welcomeDialogSeen
    ) {
      // window._hsq is the global object the hubspot tracking code uses as its event queue. We can initialize and prefill it before we load the script.
      // This is useful because we can't tell when the Hubspot script is fully loaded.
      if (!window._hsq) {
        window._hsq = [];
      }

      window._hsq.push([
        "identify",
        {
          email: user.email,
        },
      ]);
      // After identification, we need to trigger a page view to show the survey
      window._hsq.push(["trackPageView"]);

      loadHubspotScript();
    }
  }, [
    isNpsEnabled,
    internalNpsOverride,
    organization,
    user,
    isHubspotTrackingCodeLoaded,
    welcomeDialogSeen,
  ]);
};

export default useHubspotNpsSurvey;
