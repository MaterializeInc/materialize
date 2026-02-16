// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Navigate, Route } from "react-router-dom";

import { User } from "~/external-library-wrappers/frontegg";
import { SentryRoutes } from "~/sentry";

import EnableRegion from "./EnableRegion";
import { EnvironmentNotReadyLayout } from "./Layout";
import { OnboardingSteps } from "./OnboardingSteps";
import OnboardingSurvey from "./OnboardingSurvey";

export const EnvironmentNotReadyRoutes = ({ user }: { user: User }) => {
  return (
    <EnvironmentNotReadyLayout user={user}>
      <SentryRoutes>
        <Route
          path="onboarding-survey"
          element={<OnboardingSurvey user={user} />}
        />
        <Route path="enable-region" element={<EnableRegion user={user} />} />
        <Route path=":step" element={<OnboardingSteps user={user} />} />
        <Route
          path="*"
          element={<Navigate to="../onboarding-survey" replace />}
        />
      </SentryRoutes>
    </EnvironmentNotReadyLayout>
  );
};
