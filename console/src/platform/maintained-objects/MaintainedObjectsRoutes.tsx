import React from "react";
import { Route } from "react-router-dom";

import { SentryRoutes } from "~/sentry";

import { MaintainedObjects } from "./MaintainedObjects";

export const MaintainedObjectsRoutes = () => {
  return (
    <SentryRoutes>
      <Route path="*" element={<MaintainedObjects />} />
    </SentryRoutes>
  );
};
