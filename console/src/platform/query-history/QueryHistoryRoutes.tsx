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

import StripSearchIfInvalidZodSchema from "~/components/StripSearchIfInvalidZodSchema";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { SentryRoutes } from "~/sentry";

import QueryHistoryDetail from "./QueryHistoryDetail";
import QueryHistoryList from "./QueryHistoryList";
import { mapQueryHistorySearchParams } from "./queryHistoryUtils";
import { queryHistoryListUrlSchema } from "./utils";

export type ListToDetailsPageLocationState = {
  from?: {
    search?: string;
  };
};

const QueryHistoryRoutesContent = ({
  currentUserEmail,
}: {
  // The email of the current user for Cloud. Used to set the default user filter.
  currentUserEmail?: string;
}) => {
  return (
    <SentryRoutes>
      <Route
        path="/"
        element={
          <StripSearchIfInvalidZodSchema
            schema={queryHistoryListUrlSchema}
            mapSearchParamsToSchema={(searchParams) =>
              mapQueryHistorySearchParams(searchParams, currentUserEmail)
            }
          >
            {(validatedSearchParamData) => {
              const { columns, ...initialFilters } = validatedSearchParamData;
              return (
                <QueryHistoryList
                  initialFilters={initialFilters}
                  initialColumns={columns}
                  currentUserEmail={currentUserEmail}
                />
              );
            }}
          </StripSearchIfInvalidZodSchema>
        }
      />
      <Route path=":id" element={<QueryHistoryDetail />} />
      <Route path="*" element={<Navigate to="/" />} />
    </SentryRoutes>
  );
};

const QueryHistoryRoutes = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig }) =>
        runtimeConfig.isImpersonating ? (
          <QueryHistoryRoutesContent />
        ) : (
          <QueryHistoryRoutesContent
            currentUserEmail={runtimeConfig.user.email}
          />
        )
      }
      selfManagedConfigElement={() => <QueryHistoryRoutesContent />}
    />
  );
};

export default QueryHistoryRoutes;
