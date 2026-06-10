// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Route } from "react-router-dom";

import { useSchemaObjectFilters } from "~/components/SchemaObjectFilter/useSchemaObjectFilters";
import { SentryRoutes } from "~/sentry";

import NoticeDetail from "./NoticeDetail";
import NoticeList from "./NoticeList";

const NoticeRoutes = () => {
  const { databaseFilter, schemaFilter, nameFilter } =
    useSchemaObjectFilters("noticeName");

  return (
    <SentryRoutes>
      <Route
        path="/"
        element={
          <NoticeList
            databaseFilter={databaseFilter}
            schemaFilter={schemaFilter}
            nameFilter={nameFilter}
          />
        }
      />
      <Route path=":noticeKey" element={<NoticeDetail />} />
    </SentryRoutes>
  );
};

export default NoticeRoutes;
