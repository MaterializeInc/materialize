// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { http, HttpResponse } from "msw";

import { STATUSPAGE_URL } from "~/api/incident-io";
import { StatusSummary } from "~/api/incident-io/types";

export const healthyStatus: StatusSummary = {
  page_url: "https://mz.example.com",
  page_title: "Materialize Incidents",
  ongoing_incidents: [],
  in_progress_maintenances: [],
  scheduled_maintenances: [],
};

export const healthyStatusHandler = http.get(STATUSPAGE_URL, () =>
  HttpResponse.json(healthyStatus),
);

export default [healthyStatusHandler];
