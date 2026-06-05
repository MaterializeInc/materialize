// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useLocation } from "react-router-dom";

/** Soft cap so the breadcrumb can't grow without bound on long drill-downs. */
const JOURNEY_CAP = 5;

/** Returns the `state` value to pass to a `<Link>` that drills from the
 *  current object to another, extending the breadcrumb trail. */
export const useJourneyLinkState = (pageObjectId: string) => {
  const { state } = useLocation();
  const journey: string[] = state?.journey ?? [];
  return { journey: [...journey, pageObjectId].slice(-JOURNEY_CAP) };
};
