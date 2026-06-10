// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useMaybeCurrentOrganizationId } from "~/api/auth";
import useLocalStorage from "~/hooks/useLocalStorage";

export function useShowSystemObjects() {
  const maybeOrganizationId = useMaybeCurrentOrganizationId();
  const organizationIdKeyPrefix = maybeOrganizationId?.data
    ? `${maybeOrganizationId?.data ?? ""}-`
    : "";
  return useLocalStorage(
    `${organizationIdKeyPrefix}-show-system-objects`,
    false,
  );
}
