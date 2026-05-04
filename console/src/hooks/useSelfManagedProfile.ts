// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import useCurrentUser from "~/api/materialize/useCurrentUser";
import { useAuth } from "~/external-library-wrappers/oidc";

export interface SelfManagedProfile {
  name: string | undefined;
  email: string | undefined;
  picture: string | undefined;
  /** The SQL role (`current_user`) — authoritative for what Materialize sees. */
  sqlRole: string | undefined;
  isLoading: boolean;
}

/** Unified identity for self-managed UI: OIDC claims when present, with the
 * SQL role as the authoritative fallback. */
export const useSelfManagedProfile = (): SelfManagedProfile => {
  const auth = useAuth();
  const profile = auth?.user?.profile;
  const { results: sqlRole, isLoading } = useCurrentUser();

  const oidcName = typeof profile?.name === "string" ? profile.name : undefined;
  const oidcEmail =
    typeof profile?.email === "string" ? profile.email : undefined;
  const oidcPicture =
    typeof profile?.picture === "string" ? profile.picture : undefined;

  return {
    name: oidcName ?? sqlRole,
    email: oidcEmail,
    picture: oidcPicture,
    sqlRole,
    isLoading,
  };
};
