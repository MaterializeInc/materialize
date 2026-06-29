// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import useCurrentUser from "~/api/materialize/useCurrentUser";
import { type AuthContextProps } from "~/external-library-wrappers/oidc";

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
export const useSelfManagedProfile = (
  auth: AuthContextProps | undefined,
): SelfManagedProfile => {
  const profile = auth?.user?.profile;
  const isProfileValid =
    typeof profile?.exp === "number" && profile.exp * 1000 > Date.now();
  const oidcProfile = isProfileValid ? profile : undefined;
  const { results: sqlRole, isLoading } = useCurrentUser();

  const oidcName =
    typeof oidcProfile?.name === "string" ? oidcProfile.name : undefined;
  const oidcEmail =
    typeof oidcProfile?.email === "string" ? oidcProfile.email : undefined;
  const oidcPicture =
    typeof oidcProfile?.picture === "string" ? oidcProfile.picture : undefined;

  return {
    name: oidcName ?? sqlRole,
    email: oidcEmail,
    picture: oidcPicture,
    sqlRole,
    isLoading,
  };
};
