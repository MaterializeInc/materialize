// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export * from "~/external-library-wrappers/frontegg";
import type {
  ITeamUserPermission,
  User,
} from "~/external-library-wrappers/frontegg";

export const dummyValidUser: User = {
  accessToken: "access_token",
  refreshToken: "refresh_token",
  expiresIn: 1100000,
  expires: "",
  exp: 0,
  id: "1",
  email: "user@example.com",
  tenantIds: [],
  metadata: JSON.stringify({}),
  mfaEnrolled: true,
  name: "user",
  permissions: [
    {
      key: "materialize.environment.write",
    },
    {
      key: "materialize.environment.read",
    },
    {
      key: "fe.secure.read.tenantApiTokens",
    },
    {
      key: "fe.secure.write.tenantApiTokens",
    },
    {
      key: "fe.secure.delete.tenantApiTokens",
    },
    {
      key: "materialize.invoice.read",
    },
  ] as ITeamUserPermission[],
  profilePictureUrl: "https://cdn.com/image",
  roles: [
    {
      name: "Materialize platform admin",
      id: "",
      key: "MaterializePlatformAdmin",
      isDefault: false,
      permissions: [],
      vendorId: "",
      createdAt: new Date(0),
      updatedAt: new Date(0),
    },
  ],
  tenantId: "tenant-id",
  sub: "",
  verified: true,
  tenants: [],
};

export const useAuth = vi.fn(() => ({
  tenantsState: {
    tenants: [
      {
        tenantId: "tenant-id",
        name: "tenant-name",
      },
    ],
  },
}));

export const useAuthActions = vi.fn(() => ({
  switchTenant: vi.fn(),
}));

export const useAuthUser = vi.fn(() => dummyValidUser);

export const MOCK_ACCESS_TOKEN = "mock-access-token";
// ContextHolder is a global object that allows access to the Frontegg Redux store.
// It's used to get the API access token for the current user.
export const ContextHolder = {
  for: vi.fn(() => ({
    getAccessToken: vi.fn(() => MOCK_ACCESS_TOKEN),
  })),
};
