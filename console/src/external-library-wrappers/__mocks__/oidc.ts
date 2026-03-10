// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export const MOCK_OIDC_ID_TOKEN = "mock-oidc-id-token";

export const useAuth = vi.fn(() => ({
  isAuthenticated: false,
  isLoading: false,
  user: null,
  signinRedirect: vi.fn(),
  signoutRedirect: vi.fn(),
}));

export const AuthProvider = ({ children }: React.PropsWithChildren) => children;

export const hasAuthParams = vi.fn(() => false);

export const UserManager = vi.fn();

export const initOidcUserManager = vi.fn();
export const getOidcUserManager = vi.fn(() => null);
export const getOidcIdToken = vi.fn(() => MOCK_OIDC_ID_TOKEN);
