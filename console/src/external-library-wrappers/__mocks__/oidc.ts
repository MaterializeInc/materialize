// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export const useAuth = vi.fn(() => ({
  isAuthenticated: false,
  isLoading: false,
  user: null,
  signinRedirect: vi.fn(),
  signoutRedirect: vi.fn(),
}));

export const AuthProvider = ({ children }: React.PropsWithChildren) => children;

export const hasAuthParams = vi.fn(() => false);

export class MzOidcUserManager {
  getIdToken = vi.fn(() => undefined);
  getUserManager = vi.fn();
  signoutRedirect = vi.fn();

  static create = vi.fn(() => Promise.resolve(new MzOidcUserManager()));
}
