// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/* eslint-disable no-restricted-imports */
/**
 * This file is a facade for the frontegg library.
 * It is used primarily to mock the frontegg library in tests via `vi.mock` in ~/vitest.setup.ts.
 * Make sure anything you'd like to mock is updated in ./__mocks__/frontegg.ts
 *
 * Hooks exported from this file will throw errors if the user is in an unsupported mode such as self-managed or impersonation.
 * In general, it is not recommended to use these hooks directly and instead use the AppConfigSwitch component.
 */
export {
  AdminPortal,
  ContextHolder,
  FronteggProvider,
  type FronteggProviderProps,
  type FronteggThemeOptions,
  useAuth,
  useAuthActions,
  useAuthUser,
  useAuthUserOrNull,
  useIsAuthenticated,
} from "@frontegg/react";
export {
  type AuthActions,
  type AuthState,
  type User,
} from "@frontegg/redux-store";
export {
  type ISignUpCompletePayload,
  type IUserVerifiedPayload,
} from "@frontegg/redux-store/auth/interfaces";
export {
  type ITeamUserPermission,
  type ITeamUserRole,
  type ITenantsResponse,
  UserManagedByEnum,
} from "@frontegg/rest-api";
export { SocialLoginProviders } from "@frontegg/rest-api/auth/enums";
