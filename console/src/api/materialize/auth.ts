// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { NOT_SUPPORTED_MESSAGE } from "~/config/AppConfig";
import storageAvailable from "~/utils/storageAvailable";

import { apiClient, type SelfManagedApiClient } from "../apiClient";

interface LoginRequest {
  username: string;
  password: string;
}

const getApiClient = () => {
  if (
    apiClient.type !== "self-managed" ||
    (apiClient.authMode !== "Password" &&
      apiClient.authMode !== "Sasl" &&
      apiClient.authMode !== "Oidc")
  ) {
    throw new Error(NOT_SUPPORTED_MESSAGE);
  }
  return apiClient;
};

export const LOGIN_PATH = "/account/login";

// sessionStorage key used to hand a one-shot auth-error message to the login
// page after a callback/logout redirect. The Login component reads and
// removes the entry on mount, so the error never persists across refresh.
export const LOGIN_ERROR_STORAGE_KEY = "mz-login-error";

// Login API for to self-managed in password auth mode.
// Throws if the API isn't supported for the deployment/auth mode.
export async function loginOrThrow(request: { payload: LoginRequest }) {
  const { authApiBasePath } = getApiClient();

  const response = await fetch(`${authApiBasePath}/api/login`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request.payload),
  });

  const responseText = await response.text();

  if (!response.ok) {
    if (response.status === 401) {
      throw new Error("Invalid credentials", {
        cause: { status: response.status },
      });
    }
    throw new Error(responseText, {
      cause: { status: response.status },
    });
  }
  return responseText;
}

export async function logout(logoutParams: {
  apiClient: SelfManagedApiClient;
}) {
  const { authApiBasePath } = logoutParams.apiClient;

  const response = await fetch(`${authApiBasePath}/api/logout`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
  });

  const responseText = await response.text();

  const isFetchFailure = !response.ok;
  if (isFetchFailure) {
    throw new Error(responseText, {
      cause: { status: response.status },
    });
  }
  return responseText;
}

export async function logoutAndRedirect(logoutParams: {
  apiClient: SelfManagedApiClient;
  error?: string;
}) {
  logout(logoutParams);
  if (logoutParams.error && storageAvailable("sessionStorage")) {
    window.sessionStorage.setItem(LOGIN_ERROR_STORAGE_KEY, logoutParams.error);
  }
  window.location.href = LOGIN_PATH;
}

export async function logoutAndRedirectOrThrow() {
  logout({ apiClient: getApiClient() });
  window.location.href = LOGIN_PATH;
}
