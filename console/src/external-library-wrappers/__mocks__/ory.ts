// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { LoginFlow } from "~/external-library-wrappers/ory";

export {
  Configuration,
  type LoginFlow,
  ResponseError,
  type UiNode,
} from "~/external-library-wrappers/ory";

export const MOCK_FLOW_ID = "00000000-0000-0000-0000-000000000001";
export const MOCK_CSRF_TOKEN = "mock-csrf-token";

// The SSO-only shape Materialize deploys: one provider button plus the flow
// state that has to be replayed on submit.
export const dummyLoginFlow = {
  id: MOCK_FLOW_ID,
  type: "browser",
  expires_at: new Date(Date.now() + 30 * 60 * 1000).toISOString(),
  issued_at: new Date().toISOString(),
  request_url: "https://auth.example.com/self-service/login/browser",
  ui: {
    action: `https://auth.example.com/self-service/login?flow=${MOCK_FLOW_ID}`,
    method: "POST",
    nodes: [
      {
        type: "input",
        group: "oidc",
        attributes: {
          name: "provider",
          type: "submit",
          value: "auth0",
          disabled: false,
          node_type: "input",
        },
        messages: [],
        meta: {
          label: { id: 1010002, text: "Sign in with Auth0", type: "info" },
        },
      },
      {
        type: "input",
        group: "default",
        attributes: {
          name: "csrf_token",
          type: "hidden",
          value: MOCK_CSRF_TOKEN,
          required: true,
          disabled: false,
          node_type: "input",
        },
        messages: [],
        meta: {},
      },
    ],
  },
} as unknown as LoginFlow;

// Two providers, so the chooser is a real choice and the page renders it
// instead of redirecting. Also covers the node iteration: nothing about the
// renderer may be specific to one provider.
export const dummyMultiProviderLoginFlow = {
  ...dummyLoginFlow,
  ui: {
    ...dummyLoginFlow.ui,
    nodes: [
      dummyLoginFlow.ui.nodes[0],
      {
        type: "input",
        group: "oidc",
        attributes: {
          name: "provider",
          type: "submit",
          value: "okta",
          disabled: false,
          node_type: "input",
        },
        messages: [],
        meta: {
          label: { id: 1010002, text: "Sign in with Okta", type: "info" },
        },
      },
      dummyLoginFlow.ui.nodes[1],
    ],
  },
} as unknown as LoginFlow;

// Step one of an identifier-first flow: Kratos asks who you are before it will
// say which providers can serve you.
export const dummyIdentifierFirstLoginFlow = {
  ...dummyLoginFlow,
  ui: {
    ...dummyLoginFlow.ui,
    nodes: [
      dummyLoginFlow.ui.nodes[1],
      {
        type: "input",
        group: "identifier_first",
        attributes: {
          name: "identifier",
          type: "text",
          value: "",
          required: true,
          disabled: false,
          node_type: "input",
        },
        messages: [],
        meta: { label: { id: 1070004, text: "ID", type: "info" } },
      },
      {
        type: "input",
        group: "identifier_first",
        attributes: {
          name: "method",
          type: "submit",
          value: "identifier_first",
          disabled: false,
          node_type: "input",
        },
        messages: [],
        meta: { label: { id: 1010001, text: "Continue", type: "info" } },
      },
    ],
  },
} as unknown as LoginFlow;

// A password flow, which this deployment does not configure. Used to check that
// an unrenderable method is reported rather than drawn as an empty form.
export const dummyPasswordLoginFlow = {
  ...dummyLoginFlow,
  ui: {
    ...dummyLoginFlow.ui,
    nodes: [
      {
        type: "input",
        group: "default",
        attributes: {
          name: "csrf_token",
          type: "hidden",
          value: MOCK_CSRF_TOKEN,
          required: true,
          disabled: false,
          node_type: "input",
        },
        messages: [],
        meta: {},
      },
      {
        type: "input",
        group: "password",
        attributes: {
          name: "password",
          type: "password",
          required: true,
          disabled: false,
          node_type: "input",
        },
        messages: [],
        meta: { label: { id: 1070001, text: "Password", type: "info" } },
      },
    ],
  },
} as unknown as LoginFlow;

export const createBrowserLoginFlow = vi.fn(async () => dummyLoginFlow);
export const getLoginFlow = vi.fn(async () => dummyLoginFlow);

export const FrontendApi = vi.fn(() => ({
  createBrowserLoginFlow,
  getLoginFlow,
}));
