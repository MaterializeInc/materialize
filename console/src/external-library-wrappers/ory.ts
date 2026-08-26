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
 * This file is a facade for the @ory/client-fetch library.
 * It is used primarily to mock the Ory client in tests via `vi.mock` in ~/vitest.setup.ts.
 * Make sure anything you'd like to mock is updated in ./__mocks__/ory.ts
 *
 * We use @ory/client-fetch rather than @ory/client because it is maintained
 * against the current Kratos and Hydra APIs, and its models describe a flow's
 * UI nodes as a discriminated union, which is what lets a renderer narrow on
 * `node_type` without casting.
 */
export {
  Configuration,
  FrontendApi,
  type LoginFlow,
  ResponseError,
  type UiNode,
} from "@ory/client-fetch";
