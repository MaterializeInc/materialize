// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { BrowserRouterProps } from "react-router-dom";

/**
 * Opt-ins to react-router v7 behavior, to be passed to every router we mount,
 * in the app and in tests alike.
 *
 * `v7_relativeSplatPath` changes what `..` resolves to inside a splat route, so
 * a router without it resolves relative navigation differently from one with
 * it. Our route tree leans on both splat routes and relative `<Navigate>`, so a
 * test router missing these flags exercises paths the app never takes.
 */
export const ROUTER_FUTURE_FLAGS: BrowserRouterProps["future"] = {
  v7_relativeSplatPath: true,
  v7_startTransition: true,
};
