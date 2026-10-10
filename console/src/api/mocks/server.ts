// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { HttpRequestEventMap, Interceptor } from "@mswjs/interceptors";
import { ClientRequestInterceptor } from "@mswjs/interceptors/ClientRequest";
import { FetchInterceptor } from "@mswjs/interceptors/fetch/web";
import { WebSocketInterceptor } from "@mswjs/interceptors/WebSocket";
import { XMLHttpRequestInterceptor } from "@mswjs/interceptors/XMLHttpRequest";
import { defineNetwork, InterceptorSource } from "msw/experimental";
import { defaultNetworkOptions } from "msw/node";

import cloudGlobalApiHandlers from "./cloudGlobalApiHandlers";
import cloudRegionApiHandlers from "./cloudRegionApiHandlers";
import incidentIOHandlers from "./incidentIOHandlers";
import materializeHandlers from "./materializeHandlers";

// msw's socket-level fetch interception leaks undici's post-abort reconnects
// to the real network. TODO: Use setupServer once mswjs/interceptors#863 ships.
export default defineNetwork({
  ...defaultNetworkOptions,
  sources: [
    new InterceptorSource({
      interceptors: [
        new ClientRequestInterceptor(),
        new XMLHttpRequestInterceptor(),
        // The browser build declares its own, nominally distinct Interceptor.
        new FetchInterceptor() as unknown as Interceptor<HttpRequestEventMap>,
        new WebSocketInterceptor(),
      ],
    }),
  ],
  handlers: [
    ...cloudGlobalApiHandlers,
    ...cloudRegionApiHandlers,
    ...materializeHandlers,
    ...incidentIOHandlers,
  ],
});
