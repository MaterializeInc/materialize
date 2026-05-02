// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export type OpenApiRequestOptions = Omit<RequestInit, "body">;

export type HttpScheme = "http" | "https";
export type WebsocketScheme = "ws" | "wss";

export type TokenAuthConfig = {
  token: string;
};

export type PasswordAuthConfig = {
  user: string;
  password: string;
};

// The auth message the Materialize http/websocket API expects for Cloud.
// Copied from https://materialize.com/docs/integrations/websocket-api/#usage
export type MaterializeAuthConfig = TokenAuthConfig | PasswordAuthConfig;
