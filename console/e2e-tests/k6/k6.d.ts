// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Minimal type declarations for k6's runtime APIs used by cluster-detail.ts.
// k6 publishes a more complete `@types/k6` package; declaring only what we
// touch keeps this single-script change self-contained.

declare module "k6" {
  export function check<R>(
    val: R,
    sets: Record<string, (r: R) => boolean>,
    tags?: Record<string, string>,
  ): boolean;
  export function sleep(t: number): void;
}

declare module "k6/http" {
  export interface RefinedResponse {
    status: number;
    body: string | ArrayBuffer | null;
  }
  interface Params {
    headers?: Record<string, string>;
    tags?: Record<string, string>;
  }
  const http: {
    post(url: string, body: string, params?: Params): RefinedResponse;
  };
  export default http;
}
