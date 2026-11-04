// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// The package ships without types. This covers the surface the worksheet uses,
// per https://github.com/observablehq/runtime#api-reference.
declare module "@observablehq/runtime" {
  export interface Observer<T = unknown> {
    pending?(): void;
    fulfilled?(value: T, name?: string | null): void;
    rejected?(error: unknown, name?: string | null): void;
  }

  export class Variable {
    define(
      name: string | null,
      inputs: string[],
      definition: (...inputs: never[]) => unknown,
    ): this;
    define(inputs: string[], definition: (...inputs: never[]) => unknown): this;
    delete(): this;
  }

  export class Module {
    variable(observer?: Observer): Variable;
  }

  export class Runtime {
    /** `global` resolves names no variable defines. */
    constructor(
      builtins?: Record<string, unknown>,
      global?: (name: string) => unknown,
    );
    module(): Module;
    dispose(): void;
  }

  export class RuntimeError extends Error {}
}
