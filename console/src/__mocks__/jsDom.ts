// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/** basic stub for dom operations */

// Fix localStorage compatibility with Node.js 22+.
// Node.js 22+ ships an experimental globalThis.localStorage backed by SQLite
// (--localstorage-file). When no valid file is provided the object exists but
// its methods are non-functional, which shadows JSDOM's working implementation.
// Provide a simple in-memory Storage mock so tests always have a working
// localStorage regardless of Node version.
function createStorageMock(): Storage {
  let store: Record<string, string> = {};
  return {
    getItem(key: string) {
      return key in store ? store[key] : null;
    },
    setItem(key: string, value: string) {
      store[key] = String(value);
    },
    removeItem(key: string) {
      delete store[key];
    },
    clear() {
      store = {};
    },
    get length() {
      return Object.keys(store).length;
    },
    key(index: number) {
      return Object.keys(store)[index] ?? null;
    },
  };
}

Object.defineProperty(window, "localStorage", {
  writable: true,
  value: createStorageMock(),
});
Object.defineProperty(window, "sessionStorage", {
  writable: true,
  value: createStorageMock(),
});

// Fix AbortSignal compatibility between JSDOM and undici.
// JSDOM may provide its own AbortController/AbortSignal that are not recognized
// by Node's native fetch (undici), causing "Expected signal to be an instance of
// AbortSignal" errors when constructing Request objects. We try to pass the
// signal through normally; only if it causes a TypeError do we strip it. This
// preserves abort functionality when signals are compatible (most environments).
const _OriginalRequest = globalThis.Request;
globalThis.Request = class extends _OriginalRequest {
  constructor(input: RequestInfo | URL, init?: RequestInit) {
    if (init?.signal) {
      try {
        super(input, init);
      } catch (e) {
        if (e instanceof TypeError) {
          const { signal: _, ...rest } = init;
          super(input, rest);
        } else {
          throw e;
        }
      }
    } else {
      super(input, init);
    }
  }
};

Object.defineProperty(URL, "createObjectURL", {
  writable: true,
  value: vi.fn(),
});

Object.defineProperty(URL, "revokeObjectURL", {
  writable: true,
  value: vi.fn(),
});

Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: vi.fn().mockImplementation((query) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(), // deprecated
    removeListener: vi.fn(), // deprecated
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});
