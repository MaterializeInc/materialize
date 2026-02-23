// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { isSafeRedirectUrl } from "./redirectUrl";

describe("isSafeRedirectUrl", () => {
  describe("safe URLs", () => {
    it("accepts root path", () => {
      expect(isSafeRedirectUrl("/")).toBe(true);
    });

    it("accepts simple relative paths", () => {
      expect(isSafeRedirectUrl("/dashboard")).toBe(true);
      expect(isSafeRedirectUrl("/regions/aws-us-east-1")).toBe(true);
      expect(isSafeRedirectUrl("/access/app-passwords")).toBe(true);
    });

    it("accepts paths with query strings", () => {
      expect(isSafeRedirectUrl("/dashboard?tab=overview")).toBe(true);
      expect(isSafeRedirectUrl("/search?q=test&page=1")).toBe(true);
    });

    it("accepts paths with hash fragments", () => {
      expect(isSafeRedirectUrl("/docs#section")).toBe(true);
      expect(isSafeRedirectUrl("/page?query=1#anchor")).toBe(true);
    });
  });

  describe("unsafe URLs - protocol-relative", () => {
    it("rejects protocol-relative URLs", () => {
      expect(isSafeRedirectUrl("//example.com")).toBe(false);
      expect(isSafeRedirectUrl("//evil.com/path")).toBe(false);
      expect(isSafeRedirectUrl("//attacker.com?steal=cookies")).toBe(false);
    });
  });

  describe("unsafe URLs - absolute URLs", () => {
    it("rejects http URLs", () => {
      expect(isSafeRedirectUrl("http://example.com")).toBe(false);
      expect(isSafeRedirectUrl("http://evil.com/phishing")).toBe(false);
    });

    it("rejects https URLs", () => {
      expect(isSafeRedirectUrl("https://example.com")).toBe(false);
      expect(isSafeRedirectUrl("https://attacker.com")).toBe(false);
    });

    it("rejects javascript URLs", () => {
      expect(isSafeRedirectUrl("javascript:alert(1)")).toBe(false);
      expect(isSafeRedirectUrl("javascript:void(0)")).toBe(false);
      expect(isSafeRedirectUrl("JAVASCRIPT:alert(1)")).toBe(false);
    });

    it("rejects data URLs", () => {
      expect(
        isSafeRedirectUrl("data:text/html,<script>alert(1)</script>"),
      ).toBe(false);
    });

    it("rejects other protocol URLs", () => {
      expect(isSafeRedirectUrl("ftp://example.com")).toBe(false);
      expect(isSafeRedirectUrl("file:///etc/passwd")).toBe(false);
      expect(isSafeRedirectUrl("mailto:test@example.com")).toBe(false);
    });
  });

  describe("unsafe URLs - edge cases", () => {
    it("rejects URLs with backslashes", () => {
      expect(isSafeRedirectUrl("/\\example.com")).toBe(false);
      expect(isSafeRedirectUrl("\\/example.com")).toBe(false);
    });

    it("rejects empty or whitespace URLs", () => {
      expect(isSafeRedirectUrl("")).toBe(false);
      expect(isSafeRedirectUrl("   ")).toBe(false);
    });

    it("rejects URLs not starting with /", () => {
      expect(isSafeRedirectUrl("example.com")).toBe(false);
      expect(isSafeRedirectUrl("path/to/page")).toBe(false);
    });

    it("rejects URLs with tab characters (WHATWG URL parser strips these)", () => {
      // Browser URL parsers strip tabs per WHATWG spec
      // /\t/evil.com becomes //evil.com after parsing
      expect(isSafeRedirectUrl("/\t/evil.com")).toBe(false);
      expect(isSafeRedirectUrl("/foo\t/bar")).toBe(false);
    });

    it("rejects URLs with newline characters (WHATWG URL parser strips these)", () => {
      // Browser URL parsers strip newlines per WHATWG spec
      // /\n/evil.com becomes //evil.com after parsing
      expect(isSafeRedirectUrl("/\n/evil.com")).toBe(false);
      expect(isSafeRedirectUrl("/foo\n/bar")).toBe(false);
    });

    it("rejects URLs with carriage return characters (WHATWG URL parser strips these)", () => {
      // Browser URL parsers strip carriage returns per WHATWG spec
      // /\r/evil.com becomes //evil.com after parsing
      expect(isSafeRedirectUrl("/\r/evil.com")).toBe(false);
      expect(isSafeRedirectUrl("/foo\r/bar")).toBe(false);
    });
  });
});
