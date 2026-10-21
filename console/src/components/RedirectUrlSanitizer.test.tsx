// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen } from "@testing-library/react";
import React from "react";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";

import { RedirectUrlSanitizer } from "./RedirectUrlSanitizer";

const TestContent = () => <div data-testid="test-content">Test Content</div>;
const LoginPage = () => <div data-testid="login-page">Login Page</div>;

const renderWithRouter = (allowedOrigins?: string[]) => {
  return render(
    <BrowserRouter>
      <RedirectUrlSanitizer allowedOrigins={allowedOrigins}>
        <Routes>
          <Route path="/account/login" element={<LoginPage />} />
          <Route path="*" element={<TestContent />} />
        </Routes>
      </RedirectUrlSanitizer>
    </BrowserRouter>,
  );
};

describe("RedirectUrlSanitizer", () => {
  beforeEach(() => {
    history.pushState(undefined, "", "/");
    localStorage.removeItem("FRONTEGG_AFTER_AUTH_REDIRECT_URL");
  });

  describe("non-login routes", () => {
    it("renders children on non-login routes", () => {
      history.pushState(undefined, "", "/");
      renderWithRouter();
      expect(screen.getByTestId("test-content")).toBeInTheDocument();
    });

    it("renders children on non-login routes even with malicious redirectUrl", () => {
      history.pushState(undefined, "", "/dashboard?redirectUrl=//evil.com");
      renderWithRouter();
      expect(screen.getByTestId("test-content")).toBeInTheDocument();
      // URL should remain unchanged on non-login routes
      expect(window.location.search).toBe("?redirectUrl=//evil.com");
    });
  });

  describe("login route without redirectUrl", () => {
    it("renders login page when no redirectUrl is present", () => {
      history.pushState(undefined, "", "/account/login");
      renderWithRouter();
      expect(screen.getByTestId("login-page")).toBeInTheDocument();
      expect(window.location.search).toBe("");
    });
  });

  describe("login route with safe redirectUrl", () => {
    it("renders login page with safe relative path and preserves URL", () => {
      history.pushState(
        undefined,
        "",
        "/account/login?redirectUrl=%2Fdashboard",
      );
      renderWithRouter();
      expect(screen.getByTestId("login-page")).toBeInTheDocument();
      // Safe URLs should be preserved
      expect(window.location.search).toBe("?redirectUrl=%2Fdashboard");
    });

    it("renders login page with safe path including query string", () => {
      history.pushState(
        undefined,
        "",
        "/account/login?redirectUrl=%2Fsearch%3Fq%3Dtest",
      );
      renderWithRouter();
      expect(screen.getByTestId("login-page")).toBeInTheDocument();
      expect(window.location.search).toBe("?redirectUrl=%2Fsearch%3Fq%3Dtest");
    });
  });

  describe("login route with malicious redirectUrl", () => {
    it("strips malicious protocol-relative URL", () => {
      const maliciousSearch = "?redirectUrl=%2F%2Fevil.com";
      history.pushState(undefined, "", `/account/login${maliciousSearch}`);
      expect(window.location.search).toBe(maliciousSearch);

      renderWithRouter();

      expect(screen.getByTestId("login-page")).toBeInTheDocument();
      // URL should be stripped - redirectUrl param removed
      expect(window.location.search).toBe("");
      expect(window.location.pathname).toBe("/account/login");
    });

    it("strips malicious absolute http URL", () => {
      const maliciousSearch = "?redirectUrl=http%3A%2F%2Fevil.com";
      history.pushState(undefined, "", `/account/login${maliciousSearch}`);
      expect(window.location.search).toBe(maliciousSearch);

      renderWithRouter();

      expect(screen.getByTestId("login-page")).toBeInTheDocument();
      expect(window.location.search).toBe("");
      expect(window.location.pathname).toBe("/account/login");
    });

    it("strips malicious absolute https URL", () => {
      const maliciousSearch = "?redirectUrl=https%3A%2F%2Fevil.com";
      history.pushState(undefined, "", `/account/login${maliciousSearch}`);
      expect(window.location.search).toBe(maliciousSearch);

      renderWithRouter();

      expect(screen.getByTestId("login-page")).toBeInTheDocument();
      expect(window.location.search).toBe("");
      expect(window.location.pathname).toBe("/account/login");
    });

    it("strips malicious javascript URL", () => {
      const maliciousSearch = "?redirectUrl=javascript%3Aalert%281%29";
      history.pushState(undefined, "", `/account/login${maliciousSearch}`);
      expect(window.location.search).toBe(maliciousSearch);

      renderWithRouter();

      expect(screen.getByTestId("login-page")).toBeInTheDocument();
      expect(window.location.search).toBe("");
      expect(window.location.pathname).toBe("/account/login");
    });

    it("strips URLs containing backslashes", () => {
      const maliciousSearch = "?redirectUrl=%2F%5Cevil.com";
      history.pushState(undefined, "", `/account/login${maliciousSearch}`);
      expect(window.location.search).toBe(maliciousSearch);

      renderWithRouter();

      expect(screen.getByTestId("login-page")).toBeInTheDocument();
      expect(window.location.search).toBe("");
      expect(window.location.pathname).toBe("/account/login");
    });
  });

  describe("login route with an allowed external redirectUrl", () => {
    const allowedOrigins = ["https://materialize.com"];
    const search = "?redirectUrl=https%3A%2F%2Fmaterialize.com%2Fpricing%2F";

    it("keeps the redirectUrl and remembers it for social sign-in", () => {
      history.pushState(undefined, "", `/account/login${search}`);
      renderWithRouter(allowedOrigins);

      expect(screen.getByTestId("login-page")).toBeInTheDocument();
      expect(window.location.search).toBe(search);
      expect(localStorage.getItem("FRONTEGG_AFTER_AUTH_REDIRECT_URL")).toBe(
        "https://materialize.com/pricing/",
      );
    });

    it("strips it when the origin is not allowed", () => {
      history.pushState(undefined, "", `/account/login${search}`);
      renderWithRouter();

      expect(window.location.search).toBe("");
      expect(
        localStorage.getItem("FRONTEGG_AFTER_AUTH_REDIRECT_URL"),
      ).toBeNull();
    });

    it("strips look-alike origins", () => {
      history.pushState(
        undefined,
        "",
        "/account/login?redirectUrl=https%3A%2F%2Fmaterialize.com.evil.com%2F",
      );
      renderWithRouter(allowedOrigins);

      expect(window.location.search).toBe("");
      expect(
        localStorage.getItem("FRONTEGG_AFTER_AUTH_REDIRECT_URL"),
      ).toBeNull();
    });

    it("clears a remembered redirect when sign-in starts without one", () => {
      localStorage.setItem(
        "FRONTEGG_AFTER_AUTH_REDIRECT_URL",
        "https://materialize.com/pricing/",
      );
      history.pushState(undefined, "", "/account/login");
      renderWithRouter(allowedOrigins);

      expect(
        localStorage.getItem("FRONTEGG_AFTER_AUTH_REDIRECT_URL"),
      ).toBeNull();
    });

    it("clears a remembered redirect on the Console's own sign-in entry", () => {
      // Signed-out Console visitors are sent to /account/login?redirectUrl=<path>.
      for (const entrySearch of [
        "?redirectUrl=%2F",
        "?redirectUrl=%2Fregions",
      ]) {
        localStorage.setItem(
          "FRONTEGG_AFTER_AUTH_REDIRECT_URL",
          "https://materialize.com/pricing/",
        );
        history.pushState(undefined, "", `/account/login${entrySearch}`);
        const { unmount } = renderWithRouter(allowedOrigins);

        expect(window.location.search).toBe(entrySearch);
        expect(
          localStorage.getItem("FRONTEGG_AFTER_AUTH_REDIRECT_URL"),
          entrySearch,
        ).toBeNull();
        unmount();
      }
    });

    it("clears a remembered redirect when the new one is stripped", () => {
      localStorage.setItem(
        "FRONTEGG_AFTER_AUTH_REDIRECT_URL",
        "https://materialize.com/pricing/",
      );
      history.pushState(
        undefined,
        "",
        "/account/login?redirectUrl=https%3A%2F%2Fevil.com%2F",
      );
      renderWithRouter(allowedOrigins);

      expect(window.location.search).toBe("");
      expect(
        localStorage.getItem("FRONTEGG_AFTER_AUTH_REDIRECT_URL"),
      ).toBeNull();
    });

    it("keeps Frontegg's own internal redirects", () => {
      localStorage.setItem("FRONTEGG_AFTER_AUTH_REDIRECT_URL", "/regions");
      history.pushState(undefined, "", "/account/login");
      renderWithRouter(allowedOrigins);

      expect(localStorage.getItem("FRONTEGG_AFTER_AUTH_REDIRECT_URL")).toBe(
        "/regions",
      );
    });
  });
});
