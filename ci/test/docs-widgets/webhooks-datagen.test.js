// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { afterEach, beforeEach, describe, it, expect, vi } from "vitest";
import { faker } from "@faker-js/faker";
import { JSDOM } from "jsdom";
import fs from "fs";
import path from "path";

describe("Webhooks Data Generator", () => {
  let window, document;
  const uuid = /^[0-9a-f-]{36}$/;
  const fakerVersion = JSON.parse(
    fs.readFileSync(path.join(__dirname, "package.json"), "utf-8"),
  ).dependencies["@faker-js/faker"];

  beforeEach(() => {
    vi.useFakeTimers();
    const htmlPath = path.join(
      __dirname,
      "../../../doc/user/layouts/shortcodes/plugins/webhooks-datagen.html",
    );
    const dom = new JSDOM(fs.readFileSync(htmlPath, "utf-8"), {
      runScripts: "outside-only",
      url: "http://localhost",
    });
    window = dom.window;
    document = window.document;
    window.faker = faker;
    window.fetch = vi.fn().mockResolvedValue({ ok: true });
    window.alert = vi.fn();
    window.console.error = vi.fn();

    // JSDOM does not execute module scripts, so supply the real Faker instance
    // and evaluate the widget without its browser-only CDN import.
    const script = document.querySelector('script[type="module"]').textContent;
    const importStatement = script.match(
      /^\s*import \{ faker \} from 'https:\/\/esm\.sh\/@faker-js\/faker@([^']+)';/m,
    );
    expect(importStatement?.[1]).toBe(fakerVersion);
    window.eval(script.replace(importStatement[0], ""));
    document.getElementById("webhookURL").value = "http://localhost/webhook";
    document.getElementById("authPassword").value = "test-token";
  });

  afterEach(() => {
    window.close();
    vi.useRealTimers();
  });

  it.each(["", "sensorData", "userActivity", "ecommerceTransaction"])(
    "generates and sends valid data for preset %j",
    async (preset) => {
      if (preset) {
        const select = document.getElementById("useCaseSelect");
        select.value = preset;
        select.dispatchEvent(new window.Event("change"));
      }
      document.getElementById("startButton").click();
      expect(document.getElementById("startButton").style.display).toBe("none");
      expect(document.getElementById("stopButton").style.display).toBe(
        "inline-block",
      );
      await vi.advanceTimersByTimeAsync(1000);
      expect(window.fetch).toHaveBeenCalledExactlyOnceWith(
        "http://localhost/webhook",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-API-KEY": "test-token",
          },
          body: expect.any(String),
        },
      );
      const payload = JSON.parse(window.fetch.mock.calls[0][1].body);
      expect(window.console.error).not.toHaveBeenCalled();
      expect(JSON.stringify(payload)).not.toContain('"Error"');
      expect(Number.isNaN(Date.parse(payload.timestamp))).toBe(false);
      if (!preset || preset === "sensorData") {
        expect(payload).toEqual({
          sensor_id: expect.any(Number),
          timestamp: expect.any(String),
          location: {
            latitude: expect.any(Number),
            longitude: expect.any(Number),
          },
          temperature: expect.any(Number),
        });
        for (const [value, min, max] of [
          [payload.sensor_id, 1, 100],
          [payload.location.latitude, -90, 90],
          [payload.location.longitude, -180, 180],
          [payload.temperature, 20, 95],
        ]) {
          expect(value).toBeGreaterThanOrEqual(min);
          expect(value).toBeLessThanOrEqual(max);
        }
      } else {
        expect(payload.user_id).toMatch(uuid);
        if (preset === "userActivity") {
          expect(["page_view", "click", "scroll", "navigation"]).toContain(
            payload.event,
          );
          expect(payload.interaction.time_spent).toBeGreaterThanOrEqual(1);
          expect(payload.interaction.time_spent).toBeLessThanOrEqual(120);
        } else {
          expect([
            "add_to_cart",
            "remove_from_cart",
            "purchase",
            "view_product",
          ]).toContain(payload.event);
          expect(payload.product.id).toMatch(uuid);
          expect(payload.product.name).toEqual(expect.any(String));
          expect(Number.isNaN(Number(payload.product.price))).toBe(false);
        }
      }
      expect(document.getElementById("logOutput").textContent).toContain(
        "Sent:",
      );
    },
  );

  it("supports custom Faker expressions", async () => {
    document.getElementById("jsonSchema").value = JSON.stringify({
      id: "faker.string.uuid()",
      measurement: "faker.number.float({ min: 0, max: 1 })",
      status: "faker.helpers.arrayElement(['open', 'closed'])",
    });
    document.getElementById("startButton").click();
    await vi.advanceTimersByTimeAsync(1000);
    const payload = JSON.parse(window.fetch.mock.calls[0][1].body);
    expect(payload.id).toMatch(uuid);
    expect(payload.measurement).toBeGreaterThanOrEqual(0);
    expect(payload.measurement).toBeLessThanOrEqual(1);
    expect(["open", "closed"]).toContain(payload.status);
    expect(window.console.error).not.toHaveBeenCalled();
  });

  it.each(["webhookURL", "authPassword"])(
    "requires %s before starting",
    async (input) => {
      document.getElementById(input).value = "";
      document.getElementById("startButton").click();
      await vi.advanceTimersByTimeAsync(2000);
      expect(window.alert).toHaveBeenCalledOnce();
      expect(window.fetch).not.toHaveBeenCalled();
      expect(document.getElementById("startButton").style.display).not.toBe(
        "none",
      );
      expect(document.getElementById("stopButton").style.display).toBe("none");
    },
  );

  it("rejects invalid JSON and permits starting after it is corrected", async () => {
    const textarea = document.getElementById("jsonSchema");
    textarea.value = "invalid json";
    document.getElementById("startButton").click();
    await vi.advanceTimersByTimeAsync(1000);
    expect(window.fetch).not.toHaveBeenCalled();
    expect(document.getElementById("jsonError").style.display).toBe("block");
    textarea.value = '{"valid": "json"}';
    document.getElementById("startButton").click();
    await vi.advanceTimersByTimeAsync(1000);
    expect(window.fetch).toHaveBeenCalledOnce();
    expect(document.getElementById("jsonError").style.display).toBe("none");
  });

  it("stops sending events when stopped", async () => {
    document.getElementById("startButton").click();
    await vi.advanceTimersByTimeAsync(1000);
    expect(window.fetch).toHaveBeenCalledOnce();
    document.getElementById("stopButton").click();
    await vi.advanceTimersByTimeAsync(2000);
    expect(window.fetch).toHaveBeenCalledOnce();
    expect(document.getElementById("startButton").style.display).toBe(
      "inline-block",
    );
    expect(document.getElementById("stopButton").style.display).toBe("none");
  });
});
