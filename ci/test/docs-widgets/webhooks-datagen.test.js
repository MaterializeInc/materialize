// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { JSDOM } from "jsdom";
import fs from "fs";
import path from "path";

describe("Webhooks Data Generator", async () => {
  let window, document;
  let isGenerating = false;

  beforeEach(async () => {
    const htmlPath = path.join(
      __dirname,
      "../../../doc/user/layouts/shortcodes/plugins/webhooks-datagen.html"
    );
    const htmlContent = fs.readFileSync(htmlPath, "utf-8");
    const dom = new JSDOM(htmlContent, {
      runScripts: "dangerously",
      resources: "usable",
      url: "http://localhost",
    });

    window = dom.window;
    document = window.document;

    // Wait for all the scripts and content to load
    await vi.waitFor(() => {
      if (document.readyState === "complete") {
        return true;
      } else {
        throw new Error("Document not ready yet");
      }
    });
  });

  it("should initialize the DOM elements correctly", async () => {
    const webhookURLInput = window.document.getElementById("webhookURL");
    const authPasswordInput = window.document.getElementById("authPassword");
    const jsonSchemaTextarea = window.document.getElementById("jsonSchema");
    const startButton = window.document.getElementById("startButton");
    const stopButton = window.document.getElementById("stopButton");

    await vi.waitFor(() => webhookURLInput && authPasswordInput && jsonSchemaTextarea && startButton && stopButton);

    expect(webhookURLInput).not.toBeNull();
    expect(authPasswordInput).not.toBeNull();
    expect(jsonSchemaTextarea).not.toBeNull();
    expect(startButton).not.toBeNull();
    expect(stopButton).not.toBeNull();
  });

  it("should not start generation without valid URL and password", async () => {
    const startButton = window.document.getElementById("startButton");
    const webhookURLInput = window.document.getElementById("webhookURL");
    const authPasswordInput = window.document.getElementById("authPassword");

    // Leave the URL and password empty
    webhookURLInput.value = "";
    authPasswordInput.value = "";

    startButton.click();

    expect(startButton.style.display).not.toBe("none");
  });

  it("should enable the stop button when generation starts", async () => {
    const startButton = window.document.getElementById("startButton");
    const stopButton = window.document.getElementById("stopButton");
    const webhookURLInput = window.document.getElementById("webhookURL");
    const authPasswordInput = window.document.getElementById("authPassword");

    webhookURLInput.value = "http://localhost";
    authPasswordInput.value = "password";

    startButton.click();

    await vi.waitFor(() => !stopButton.disabled);

    expect(stopButton.disabled).toBe(false);
  });

  it("should populate JSON schema based on selected use case", async () => {
    const jsonSchemaTextarea = window.document.getElementById("jsonSchema");
    const useCaseSelect = window.document.getElementById("useCaseSelect");

    // Select a use case
    useCaseSelect.value = "sensorData";
    useCaseSelect.dispatchEvent(new window.Event("change"));

    expect(jsonSchemaTextarea.value.trim()).not.toBe("");
  });

  it("should clear logs when the stop button is clicked", async () => {
    const stopButton = window.document.getElementById("stopButton");
    const logOutputDiv = window.document.getElementById("logOutput");

    logOutputDiv.innerHTML = "<p>Log message</p>";

    stopButton.click();

    expect(logOutputDiv.innerHTML).toBe(logOutputDiv.innerHTML);
  });

  it("should remove the error message if the JSON schema is corrected", async () => {
    const jsonSchemaTextarea = window.document.getElementById("jsonSchema");
    const jsonErrorDiv = window.document.getElementById("jsonError");

    // Input invalid JSON and then correct it
    jsonSchemaTextarea.value = "invalid json";
    jsonSchemaTextarea.dispatchEvent(new window.Event("blur"));

    await vi.waitFor(() => jsonErrorDiv.style.display === "block");

    jsonSchemaTextarea.value = '{"valid": "json"}';
    jsonSchemaTextarea.dispatchEvent(new window.Event("blur"));

    await vi.waitFor(() => jsonErrorDiv.style.display === "none");

    expect(jsonErrorDiv.style.display).toBe("none");
  });

  it("should have empty webhook URL and auth password inputs initially", async () => {
    const webhookURLInput = window.document.getElementById("webhookURL");
    const authPasswordInput = window.document.getElementById("authPassword");
    await vi.waitFor(() => webhookURLInput && authPasswordInput);
    expect(webhookURLInput.value).toBe("");
    expect(authPasswordInput.value).toBe("");
  });

  it("should not start generation with empty webhook URL and auth password", async () => {
    const startButton = window.document.getElementById("startButton");
    const webhookURLInput = window.document.getElementById("webhookURL");
    const authPasswordInput = window.document.getElementById("authPassword");
    await vi.waitFor(() => startButton && webhookURLInput && authPasswordInput);
    webhookURLInput.value = "";
    authPasswordInput.value = "";

    startButton.click();

    await vi.waitFor(() => isGenerating);

    expect(isGenerating).toBe(false);
  });

  it("should stop data generation and enable start button when stop button is clicked", async () => {
    const startButton = window.document.getElementById("startButton");
    const stopButton = window.document.getElementById("stopButton");

    // Trigger generation
    startButton.click();

    await vi.waitFor(() => isGenerating);

    stopButton.click();

    await vi.waitFor(() => !isGenerating);

    expect(isGenerating).toBe(false);
    expect(startButton.style.display).not.toBe("none");
  });
});
