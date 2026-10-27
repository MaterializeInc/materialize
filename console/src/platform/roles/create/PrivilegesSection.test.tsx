// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import React from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { renderComponent } from "~/test/utils";

import { PrivilegesSection } from "./PrivilegesSection";
import { useCreateRoleForm } from "./useCreateRoleForm";
import { ObjectOption } from "./useObjectsByType";

// Mock the useObjectsByType hook
vi.mock("./useObjectsByType", () => ({
  useObjectsByType: vi.fn(),
}));

import { useObjectsByType } from "./useObjectsByType";

const mockUseObjectsByType = vi.mocked(useObjectsByType);

// Test wrapper component that provides the form
const TestFormWrapper = () => {
  const form = useCreateRoleForm();
  return <PrivilegesSection form={form} />;
};

// Helper to open inline form and select object type + object
const selectObjectTypeAndObject = async (
  user: ReturnType<typeof userEvent.setup>,
  objectType: string,
  objectName: string,
) => {
  const addPrivButton = screen.getByRole("button", {
    name: "+ Add privilege",
  });
  await user.click(addPrivButton);

  const objectTypeInput = screen.getByLabelText("Select object type");
  await user.click(objectTypeInput);
  await waitFor(() => expect(screen.getByText(objectType)).toBeVisible());
  await user.click(screen.getByText(objectType));

  if (objectType !== "System") {
    await waitFor(() =>
      expect(screen.getByLabelText("Select object")).toBeVisible(),
    );
    const objectInput = screen.getByLabelText("Select object");
    await user.click(objectInput);
    await waitFor(() => expect(screen.getByText(objectName)).toBeVisible());
    await user.click(screen.getByText(objectName));
  }
};

describe("PrivilegesSection", () => {
  beforeEach(() => {
    // Reset mock before each test
    mockUseObjectsByType.mockReset();
    // Default implementation returns empty objects and not loading
    mockUseObjectsByType.mockReturnValue({
      objects: [],
      isLoading: false,
    });
  });

  describe("Initial state", () => {
    it("shows title, description, add button, and no inline form", async () => {
      await renderComponent(<TestFormWrapper />);

      expect(screen.getByText("Direct privileges")).toBeVisible();
      expect(
        screen.getByText(
          /Add privileges that apply specifically to this role only/,
        ),
      ).toBeVisible();
      expect(
        screen.getByRole("button", { name: "+ Add privilege" }),
      ).toBeVisible();
      expect(
        screen.queryByLabelText("Select object type"),
      ).not.toBeInTheDocument();
    });
  });

  describe("Inline form interaction", () => {
    it("closes inline form on cancel", async () => {
      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addButton = screen.getByRole("button", { name: "+ Add privilege" });
      await user.click(addButton);
      expect(screen.getByLabelText("Select object type")).toBeVisible();

      const cancelButton = screen.getByRole("button", { name: "Cancel" });
      await user.click(cancelButton);

      await waitFor(() => {
        expect(
          screen.queryByLabelText("Select object type"),
        ).not.toBeInTheDocument();
      });
      expect(
        screen.getByRole("button", { name: "+ Add privilege" }),
      ).toBeVisible();
    });
  });

  describe("Object type dropdown", () => {
    it("shows all object types in dropdown", async () => {
      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addButton = screen.getByRole("button", { name: "+ Add privilege" });
      await user.click(addButton);

      const objectTypeInput = screen.getByLabelText("Select object type");
      await user.click(objectTypeInput);

      // Wait for options to appear
      await waitFor(() => {
        expect(screen.getByText("Cluster")).toBeVisible();
      });

      // Check that all expected object types are present
      const expectedTypes = [
        "Cluster",
        "Database",
        "Schema",
        "Table",
        "View",
        "Materialized View",
        "Type",
        "Source",
        "Connection",
        "Secret",
        "System",
      ];

      for (const typeName of expectedTypes) {
        expect(screen.getByText(typeName)).toBeVisible();
      }
    });
  });

  describe("System object type behavior", () => {
    it("hides object dropdown when System type is selected", async () => {
      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addButton = screen.getByRole("button", { name: "+ Add privilege" });
      await user.click(addButton);

      const objectTypeInput = screen.getByLabelText("Select object type");
      await user.click(objectTypeInput);

      await waitFor(() => {
        expect(screen.getByText("System")).toBeVisible();
      });

      const systemOption = screen.getByText("System");
      await user.click(systemOption);

      // Object dropdown should not be visible for system type
      expect(screen.queryByLabelText("Select object")).not.toBeInTheDocument();
    });

    it("automatically sets object to SYSTEM for system type", async () => {
      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addButton = screen.getByRole("button", { name: "+ Add privilege" });
      await user.click(addButton);

      const objectTypeInput = screen.getByLabelText("Select object type");
      await user.click(objectTypeInput);

      await waitFor(() => {
        expect(screen.getByText("System")).toBeVisible();
      });

      const systemOption = screen.getByText("System");
      await user.click(systemOption);

      // Privileges section should appear (meaning object is set)
      await waitFor(() => {
        expect(screen.getByText("Privileges")).toBeVisible();
      });
    });
  });

  describe("Object dropdown for non-system types", () => {
    it("shows object dropdown with loading, empty, and populated states", async () => {
      const mockClusters: ObjectOption[] = [
        { id: "u1", name: "default" },
        { id: "u2", name: "analytics" },
      ];

      mockUseObjectsByType.mockReturnValue({
        objects: mockClusters,
        isLoading: false,
      });

      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addButton = screen.getByRole("button", { name: "+ Add privilege" });
      await user.click(addButton);

      const objectTypeInput = screen.getByLabelText("Select object type");
      await user.click(objectTypeInput);

      await waitFor(() => {
        expect(screen.getByText("Cluster")).toBeVisible();
      });

      const clusterOption = screen.getByText("Cluster");
      await user.click(clusterOption);

      // Object dropdown should appear with objects
      await waitFor(() => {
        expect(screen.getByLabelText("Select object")).toBeVisible();
      });

      const objectInput = screen.getByLabelText("Select object");
      await user.click(objectInput);

      await waitFor(() => {
        expect(screen.getByText("default")).toBeVisible();
      });
      expect(screen.getByText("analytics")).toBeVisible();
    });

    it("shows loading state for objects", async () => {
      mockUseObjectsByType.mockReturnValue({
        objects: [],
        isLoading: true,
      });

      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addButton = screen.getByRole("button", { name: "+ Add privilege" });
      await user.click(addButton);

      const objectTypeInput = screen.getByLabelText("Select object type");
      await user.click(objectTypeInput);

      await waitFor(() => {
        expect(screen.getByText("Cluster")).toBeVisible();
      });

      const clusterOption = screen.getByText("Cluster");
      await user.click(clusterOption);

      await waitFor(() => {
        expect(screen.getByText("Loading...")).toBeVisible();
      });
    });

    it("shows empty state when no objects available", async () => {
      mockUseObjectsByType.mockReturnValue({
        objects: [],
        isLoading: false,
      });

      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addButton = screen.getByRole("button", { name: "+ Add privilege" });
      await user.click(addButton);

      const objectTypeInput = screen.getByLabelText("Select object type");
      await user.click(objectTypeInput);

      await waitFor(() => {
        expect(screen.getByText("Cluster")).toBeVisible();
      });

      const clusterOption = screen.getByText("Cluster");
      await user.click(clusterOption);

      await waitFor(() => {
        expect(screen.getByText("No objects available")).toBeVisible();
      });
    });
  });

  describe("Privilege checkboxes based on object type", () => {
    it("shows correct privileges for Cluster type", async () => {
      const mockClusters: ObjectOption[] = [{ id: "u1", name: "default" }];
      mockUseObjectsByType.mockReturnValue({
        objects: mockClusters,
        isLoading: false,
      });

      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addButton = screen.getByRole("button", { name: "+ Add privilege" });
      await user.click(addButton);

      const objectTypeInput = screen.getByLabelText("Select object type");
      await user.click(objectTypeInput);
      await waitFor(() => expect(screen.getByText("Cluster")).toBeVisible());
      await user.click(screen.getByText("Cluster"));

      await waitFor(() =>
        expect(screen.getByLabelText("Select object")).toBeVisible(),
      );
      const objectInput = screen.getByLabelText("Select object");
      await user.click(objectInput);
      await waitFor(() => expect(screen.getByText("default")).toBeVisible());
      await user.click(screen.getByText("default"));

      await waitFor(() => {
        expect(screen.getByText("USAGE")).toBeVisible();
      });
      expect(screen.getByText("CREATE")).toBeVisible();
      expect(screen.queryByText("SELECT")).not.toBeInTheDocument();
    });

    it("shows correct privileges for Table type", async () => {
      const mockTables: ObjectOption[] = [
        { id: "u1", name: "public.users.customers" },
      ];
      mockUseObjectsByType.mockReturnValue({
        objects: mockTables,
        isLoading: false,
      });

      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addButton = screen.getByRole("button", { name: "+ Add privilege" });
      await user.click(addButton);

      const objectTypeInput = screen.getByLabelText("Select object type");
      await user.click(objectTypeInput);
      await waitFor(() => expect(screen.getByText("Table")).toBeVisible());
      await user.click(screen.getByText("Table"));

      await waitFor(() =>
        expect(screen.getByLabelText("Select object")).toBeVisible(),
      );
      const objectInput = screen.getByLabelText("Select object");
      await user.click(objectInput);
      await waitFor(() =>
        expect(screen.getByText("public.users.customers")).toBeVisible(),
      );
      await user.click(screen.getByText("public.users.customers"));

      await waitFor(() => {
        expect(screen.getByText("SELECT")).toBeVisible();
      });
      expect(screen.getByText("INSERT")).toBeVisible();
      expect(screen.getByText("UPDATE")).toBeVisible();
      expect(screen.getByText("DELETE")).toBeVisible();
      expect(screen.queryByText("USAGE")).not.toBeInTheDocument();
    });

    it("shows correct privileges for System type", async () => {
      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addButton = screen.getByRole("button", { name: "+ Add privilege" });
      await user.click(addButton);

      const objectTypeInput = screen.getByLabelText("Select object type");
      await user.click(objectTypeInput);
      await waitFor(() => expect(screen.getByText("System")).toBeVisible());
      await user.click(screen.getByText("System"));

      await waitFor(() => {
        expect(screen.getByText("CREATEROLE")).toBeVisible();
      });
      expect(screen.getByText("CREATEDB")).toBeVisible();
      expect(screen.getByText("CREATECLUSTER")).toBeVisible();
      expect(screen.getByText("CREATENETWORKPOLICY")).toBeVisible();
      expect(screen.queryByText("USAGE")).not.toBeInTheDocument();
    });
  });

  describe("Add button validation", () => {
    it("Add button enabled only when object type, object, and privileges selected", async () => {
      const mockClusters: ObjectOption[] = [{ id: "u1", name: "default" }];
      mockUseObjectsByType.mockReturnValue({
        objects: mockClusters,
        isLoading: false,
      });

      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      const addPrivButton = screen.getByRole("button", {
        name: "+ Add privilege",
      });
      await user.click(addPrivButton);

      // Initially disabled
      const addButton = screen.getByRole("button", { name: "Add privilege" });
      expect(addButton).toBeDisabled();

      // Still disabled after selecting object type
      const objectTypeInput = screen.getByLabelText("Select object type");
      await user.click(objectTypeInput);
      await waitFor(() => expect(screen.getByText("Cluster")).toBeVisible());
      await user.click(screen.getByText("Cluster"));
      await waitFor(() => expect(addButton).toBeDisabled());

      // Still disabled after selecting object
      await waitFor(() =>
        expect(screen.getByLabelText("Select object")).toBeVisible(),
      );
      const objectInput = screen.getByLabelText("Select object");
      await user.click(objectInput);
      await waitFor(() => expect(screen.getByText("default")).toBeVisible());
      await user.click(screen.getByText("default"));
      await waitFor(() => expect(addButton).toBeDisabled());

      // Enabled after selecting privilege
      await waitFor(() => expect(screen.getByText("USAGE")).toBeVisible());
      const usageCheckbox = screen.getByRole("checkbox", { name: "USAGE" });
      await user.click(usageCheckbox);
      await waitFor(() => expect(addButton).toBeEnabled());
    });
  });

  describe("Adding privileges", () => {
    it("adds privilege card, shows multiple privileges, closes form", async () => {
      const mockClusters: ObjectOption[] = [{ id: "u1", name: "default" }];
      mockUseObjectsByType.mockReturnValue({
        objects: mockClusters,
        isLoading: false,
      });

      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      await selectObjectTypeAndObject(user, "Cluster", "default");

      await waitFor(() => expect(screen.getByText("USAGE")).toBeVisible());
      const usageCheckbox = screen.getByRole("checkbox", { name: "USAGE" });
      await user.click(usageCheckbox);

      const createCheckbox = screen.getByRole("checkbox", { name: "CREATE" });
      await user.click(createCheckbox);

      const addButton = screen.getByRole("button", { name: "Add privilege" });
      await user.click(addButton);

      // Verify privilege card with correct details
      await waitFor(() => {
        const labels = screen.getAllByText("Object type");
        expect(labels.length).toBeGreaterThan(0);
        expect(screen.getByText("Cluster")).toBeVisible();
      });
      const objectLabels = screen.getAllByText("Object");
      expect(objectLabels.length).toBeGreaterThan(0);
      expect(screen.getByText("default")).toBeVisible();

      // Verify multiple privileges shown in list
      expect(screen.getByText(/^USAGE$/)).toBeVisible();
      expect(screen.getByText(/^CREATE$/)).toBeVisible();

      // Verify inline form closed and reset
      expect(
        screen.queryByLabelText("Select object type"),
      ).not.toBeInTheDocument();
      expect(
        screen.getByRole("button", { name: "+ Add privilege" }),
      ).toBeVisible();
    });
  });

  describe("Removing privileges", () => {
    it("removes privilege on X click", async () => {
      const mockClusters: ObjectOption[] = [{ id: "u1", name: "default" }];
      mockUseObjectsByType.mockReturnValue({
        objects: mockClusters,
        isLoading: false,
      });

      const user = userEvent.setup();
      await renderComponent(<TestFormWrapper />);

      await selectObjectTypeAndObject(user, "Cluster", "default");

      await waitFor(() => expect(screen.getByText("USAGE")).toBeVisible());
      const usageCheckbox = screen.getByRole("checkbox", { name: "USAGE" });
      await user.click(usageCheckbox);

      const addButton = screen.getByRole("button", { name: "Add privilege" });
      await user.click(addButton);

      await waitFor(() => {
        expect(screen.getByText(/^USAGE$/)).toBeVisible();
      });

      // Remove privilege
      const removeButton = screen.getByRole("button", {
        name: "Remove privilege",
      });
      await user.click(removeButton);

      await waitFor(() => {
        expect(screen.queryByText(/^USAGE$/)).not.toBeInTheDocument();
      });
    });
  });
});
