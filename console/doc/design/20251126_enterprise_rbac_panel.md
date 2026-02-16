# RBAC Enterprise panel design doc

Important links:

- Figma Link: https://www.figma.com/design/a0dzpJUVB1KaylMTYNTIWH/Enterprise-Admin-Panel?node-id=107-11269&t=eHxTff0Y7koJvcgx-0

- Product Brief: https://www.notion.so/materialize/Enterprise-Admin-Panel-for-roles-access-controls-29613f48d37b80bcb0c0d5ef0c29dee1

- Github epic: https://github.com/MaterializeInc/database-issues/issues/9904

This design doc focuses on the data implementation and considerations for the console for the user to view roles and create roles.

## Problem

Our current RBAC management reliance on SQL is confusing, clunky and an error prone experience. The console needs an intuitive experience for the user to manage their roles and privileges in a clear and efficient view.

## Success Criteria

RBAC Panel is only shown for a super user to be able to:

- View roles in a table and graph view
- See role details and privileges on the role. This includes inherited privileges on a role in the privileges view.
- Create a role, grant privileges to that role and add inherited privileges from another role
- Perform CRUD operations of a role for a user

### Out of Scope

- Timestamps and activity for the role creation and activity

## View roles

Roles view should show the roles that this user has created and how many users have this role.

**Search & Filter:**

- **Search by Name**: As an admin, the user should be able to filter by a role name.
- (No complex filters are planned for the Roles page initially).

### `useRoles` hook

We will subscribe to this query to make sure we can keep showing the latest changes to the roles table.

```sql
SELECT
  r.name AS role_name,
  COUNT(rm.member) AS member_count
FROM mz_roles r
LEFT JOIN mz_role_members rm ON r.id = rm.role_id
WHERE r.name NOT LIKE 'mz_%'
GROUP BY r.id, r.name
ORDER BY role_name;
```

## `useRoleDetails`

Role details view needs to show role metadata with role name, user count and inherited roles.

**Out of scope:** Role description, type, Created at, last modified since this data doesn’t exist in the Materialize tables yet.

```sql
SELECT
  r.name,
  mc.members,
  pr.inherited_roles
FROM mz_roles r
LEFT JOIN LATERAL (
  SELECT list_agg(member_role.name) AS members
  FROM mz_role_members m
  JOIN mz_roles member_role ON member_role.id = m.member
  WHERE m.role_id = r.id
) mc ON true
LEFT JOIN LATERAL (
  SELECT list_agg(parent.name ORDER BY parent.name) AS inherited_roles
  FROM mz_role_members rm
  JOIN mz_roles parent ON parent.id = rm.role_id
  WHERE rm.member = r.id
) pr ON true
WHERE r.name = 'engineer';

```

We will use client-side formatting for:

- **Member count**: `members?.length ?? 0` instead of `COUNT(*)` in SQL
- **Inherited roles display**: Format the array as needed (comma-separated, chips, list, etc.)
- **Empty state handling**: Check for null/empty arrays instead of `COALESCE(..., 'None')` in SQL

To ensure that the roles will always be up to date, we will use `useGlobalUpsertSubscribe` and hoist this globally.

## `useRolePrivileges`

In the role details, we need to show a table of privileges that the role has, including both direct privileges and those inherited from parent roles.

We can use the native `SHOW PRIVILEGES` command, which recursively lists all privileges held by a role (including inherited ones) and identifies the `grantee` (the specific role that holds the privilege).

```sql
SHOW PRIVILEGES FOR '<role_name>';
```

**Client-side mapping:**

The `SHOW` command returns rows that we can map directly to the UI columns. We use the **React Query + Kysely** pattern for caching and testability.

| UI Column          | Source Column    | Logic                                                                                                       |
| :----------------- | :--------------- | :---------------------------------------------------------------------------------------------------------- |
| **Object**         | `name`           | Display raw name (or qualify with `schema`/`database` if needed)                                            |
| **Type**           | `object_type`    | Capitalize (e.g., `table` → `Table`)                                                                        |
| **Privileges**     | `privilege_type` | Display raw value (e.g., `SELECT`, `USAGE`)                                                                 |
| **Inherited From** | `grantee`        | If `grantee === currentRole`, display **"Direct"**. Otherwise, display the `grantee` name (e.g., `PUBLIC`). |

**Query builder (`src/api/materialize/roles/rolePrivileges.ts`):**

```tsx
import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { queryBuilder } from "~/api/materialize/db";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";

export type RolePrivilegesParameters = {
  roleName: string;
};

export function buildRolePrivilegesQuery(params: RolePrivilegesParameters) {
  return sql<{
    grantor: string;
    grantee: string;
    database: string;
    schema: string;
    name: string;
    object_type: string;
    privilege_type: string;
  }>`SHOW PRIVILEGES FOR ${sql.id(params.roleName)}`.compile(queryBuilder);
}

export function fetchRolePrivileges({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: RolePrivilegesParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildRolePrivilegesQuery(parameters);
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}
```

**Hook (`src/platform/roles/queries.ts`):**

```tsx
export function useRolePrivileges(params: RolePrivilegesParameters) {
  return useQuery({
    queryKey: roleQueryKeys.privileges(params),
    queryFn: ({ queryKey, signal }) => {
      const [, parameters] = queryKey;
      return fetchRolePrivileges({
        parameters,
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}
```

**Component data transformation (`src/platform/roles/PrivilegesList.tsx`):**

```tsx
const { data, isLoading, isError } = useRolePrivileges({ roleName });

// Filter out PUBLIC privileges and transform for UI display
const privileges = (data?.rows ?? [])
  .filter((row) => row.grantee !== "PUBLIC")
  .map((row) => ({
    object: row.name,
    type: row.object_type.charAt(0).toUpperCase() + row.object_type.slice(1),
    privilege: row.privilege_type,
    inheritedFrom: row.grantee === roleName ? "Direct" : row.grantee,
  }));
```

## `useRoleUsers`

Role details tab has a users tab that should display the table of users being assigned to this role.

```sql
SELECT
    member.id,
    member.name AS member_name
FROM mz_catalog.mz_role_members rm
JOIN mz_catalog.mz_roles member ON rm.member = member.id
WHERE rm.role_id = (SELECT id FROM mz_catalog.mz_roles WHERE name = 'analyst')
ORDER BY member_name;
```

User should be able to remove a user from the role so clicking on the row should prompt if the user should be removed from the role.

```sql
REVOKE <role_name> FROM "<user_email>";
```

## Creating a new role

To let the user create a role, user needs to configure the privileges that need to be granted to a role.

## Example Form Flow

When a user configures privileges in your UI:

1. **User selects Object Type**: `"Cluster"`
   - Show privileges: `['USAGE', 'CREATE']` (from `PRIVILEGE_TO_OBJECT_MAPPING`)
2. **User selects Object**: `"dev_cluster"` (dropdown populated from clusters query)
   - Store: `{ objectType: 'cluster', objectId: 'u123', privileges: [] }`
3. **User checks privileges**: ☑ `USAGE`
   - Update: `{ objectType: 'cluster', objectId: 'u123', privileges: ['USAGE'] }`
4. **User clicks "Add privilege"**
   - Add to form state array
5. **User clicks "Create"**

   - Generate SQL:

   ```sql
   CREATE ROLE analyst;
   GRANT USAGE ON CLUSTER dev_cluster TO analyst;
   ```

### Privilege-to-Object Type Mapping

For a user to add “Direct” privileges, we need to show what “Privilege Type” is available for “Object Type”. This mapping can be defined in TypeScript following Materialize descriptions:

```tsx
export type PrivilegeType =
  | "SELECT"
  | "INSERT"
  | "UPDATE"
  | "DELETE"
  | "USAGE"
  | "CREATE"
  | "CREATEROLE"
  | "CREATEDB"
  | "CREATECLUSTER"
  | "CREATENETWORKPOLICY";

// To get all object types that can have privileges:
// SELECT DISTINCT object_type FROM mz_internal.mz_show_all_privileges;
export type ObjectTypeWithPrivileges =
  | "table"
  | "view"
  | "materialized-view"
  | "source"
  | "cluster"
  | "database"
  | "schema"
  | "connection"
  | "secret"
  | "type"
  | "system";

export const PRIVILEGE_TO_OBJECT_MAPPING: Record<ObjectType, PrivilegeType[]> =
  {
    table: ["SELECT", "INSERT", "UPDATE", "DELETE"],
    view: ["SELECT"],
    "materialized-view": ["SELECT"],
    source: ["SELECT"],
    sink: [],
    cluster: ["USAGE", "CREATE"],
    database: ["USAGE", "CREATE"],
    schema: ["USAGE", "CREATE"],
    connection: ["USAGE"],
    secret: ["USAGE"],
    type: ["USAGE"],
    system: ["CREATEROLE", "CREATEDB", "CREATECLUSTER", "CREATENETWORKPOLICY"],
  };

export const OBJECT_TYPE_LABELS: Record<ObjectType, string> = {
  table: "Table",
  view: "View",
  "materialized-view": "Materialized view",
  source: "Source",
  sink: "Sink",
  cluster: "Cluster",
  database: "Database",
  schema: "Schema",
  connection: "Connection",
  secret: "Secret",
  type: "Type",
  system: "System",
};
```

### Queries for the form to show the objects in the organization

Showing the objects like sources, databases, table, cluster would depend on what type of object a user is selecting for those privileges to be granted to that role. If a user selects `cluster` as **Object Type** so the form needs to show the list of `clusters` that exists in the user’s system.

Instead of ad-hoc SQL queries, we should reuse existing global subscription hooks to populate the object lists in the form. These hooks ensure the UI stays in sync with the catalog state efficiently.

- **`useAllClusters`**: Returns all clusters. Use this to populate the "Cluster" dropdown.
- **`useAllNamespaces`** (or **`useAllSchemas`**): Returns schemas and their associated databases. Use this to derive the list of **Databases** and **Schemas**.
- **`useAllObjects`**: Returns a comprehensive list of database objects including tables, views, materialized views, sources, sinks, connections, and secrets. This hook includes `schemaId` and `databaseId` for each object, allowing us to filter objects by the selected schema on the client side.

**Example client-side filtering:**

```tsx
// Example: Get all tables in a specific schema
const { data: allObjects } = useAllObjects();
const tablesInSchema = allObjects.filter(
  (obj) => obj.objectType === "table" && obj.schemaId === selectedSchemaId,
);
```

### SQL Generation for the role creation

We need to show SQL statement generation for creating a role and granting a privilege to that role. This can be done in TypeScript using a helper function that iterates over privileges and object names to create a SQL statement like `GRANT SELECT ON TABLE "my_table" TO analyst`.

```tsx
export type DirectPrivilege = {
  objectType: ObjectType;
  objectId?: string;
  privileges: PrivilegeType[];
};

export type CreateRoleFormData = {
  name: string;
  inheritedRoles: string[];
  directPrivileges: DirectPrivilege[];
};

export function buildCreateRoleStatements(data: CreateRoleFormData): string[] {
  const statements: string[] = [];

  // 1. CREATE ROLE statement
  statements.push(`CREATE ROLE ${data.name};`);

  // 2. GRANT ROLE statements (for inherited roles)
  for (const inheritedRole of data.inheritedRoles) {
    statements.push(`GRANT ${inheritedRole} TO ${data.name};`);
  }

  // 3. GRANT PRIVILEGE statements (for direct privileges)
  for (const privilege of data.directPrivileges) {
    const privilegeList = privilege.privileges.join(", ");

    if (privilege.objectType === "system") {
      // System-level privileges
      statements.push(`GRANT ${privilegeList} ON SYSTEM TO ${data.name};`);
    } else if (privilege.objectId) {
      // Object-specific privileges
      const objectTypeKeyword = privilege.objectType
        .toUpperCase()
        .replace("-", " ");
      const objectName = getObjectName(privilege.objectId);

      statements.push(
        `GRANT ${privilegeList} ON ${objectTypeKeyword} ${objectName} TO ${data.name};`,
      );
    }
  }

  return statements;
}
```

## Inherited roles

A user can select existing roles whose privileges can be inherited in this role. When a user selects the “Inherited Role”, dropdown should show existing roles (excluding system roles) and show the privileges for those roles.

### Form patterns

We will use **React Hook Form** (`useForm`) for state management and validation.

### Form submission

We will use **`useMutation`** from `@tanstack/react-query` combined with **`executeSqlV2`** to execute the SQL commands, following the existing codebase pattern for mutations.

## Editing roles with users

In the users section, RBAC panel should show all the users in the organization and assign roles to the users.

**Search & Filter:**

- **Filter by Role**: As an admin. the user needs to see what specific users are assigned to a role.

### `useUsers`

```sql
SELECT name
FROM mz_roles
WHERE rolcanlogin = 'true';
```

### Assign roles to the users

```sql
GRANT "<role>" TO "<user email>";
```

### Showing what roles are assigned to users

```sql
SELECT
    r.name AS member_name,
    assigned_role.name AS role_granted
FROM mz_catalog.mz_role_members rm
JOIN mz_catalog.mz_roles r ON rm.member = r.id
JOIN mz_catalog.mz_roles assigned_role ON rm.role_id = assigned_role.id
WHERE r.name = "<user email>";
```

### Revoking a role from the user

```sql
REVOKE "<role>" FROM "<user email>";
```

Since we are showing roles across the form and operations to edit and add roles, we will use the Jotai atom to store existing roles and fetch them once upon load.

## Graph view of the roles in RBAC panel

A user should be able to toggle between a Table View and a graph view to see the roles and their hierarchy.

We can reuse the existing graph visualization components from the **Workflow Graph** implementation (`src/components/WorkflowGraph/`) to ensure consistency and reduce effort. Specifically, we can leverage:

- **`dagre`**: For calculating the graph layout (nodes and edges positioning).
- **`Canvas`**: For the pan/zoom container.
- **`GraphNode`**: For rendering individual role nodes.
- **`GraphEdge`** and **`GraphEdgeContainer`**: For rendering the connections (inheritance relationships) between roles.

Data will be mapped as follows:

- **Nodes**: List of roles from `mz_roles`.
- **Edges**: Inheritance relationships derived from `mz_role_members`.

## Error Handling

The console would need to show errors gracefully while creating roles.

- **Duplication errors:** Prevent the user from creating the same role if it exists
- **Invalid permissions:** If the user doesn’t have permission to create a role or grant privileges to a role
- **Circular inheritance:** Materialize already prevents circular inheritance. If an `analyst` → `engineer` then the UI needs to prevent `engineer` → `analyst`. Database errors in the UI should prevent the user from creating the role
- **Partial Failures:** Since DDL is non-transactional, we will **split execution** into two steps (`CREATE ROLE` then `GRANT`). This ensures that if the grants fail, we can gracefully inform the user that the role was created but requires further configuration.

## Validation

- We will need to verify if the user has `CREATEROLE` privileges so we can show the `Create Role` button
- For deleting roles and privileges, we will use the `DeleteObjectModal` pattern in the console

### Open questions

- If the organization has hundreds of roles and the graph view becomes cluttered then we would need to consider some collapsing and filter to only show “role X and its neighbors”. For now this seems like it might not be necessary but might be a consideration in the future.

## Terraform Code Generation

As shown in the Figma designs, we aim to provide Terraform code snippets for creating roles and grants. To ensure accuracy and maintainability:

- **Automated Generation**: We should automate the code generation to keep it aligned with our [Terraform provider](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs).
- **Version Pinning**: We will fix the generated code to the specific versoin of Materialize Terraform provider and explicitly display this version in the UI.

Example snippet structure:

```hcl
resource "materialize_role" "analyst" {
  name = "analyst"
}

resource "materialize_grant_privilege" "analyst_cluster_usage" {
  grantee_name   = materialize_role.analyst.name
  privilege      = "USAGE"
  object_type    = "CLUSTER"
  object_name    = "dev_cluster"
}
```

---
