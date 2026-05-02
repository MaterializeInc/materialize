---
headless: true
---
{{< yaml-table data="rbac/organization_roles" >}}

{{< note >}}
- The first user for an organization is automatically assigned the
  **Organization Admin** role.

- {{% include-headless "/headless/rbac-sm/org-admin-recommendation" %}}

- Users/service accounts can be granted additional database roles and privileges
  as needed.

{{</note>}}
