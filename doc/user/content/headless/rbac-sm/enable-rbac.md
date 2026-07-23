---
headless: true
---
{{< warning >}}
If RBAC is not enabled, all users have <red>**superuser**</red> privileges.
{{</ warning >}}

Whether role-based access control (RBAC) checks are enabled (i.e., enforced)
is controlled by the `enableRbac` field on the Materialize resource and the
`enable_rbac_checks` system parameter:

- For Materialize resources using the `v1` CRD, RBAC is **enabled by
  default**. To disable RBAC, set `spec.enableRbac: false` when instantiating
  the Materialize object.

- For Materialize resources using the `v1alpha1` CRD, RBAC is **not enabled
  by default**. To enable RBAC, set `spec.enableRbac: true` when
  instantiating the Materialize object.

- After the Materialize instance is running, you can enable RBAC by running
  the following command as the `mz_system` user:

  ```mzsql
  ALTER SYSTEM SET enable_rbac_checks = 'on';
  ```

If more than one method is used, the `ALTER SYSTEM` command will take precedence
over the Kubernetes configuration.

To view the current value for `enable_rbac_checks`, run the following `SHOW`
command:

```mzsql
SHOW enable_rbac_checks;
```

{{< important >}}
If RBAC is not enabled, all users have <red>**superuser**</red> privileges.
{{</ important >}}
