{{< warning >}}
If RBAC is not enabled, all users have <red>**superuser**</red> privileges.
{{</ warning >}}

By default, role-based access control (RBAC) checks are not enabled (i.e.,
enforced) when turning on [password
authentication](/manage/authentication/#configuring-password-authentication). To
enable RBAC, set the system parameter `enable_rbac_checks` to `'on'` or `True`.
You can enable the parameter in one of the following ways:

- For [local installations using
  Kind/Minikube](/installation/#installation-guides), set `spec.enableRbac:
  true` option when instantiating the Materialize object.

- For [Cloud deployments using Materialize's
  Terraforms](/installation/#installation-guides), set `enable_rbac_checks` in
  the environment CR via the `environmentdExtraArgs` flag option.

- After the Materialize instance is running, run the following command as
  `mz_system` user:

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
