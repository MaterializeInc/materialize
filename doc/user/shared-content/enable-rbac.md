By default, role-based access control (RBAC) checks are not enabled (i.e.,
enforced) when turning on password authentication. To enable RBAC, set the
system parameter `enable_rbac_checks` to `'on'` or `True`. You can enable the
parameter in one of the following ways:

- For [local installations using Kind/Minikube](/installation/#install-locally),
  set `spec.enableRbac: true` option when instantiating the Materialize object.

- For [Cloud deployments using Materialize's
  Terraforms](/installation/#install-on-cloud-provider), set
  `enable_rbac_checks` in the environment CR via the `environmentdExtraArgs`
  flag option.

- After the Materialize instance is running, run the following command as
  `mz_system` user:

  ```mzsql
  ALTER SYSTEM SET enable_rbac_checks = 'on';
  ```

If more than one method is used, the `ALTER SYSTEM` command will take precedence
over the Kubernetes configuration.

You may view the current value for `enable_rbac_checks`  by running:

```mzsql
SHOW enable_rbac_checks;
```

If RBAC is not enabled, all users will behave as if they were _superuser_.
