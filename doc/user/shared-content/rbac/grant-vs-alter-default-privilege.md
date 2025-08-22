- Use `GRANT|REVOKE ...` to modify privileges on **existing** objects.

- Use `ALTER DEFAULT PRIVILEGES` to ensure that privileges are automatically
  granted or revoked when **new objects** of a certain type are created by
  others. Then, as needed, you can use `GRANT|REVOKE <privilege>` to adjust
  those privileges.
