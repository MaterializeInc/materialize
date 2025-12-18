To create additional users or service accounts, login as the `mz_system` user,
using the `external_login_password_mz_system` password, and use [`CREATE ROLE
... WITH LOGIN PASSWORD ...`](/sql/create-role):

```mzsql
CREATE ROLE <user> WITH LOGIN PASSWORD '<password>';
```
