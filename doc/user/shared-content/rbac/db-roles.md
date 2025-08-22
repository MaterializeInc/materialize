
In Materialize, a database role is created:
- Automatically when a user/service account is created:
  - When a [user account is
  created](/manage/users-service-accounts/invite-users/), an associated database
  role with the user email as its name is created.
  - When a [service account is
  created](/manage/users-service-accounts/create-service-accounts/), an
  associated database role with the service account user as its name is created.
- Manually to create a role independent of any specific account,
  usually to define a set of shared privileges that can be granted to other
  user/service/standalone roles.
