---
headless: true
---
- Ownership of the cluster.

- To attach or detach a [query policy](/clusters/query-policies/), you need
  `CREATE` privileges on the cluster instead of ownership, if `QUERY POLICY`
  is the only option you change. To attach a policy, you also need `USAGE`
  privileges on the policy.

- To rename a cluster, you must also have membership in the `<new_owner_role>`.

- To swap names with another cluster, you must also have ownership of the other
  cluster.
