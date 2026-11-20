---
headless: true
---
`USAGE` on a secret lets a role read the secret's value. Materialize does not
return secret contents in query results, but a role with `USAGE` can reference
the secret in a connection or source that sends it to an endpoint the role
controls. Grant `USAGE` on a secret only to roles you would trust with the
secret itself.
