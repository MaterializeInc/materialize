---
headless: true
---
The privileges required by the role that authenticates the scrape request are:

- `USAGE` privileges on the API. A role that lacks `USAGE` on the API
  receives `404 Not Found`; the response does not distinguish "API exists
  but you cannot see it" from "no such API".
- `USAGE` privileges on the database and schema containing the API, and on
  the database and schema containing each metric's `VALUES FROM` relation.
- `USAGE` privileges on the cluster the API runs on.
- `SELECT` privileges on every relation named in `VALUES FROM` across the
  API's metrics. A scrape that hits a metric whose relation the role cannot
  read fails with `403 Forbidden`; other metrics on the same API are not
  exposed for that request.
