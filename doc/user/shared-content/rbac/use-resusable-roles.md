When possible, avoid granting privileges directly to individual user or service
account roles. Instead, create reusable, functional roles (e.g., `data_reader`,
`view_manager`) with well-defined privileges, and grant these roles to the
individual user or service account roles. You can also grant functional roles to
other functional roles to compose more complex functional roles.
