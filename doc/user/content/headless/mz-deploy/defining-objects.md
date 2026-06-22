---
headless: true
---
- Each file contains one primary `CREATE` statement, and for `model/` object
  files, optional companion statements like `CREATE INDEX`, `COMMENT ON`, and
  `GRANT`.

- The object name in your `CREATE` statement must match the `.sql` file name.

- You do not create the database and schema explicitly; `mz-deploy` derives them
  from the file path and automatically creates them during deployment. See
  [Project structure](/manage/mz-deploy/develop/project-structure/) for details.

- You cannot use the same schema for both storage objects (e.g., sources, tables
  created from sources) and compute objects (e.g., views, materialized views,
  and indexes). Storage and compute objects must reside in separate schemas.
