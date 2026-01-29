---
headless: true
---
When replacing an existing materialized view, the operation:

- Replaces the existing materialized view definition with that of the
  replacement view and drops the replacement view at the same time.

- Emits a diff representing the changes between the old and new output. All
  downstream objects must process this diff, which may cause temporary CPU and
  memory spikes depending on the size of the changes.
