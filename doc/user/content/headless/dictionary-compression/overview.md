---
headless: true
---
Dictionary compression reduces the memory that
[arrangements](/get-started/arrangements/#arrangements) use when a column holds
the same values over and over. Instead of storing a repeated value in full once
per row, Materialize stores that value once and has each row reference it.
