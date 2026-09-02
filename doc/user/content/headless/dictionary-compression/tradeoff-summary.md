---
headless: true
---
Dictionary compression trades CPU for memory, and it does **not** reduce memory
on every workload. The savings come from large arrangements with columns that
hold a small set of longer values repeated across many rows, such as status
strings, enum-like labels, or tenant IDs. High-cardinality columns pay the CPU
cost with little or no memory benefit, and that cost is most visible as slower
hydration.

For the full tradeoff, guidance on whether your workload is a good fit, and how
to measure the effect, see [Dictionary
compression](/transform-data/dictionary-compression/).
