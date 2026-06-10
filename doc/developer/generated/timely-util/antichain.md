---
source: src/timely-util/src/antichain.rs
revision: 74ebdd68dd
---

# timely-util::antichain

Defines the `AntichainExt` extension trait and `FrontierPrinter` helper, which add a `pretty()` method to timely's `Antichain`, `MutableAntichain`, and `AntichainRef` types.
`FrontierPrinter` formats a frontier as a brace-delimited, comma-separated set of timestamps, improving log readability.
