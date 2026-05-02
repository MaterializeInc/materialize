---
source: src/environmentd/src/http/memory.rs
revision: f0a264e83a
---

# environmentd::http::memory

Serves HTML memory profiling pages using Askama templates: `handle_memory` renders `memory.html` and `handle_hierarchical_memory` renders `hierarchical-memory.html`, both embedding the current build version.
