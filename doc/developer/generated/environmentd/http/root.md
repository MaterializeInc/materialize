---
source: src/environmentd/src/http/root.rs
revision: d3ab9f79a2
---

# environmentd::http::root

Provides the homepage handler (`handle_home`) that renders the `home.html` Askama template with version/build-sha/routes information, and a static file handler for bundled web assets (included at compile time via `include_dir!` and served from disk in dev mode).
