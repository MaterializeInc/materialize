---
source: src/frontegg-mock/src/models/sso.rs
revision: 8041e666f1
---

# frontegg-mock::models::sso

Defines the full suite of SSO configuration data types: `SSOConfigStorage` (in-memory record), `SSOConfigResponse`/`SSOConfigCreateRequest`/`SSOConfigUpdateRequest` (API shapes), `Domain`/`DomainResponse`/`DomainUpdateRequest`, and `From` conversions between storage and response forms.
`Domain.txt_record` is generated deterministically at response time using two random UUIDs, matching Frontegg's DNS validation challenge format.
