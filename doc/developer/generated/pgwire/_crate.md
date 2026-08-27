---
source: src/pgwire/src/lib.rs
revision: 34a619c51e
---

# pgwire

Implements the PostgreSQL Frontend/Backend wire protocol server for Materialize, accepting client connections and executing SQL via `mz_adapter`.
The crate is organized into four main modules: `codec` (frame serialization/deserialization), `message` (backend message types), `protocol` (session state machine), and `server` (TCP connection entry point).
Key public exports are `Server` and `Config` (for instantiating the server), `MetricsConfig` (for registering Prometheus metrics), and `match_handshake` (for protocol sniffing).
A `fuzz_exports` module (behind `#[cfg(feature = "fuzzing")]`) re-exports `codec::Codec` and the authentication sub-parsers (`decode_password`, `decode_sasl_initial_response`, `decode_sasl_response`) for the fuzz crate; the auth parsers are included because `Codec::decode` copies authentication payloads verbatim into `FrontendMessage::RawAuthentication` rather than decoding them, making the pre-auth SASL/password grammars unreachable from the decoder alone.
Primary dependencies are `mz-adapter`, `mz-pgwire-common`, `mz-frontegg-auth`, `mz-authenticator`, `mz-pgrepr`, `mz-server-core`, and `mz-pgcopy`; it is consumed by `mz-environmentd`.
