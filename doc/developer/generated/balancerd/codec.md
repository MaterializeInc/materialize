---
source: src/balancerd/src/codec.rs
revision: 2daa609ac4
---

# balancerd::codec

Provides `FramedConn<A>`, a pgwire framing layer used by the balancer during the authentication phase of a pgwire connection.

`FramedConn` wraps a `tokio_util::codec::Framed` stream using the internal `Codec` type, which decodes only the `Password` and `Terminate` frontend message types and encodes `AuthenticationCleartextPassword` and `ErrorResponse` backend messages. The codec decodes at most one frame per connection (the client's credential) and is bounded by `MAX_PREAUTH_FRAME_SIZE` throughout. Once the destination is resolved the connection is spliced and the remaining bytes are proxied without being framed.
This minimal codec is sufficient for balancerd to extract user credentials before handing the raw TCP stream off to the target `environmentd` process.
