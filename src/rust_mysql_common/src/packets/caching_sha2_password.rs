use std::io;

use crate::{
    io::ParseBuf,
    proto::{MyDeserialize, MySerialize},
};

define_header!(
    PublicKeyRequestHeader,
    InvalidPublicKeyRequest("Invalid PublicKeyRequest header"),
    0x02
);

/// A client request for a server public RSA key, used by some authentication mechanisms
/// to add a layer of protection to an unsecured channel (see [`PublicKeyResponse`]).
///
/// [`PublicKeyResponse`]: crate::packets::PublicKeyResponse
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicKeyRequest {
    __header: PublicKeyRequestHeader,
}

impl PublicKeyRequest {
    pub fn new() -> Self {
        Self {
            __header: PublicKeyRequestHeader::new(),
        }
    }
}

impl<'de> MyDeserialize<'de> for PublicKeyRequest {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            __header: buf.parse(())?,
        })
    }
}

impl MySerialize for PublicKeyRequest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.__header.serialize(&mut *buf);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        io::ParseBuf,
        packets::caching_sha2_password::PublicKeyRequest,
        proto::{MyDeserialize, MySerialize},
    };

    #[test]
    fn should_parse_rsa_public_key_request_packet() {
        const RSA_PUBLIC_KEY_REQUEST: &[u8] = b"\x02";

        let public_rsa_key_request =
            PublicKeyRequest::deserialize((), &mut ParseBuf(RSA_PUBLIC_KEY_REQUEST));

        assert!(public_rsa_key_request.is_ok());
    }

    #[test]
    fn should_build_rsa_public_key_request_packet() {
        let rsa_public_key_request = PublicKeyRequest::new();

        let mut actual = Vec::new();
        rsa_public_key_request.serialize(&mut actual);

        let expected: Vec<u8> = [0x02].to_vec();

        assert_eq!(expected, actual);
    }
}
