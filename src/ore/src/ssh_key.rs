// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Utilities for generating and managing SSH keys.

use std::fmt;

use openssl::pkey::{PKey, Private};
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey, KeypairData};
use ssh_key::public::Ed25519PublicKey;
use ssh_key::{LineEnding, PrivateKey};

/// Keeps track of a public-private keypair, stored in the OpenSSH format
#[derive(Debug, Clone)]
pub struct SshKeypair {
    // Even though the type is called PrivateKey, it includes the full keypair,
    // and zeroes memory on `Drop`.
    keypair: PrivateKey,
}

impl SshKeypair {
    /// Generate a new keypair for SSH connections.
    ///
    /// Ed25519 keys are generated via OpenSSL, using [`ssh_key`] to convert
    /// them into the proprietary OpenSSH format.
    pub fn new() -> Result<SshKeypair, anyhow::Error> {
        let openssl_key = PKey::<Private>::generate_ed25519()?;

        let keypair_data = KeypairData::Ed25519(Ed25519Keypair {
            public: Ed25519PublicKey::try_from(openssl_key.raw_public_key()?.as_slice())?,
            private: Ed25519PrivateKey::try_from(openssl_key.raw_private_key()?.as_slice())?,
        });

        let keypair = PrivateKey::new(keypair_data, "materialize")?;

        Ok(SshKeypair { keypair })
    }

    /// Deserialize a keypair from a private key
    ///
    /// In the OpenSSH format, the public key is embedded inside the private key
    fn from_private_key(private_key: &[u8]) -> Result<SshKeypair, anyhow::Error> {
        let private_key = PrivateKey::from_openssh(private_key)?;

        Ok(SshKeypair {
            keypair: private_key,
        })
    }

    /// Return the public key encoded in the OpenSSH format
    pub fn ssh_public_key(&self) -> String {
        self.keypair.public_key().to_string()
    }

    /// Return the private key encoded in the OpenSSH format
    pub fn ssh_private_key(&self) -> Zeroizing<String> {
        self.keypair.to_openssh(LineEnding::LF).expect("TODO")
        // &self.private_key
    }
}

impl PartialEq for SshKeypair {
    fn eq(&self, other: &Self) -> bool {
        self.keypair.fingerprint(Default::default())
            == other.keypair.fingerprint(Default::default())
    }
}

impl Eq for SshKeypair {}

impl Serialize for SshKeypair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SshKeypair", 2)?;
        // Public key is still encoded for backwards compatibility, but it is not used anymore
        state.serialize_field("public_key", self.ssh_public_key().as_bytes())?;
        state.serialize_field("private_key", self.ssh_private_key().as_bytes())?;
        state.end()
    }
}

use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use zeroize::Zeroizing;

impl<'de> Deserialize<'de> for SshKeypair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            PublicKey,
            PrivateKey,
        }

        struct SshKeypairVisitor;

        impl<'de> Visitor<'de> for SshKeypairVisitor {
            type Value = SshKeypair;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct SshKeypair")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<SshKeypair, V::Error>
            where
                V: SeqAccess<'de>,
            {
                // Public key is still read for backwards compatibility, but it is not used anymore
                let _public_key: Vec<u8> = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let private_key: Zeroizing<Vec<u8>> = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                SshKeypair::from_private_key(&private_key).map_err(de::Error::custom)
            }

            fn visit_map<V>(self, mut map: V) -> Result<SshKeypair, V::Error>
            where
                V: MapAccess<'de>,
            {
                // Public key is still read for backwards compatibility, but it is not used anymore
                let mut _public_key: Option<Vec<u8>> = None;
                let mut private_key: Option<Zeroizing<Vec<u8>>> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::PublicKey => {
                            if _public_key.is_some() {
                                return Err(de::Error::duplicate_field("public_key"));
                            }
                            _public_key = Some(map.next_value()?);
                        }
                        Field::PrivateKey => {
                            if private_key.is_some() {
                                return Err(de::Error::duplicate_field("private_key"));
                            }
                            private_key = Some(map.next_value()?);
                        }
                    }
                }
                let private_key =
                    private_key.ok_or_else(|| de::Error::missing_field("private_key"))?;
                SshKeypair::from_private_key(&private_key).map_err(de::Error::custom)
            }
        }

        const FIELDS: &'static [&'static str] = &["public_key", "private_key"];
        deserializer.deserialize_struct("SshKeypair", FIELDS, SshKeypairVisitor)
    }
}

/// A keyset including two SSH keypairs, used to support key rotation.
///
/// When a keyset is rotated, the secondary keypair becomes the new
/// primary keypair, and a new secondary keypair is generated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshKeyset {
    primary: SshKeypair,
    secondary: SshKeypair,
}

impl SshKeyset {
    /// Generate a new keyset with random keys
    pub fn new() -> Result<SshKeyset, anyhow::Error> {
        Ok(SshKeyset {
            primary: SshKeypair::new()?,
            secondary: SshKeypair::new()?,
        })
    }

    /// Return a rotated keyset.
    ///
    /// The rotation works by promoting the secondary keypair to primary,
    /// and generating a new random secondary keypair.
    pub fn rotate(&self) -> anyhow::Result<SshKeyset> {
        Ok(SshKeyset {
            primary: self.secondary.clone(),
            secondary: SshKeypair::new()?,
        })
    }

    /// Return the pair of public keys
    pub fn public_keys(&self) -> (String, String) {
        let primary = self.primary().ssh_public_key();
        let secondary = self.secondary().ssh_public_key();
        (primary, secondary)
    }

    /// Return the primary keypair
    pub fn primary(&self) -> &SshKeypair {
        &self.primary
    }

    /// Return the secondary keypair
    pub fn secondary(&self) -> &SshKeypair {
        &self.secondary
    }

    /// Serialize via JSON
    pub fn to_bytes(&self) -> Zeroizing<Vec<u8>> {
        Zeroizing::new(serde_json::to_vec(self).expect("serialization of keyset cannot fail"))
    }

    /// Deserialize via JSON
    pub fn from_bytes(data: &[u8]) -> anyhow::Result<SshKeyset> {
        Ok(serde_json::from_slice(data)?)
    }
}

#[cfg(test)]
#[cfg(feature = "ssh")]
mod tests {
    use super::SshKeypair;
    use super::SshKeyset;
    use openssl::pkey::{PKey, Private};
    use serde::{Deserialize, Serialize};
    use ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey, KeypairData};
    use ssh_key::public::Ed25519PublicKey;
    use ssh_key::{LineEnding, PrivateKey};

    #[test]
    #[cfg(feature = "ssh")]
    fn test_keypair_generation() -> anyhow::Result<()> {
        for _ in 0..100 {
            let keypair = SshKeypair::new()?;

            // Public keys should be in ASCII
            let public_key = keypair.ssh_public_key();
            // Public keys should be in the OpenSSH format
            assert!(public_key.starts_with("ssh-ed25519 "));
            // Private keys should be in ASCII
            let private_key = keypair.ssh_private_key();
            // Private keys should also be in the OpenSSH format
            assert!(private_key.starts_with("-----BEGIN OPENSSH PRIVATE KEY-----"));
        }
        Ok(())
    }

    #[test]
    #[cfg(feature = "ssh")]
    fn test_unique_keys() -> anyhow::Result<()> {
        for _ in 0..100 {
            let keyset = SshKeyset::new()?;
            assert_ne!(keyset.primary(), keyset.secondary());
        }
        Ok(())
    }

    #[test]
    #[cfg(feature = "ssh")]
    fn test_keypair_serialization_roundtrip() -> anyhow::Result<()> {
        for _ in 0..100 {
            let keypair = SshKeypair::new()?;
            let roundtripped_keypair: SshKeypair = serde_json::from_slice(
                &serde_json::to_vec(&keypair).expect("serialization of keyset cannot fail"),
            )?;

            assert_eq!(keypair, roundtripped_keypair);
        }
        Ok(())
    }

    #[test]
    #[cfg(feature = "ssh")]
    fn test_keyset_serialization_roundtrip() -> anyhow::Result<()> {
        for _ in 0..100 {
            let keyset = SshKeyset::new()?;
            let roundtripped_keyset = SshKeyset::from_bytes(keyset.to_bytes().as_slice())?;

            assert_eq!(keyset, roundtripped_keyset);
        }
        Ok(())
    }

    #[test]
    #[cfg(feature = "ssh")]
    fn test_key_rotation() -> anyhow::Result<()> {
        for _ in 0..100 {
            let keyset = SshKeyset::new()?;
            let rotated_keyset = keyset.rotate()?;

            assert_eq!(keyset.secondary(), rotated_keyset.primary());
            assert_ne!(keyset.primary(), rotated_keyset.secondary());
            assert_ne!(rotated_keyset.primary(), rotated_keyset.secondary());
        }
        Ok(())
    }

    /// Ensure the new code can read legacy generated Keypairs
    #[test]
    #[cfg(feature = "ssh")]
    fn test_deserializing_legacy_keypairs() -> anyhow::Result<()> {
        for _ in 0..100 {
            let legacy_keypair = LegacySshKeypair::new()?;
            let roundtripped_keypair: SshKeypair = serde_json::from_slice(
                &serde_json::to_vec(&legacy_keypair).expect("serialization of keyset cannot fail"),
            )?;

            assert_eq!(
                legacy_keypair.private_key,
                roundtripped_keypair.ssh_private_key().as_bytes()
            );
        }
        Ok(())
    }

    /// Ensure the legacy code can read newly generated Keypairs, e.g. if we have to rollback
    #[test]
    #[cfg(feature = "ssh")]
    fn test_serializing_legacy_keypairs() -> anyhow::Result<()> {
        for _ in 0..100 {
            let keypair = SshKeypair::new()?;
            let roundtripped_legacy_keypair: LegacySshKeypair = serde_json::from_slice(
                &serde_json::to_vec(&keypair).expect("serialization of keyset cannot fail"),
            )?;

            assert_eq!(
                roundtripped_legacy_keypair.private_key,
                keypair.ssh_private_key().as_bytes()
            );
        }
        Ok(())
    }

    /// Ensure the new code can read legacy generated Keysets
    #[test]
    #[cfg(feature = "ssh")]
    fn test_deserializing_legacy_keysets() -> anyhow::Result<()> {
        for _ in 0..100 {
            let legacy_keypair = LegacySshKeyset::new()?;
            let roundtripped_keypair: SshKeyset = serde_json::from_slice(
                &serde_json::to_vec(&legacy_keypair).expect("serialization of keyset cannot fail"),
            )?;

            // assert_eq!(legacy_keypair.private_key, roundtripped_keypair.ssh_private_key().as_bytes());
            assert_eq!(
                legacy_keypair.primary.private_key,
                roundtripped_keypair.primary().ssh_private_key().as_bytes()
            );
            assert_eq!(
                legacy_keypair.secondary.private_key,
                roundtripped_keypair
                    .secondary()
                    .ssh_private_key()
                    .as_bytes()
            );
        }
        Ok(())
    }

    /// Ensure the legacy code can read newly generated Keysets, e.g. if we have to rollback
    #[test]
    #[cfg(feature = "ssh")]
    fn test_serializing_legacy_keysets() -> anyhow::Result<()> {
        for _ in 0..100 {
            let keypair = SshKeyset::new()?;
            let roundtripped_legacy_keypair: LegacySshKeyset = serde_json::from_slice(
                &serde_json::to_vec(&keypair).expect("serialization of keyset cannot fail"),
            )?;

            assert_eq!(
                roundtripped_legacy_keypair.primary.private_key,
                keypair.primary().ssh_private_key().as_bytes()
            );
            assert_eq!(
                roundtripped_legacy_keypair.secondary.private_key,
                keypair.secondary().ssh_private_key().as_bytes()
            );
        }
        Ok(())
    }

    /// The previously used Keypair struct, here to test serialization logic across versions
    #[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
    struct LegacySshKeypair {
        public_key: Vec<u8>,
        private_key: Vec<u8>,
    }

    impl LegacySshKeypair {
        fn new() -> Result<Self, anyhow::Error> {
            let openssl_key = PKey::<Private>::generate_ed25519()?;

            let keypair = KeypairData::Ed25519(Ed25519Keypair {
                public: Ed25519PublicKey::try_from(openssl_key.raw_public_key()?.as_slice())?,
                private: Ed25519PrivateKey::try_from(openssl_key.raw_private_key()?.as_slice())?,
            });

            let private_key_wrapper = PrivateKey::new(keypair, "materialize")?;
            let openssh_private_key = &*private_key_wrapper.to_openssh(LineEnding::LF)?;
            let openssh_public_key = private_key_wrapper.public_key().to_openssh()?;

            Ok(Self {
                public_key: openssh_public_key.as_bytes().to_vec(),
                private_key: openssh_private_key.as_bytes().to_vec(),
            })
        }
    }

    /// The previously used Keyset struct, here to test serialization logic across versions
    #[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
    struct LegacySshKeyset {
        primary: LegacySshKeypair,
        secondary: LegacySshKeypair,
    }

    impl LegacySshKeyset {
        /// Generate a new keyset with random keys
        fn new() -> Result<Self, anyhow::Error> {
            Ok(Self {
                primary: LegacySshKeypair::new()?,
                secondary: LegacySshKeypair::new()?,
            })
        }
    }
}
