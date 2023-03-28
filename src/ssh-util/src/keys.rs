// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for generating and managing SSH keys.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use openssl::pkey::{PKey, Private};
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey, KeypairData};
use ssh_key::public::Ed25519PublicKey;
use ssh_key::{HashAlg, LineEnding, PrivateKey};
use zeroize::Zeroizing;

/// A SSH key pair consisting of a public and private key.
#[derive(Debug, Clone)]
pub struct SshKeyPair {
    // Even though the type is called PrivateKey, it includes the full key pair,
    // and zeroes memory on `Drop`.
    key_pair: PrivateKey,
}

impl SshKeyPair {
    /// Generates a new SSH key pair.
    ///
    /// Ed25519 keys are generated via OpenSSL, using [`ssh_key`] to convert
    /// them into the OpenSSH format.
    pub fn new() -> Result<SshKeyPair, anyhow::Error> {
        let openssl_key = PKey::<Private>::generate_ed25519()?;

        let key_pair_data = KeypairData::Ed25519(Ed25519Keypair {
            public: Ed25519PublicKey::try_from(openssl_key.raw_public_key()?.as_slice())?,
            private: Ed25519PrivateKey::try_from(openssl_key.raw_private_key()?.as_slice())?,
        });

        let key_pair = PrivateKey::new(key_pair_data, "materialize")?;

        Ok(SshKeyPair { key_pair })
    }

    /// Deserializes a key pair from a key pair set that was serialized with
    /// [`SshKeyPairSet::serialize`].
    pub fn from_bytes(data: &[u8]) -> anyhow::Result<SshKeyPair> {
        let set = SshKeyPairSet::from_bytes(data)?;
        Ok(set.primary().clone())
    }

    /// Deserializes a key pair from an OpenSSH-formatted private key.
    fn from_private_key(private_key: &[u8]) -> Result<SshKeyPair, anyhow::Error> {
        let private_key = PrivateKey::from_openssh(private_key)?;

        Ok(SshKeyPair {
            key_pair: private_key,
        })
    }

    /// Returns the public key encoded in the OpenSSH format.
    pub fn ssh_public_key(&self) -> String {
        self.key_pair.public_key().to_string()
    }

    /// Return the private key encoded in the OpenSSH format.
    pub fn ssh_private_key(&self) -> Zeroizing<String> {
        self.key_pair
            .to_openssh(LineEnding::LF)
            .expect("encoding as OpenSSH cannot fail")
    }
}

impl Hash for SshKeyPair {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.key_pair
            .fingerprint(HashAlg::default())
            .as_ref()
            .hash(state)
    }
}

impl PartialEq for SshKeyPair {
    fn eq(&self, other: &Self) -> bool {
        self.key_pair.fingerprint(HashAlg::default())
            == other.key_pair.fingerprint(HashAlg::default())
    }
}

impl Eq for SshKeyPair {}

impl PartialOrd for SshKeyPair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key_pair
            .fingerprint(HashAlg::default())
            .partial_cmp(&other.key_pair.fingerprint(HashAlg::default()))
    }
}

impl Ord for SshKeyPair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key_pair
            .fingerprint(HashAlg::default())
            .cmp(&other.key_pair.fingerprint(HashAlg::default()))
    }
}

impl Serialize for SshKeyPair {
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

impl<'de> Deserialize<'de> for SshKeyPair {
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

        struct SshKeyPairVisitor;

        impl<'de> Visitor<'de> for SshKeyPairVisitor {
            type Value = SshKeyPair;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct SshKeypair")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<SshKeyPair, V::Error>
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
                SshKeyPair::from_private_key(&private_key).map_err(de::Error::custom)
            }

            fn visit_map<V>(self, mut map: V) -> Result<SshKeyPair, V::Error>
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
                SshKeyPair::from_private_key(&private_key).map_err(de::Error::custom)
            }
        }

        const FIELDS: &[&str] = &["public_key", "private_key"];
        deserializer.deserialize_struct("SshKeypair", FIELDS, SshKeyPairVisitor)
    }
}

/// A set of two SSH key pairs, used to support key rotation.
///
/// When a key pair set is rotated, the secondary key pair becomes the new
/// primary key pair, and a new secondary key pair is generated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshKeyPairSet {
    primary: SshKeyPair,
    secondary: SshKeyPair,
}

impl SshKeyPairSet {
    /// Generates a new key pair set with random key pairs.
    pub fn new() -> Result<SshKeyPairSet, anyhow::Error> {
        Ok(SshKeyPairSet {
            primary: SshKeyPair::new()?,
            secondary: SshKeyPair::new()?,
        })
    }

    /// Rotate the key pairs in the set.
    ///
    /// The rotation promotes the secondary key_pair to primary and generates a
    /// new random secondary key pair.
    pub fn rotate(&self) -> Result<SshKeyPairSet, anyhow::Error> {
        Ok(SshKeyPairSet {
            primary: self.secondary.clone(),
            secondary: SshKeyPair::new()?,
        })
    }

    /// Returns the primary and secondary public keys in the set.
    pub fn public_keys(&self) -> (String, String) {
        let primary = self.primary().ssh_public_key();
        let secondary = self.secondary().ssh_public_key();
        (primary, secondary)
    }

    /// Return the primary key pair.
    pub fn primary(&self) -> &SshKeyPair {
        &self.primary
    }

    /// Returns the secondary pair.
    pub fn secondary(&self) -> &SshKeyPair {
        &self.secondary
    }

    /// Serializes the key pair set to an unspecified encoding.
    ///
    /// You can deserialize a key pair set with [`SshKeyPairSet::deserialize`].
    pub fn to_bytes(&self) -> Zeroizing<Vec<u8>> {
        Zeroizing::new(serde_json::to_vec(self).expect("serialization of key_set cannot fail"))
    }

    /// Deserializes a key pair set that was serialized with
    /// [`SshKeyPairSet::serialize`].
    pub fn from_bytes(data: &[u8]) -> anyhow::Result<SshKeyPairSet> {
        Ok(serde_json::from_slice(data)?)
    }
}

#[cfg(test)]
mod tests {
    use openssl::pkey::{PKey, Private};
    use serde::{Deserialize, Serialize};
    use ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey, KeypairData};
    use ssh_key::public::Ed25519PublicKey;
    use ssh_key::{LineEnding, PrivateKey};

    use super::{SshKeyPair, SshKeyPairSet};

    #[mz_ore::test]
    fn test_key_pair_generation() -> anyhow::Result<()> {
        for _ in 0..100 {
            let key_pair = SshKeyPair::new()?;

            // Public keys should be in ASCII
            let public_key = key_pair.ssh_public_key();
            // Public keys should be in the OpenSSH format
            assert!(public_key.starts_with("ssh-ed25519 "));
            // Private keys should be in ASCII
            let private_key = key_pair.ssh_private_key();
            // Private keys should also be in the OpenSSH format
            assert!(private_key.starts_with("-----BEGIN OPENSSH PRIVATE KEY-----"));
        }
        Ok(())
    }

    #[mz_ore::test]
    fn test_unique_keys() -> anyhow::Result<()> {
        for _ in 0..100 {
            let key_set = SshKeyPairSet::new()?;
            assert_ne!(key_set.primary(), key_set.secondary());
        }
        Ok(())
    }

    #[mz_ore::test]
    fn test_key_pair_serialization_roundtrip() -> anyhow::Result<()> {
        for _ in 0..100 {
            let key_pair = SshKeyPair::new()?;
            let roundtripped_key_pair: SshKeyPair = serde_json::from_slice(
                &serde_json::to_vec(&key_pair).expect("serialization of key_set cannot fail"),
            )?;

            assert_eq!(key_pair, roundtripped_key_pair);
        }
        Ok(())
    }

    #[mz_ore::test]
    fn test_key_set_serialization_roundtrip() -> anyhow::Result<()> {
        for _ in 0..100 {
            let key_set = SshKeyPairSet::new()?;
            let roundtripped_key_set = SshKeyPairSet::from_bytes(key_set.to_bytes().as_slice())?;

            assert_eq!(key_set, roundtripped_key_set);
        }
        Ok(())
    }

    #[mz_ore::test]
    fn test_key_rotation() -> anyhow::Result<()> {
        for _ in 0..100 {
            let key_set = SshKeyPairSet::new()?;
            let rotated_key_set = key_set.rotate()?;

            assert_eq!(key_set.secondary(), rotated_key_set.primary());
            assert_ne!(key_set.primary(), rotated_key_set.secondary());
            assert_ne!(rotated_key_set.primary(), rotated_key_set.secondary());
        }
        Ok(())
    }

    /// Ensure the new code can read legacy generated Keypairs
    #[mz_ore::test]
    fn test_deserializing_legacy_key_pairs() -> anyhow::Result<()> {
        for _ in 0..100 {
            let legacy_key_pair = LegacySshKeyPair::new()?;
            let roundtripped_key_pair: SshKeyPair = serde_json::from_slice(
                &serde_json::to_vec(&legacy_key_pair)
                    .expect("serialization of key_set cannot fail"),
            )?;

            assert_eq!(
                legacy_key_pair.private_key,
                roundtripped_key_pair.ssh_private_key().as_bytes()
            );
        }
        Ok(())
    }

    /// Ensure the legacy code can read newly generated Keypairs, e.g. if we have to rollback
    #[mz_ore::test]
    fn test_serializing_legacy_key_pairs() -> anyhow::Result<()> {
        for _ in 0..100 {
            let key_pair = SshKeyPair::new()?;
            let roundtripped_legacy_key_pair: LegacySshKeyPair = serde_json::from_slice(
                &serde_json::to_vec(&key_pair).expect("serialization of key_set cannot fail"),
            )?;

            assert_eq!(
                roundtripped_legacy_key_pair.private_key,
                key_pair.ssh_private_key().as_bytes()
            );
        }
        Ok(())
    }

    /// Ensure the new code can read legacy generated Keysets
    #[mz_ore::test]
    fn test_deserializing_legacy_key_sets() -> anyhow::Result<()> {
        for _ in 0..100 {
            let legacy_key_pair = LegacySshKeyPairSet::new()?;
            let roundtripped_key_pair: SshKeyPairSet = serde_json::from_slice(
                &serde_json::to_vec(&legacy_key_pair)
                    .expect("serialization of key_set cannot fail"),
            )?;

            // assert_eq!(legacy_key_pair.private_key, roundtripped_key_pair.ssh_private_key().as_bytes());
            assert_eq!(
                legacy_key_pair.primary.private_key,
                roundtripped_key_pair.primary().ssh_private_key().as_bytes()
            );
            assert_eq!(
                legacy_key_pair.secondary.private_key,
                roundtripped_key_pair
                    .secondary()
                    .ssh_private_key()
                    .as_bytes()
            );
        }
        Ok(())
    }

    /// Ensure the legacy code can read newly generated Keysets, e.g. if we have to rollback
    #[mz_ore::test]
    fn test_serializing_legacy_key_sets() -> anyhow::Result<()> {
        for _ in 0..100 {
            let key_pair = SshKeyPairSet::new()?;
            let roundtripped_legacy_key_pair: LegacySshKeyPairSet = serde_json::from_slice(
                &serde_json::to_vec(&key_pair).expect("serialization of key_set cannot fail"),
            )?;

            assert_eq!(
                roundtripped_legacy_key_pair.primary.private_key,
                key_pair.primary().ssh_private_key().as_bytes()
            );
            assert_eq!(
                roundtripped_legacy_key_pair.secondary.private_key,
                key_pair.secondary().ssh_private_key().as_bytes()
            );
        }
        Ok(())
    }

    /// The previously used Keypair struct, here to test serialization logic across versions
    #[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
    struct LegacySshKeyPair {
        public_key: Vec<u8>,
        private_key: Vec<u8>,
    }

    impl LegacySshKeyPair {
        fn new() -> Result<Self, anyhow::Error> {
            let openssl_key = PKey::<Private>::generate_ed25519()?;

            let key_pair = KeypairData::Ed25519(Ed25519Keypair {
                public: Ed25519PublicKey::try_from(openssl_key.raw_public_key()?.as_slice())?,
                private: Ed25519PrivateKey::try_from(openssl_key.raw_private_key()?.as_slice())?,
            });

            let private_key_wrapper = PrivateKey::new(key_pair, "materialize")?;
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
    struct LegacySshKeyPairSet {
        primary: LegacySshKeyPair,
        secondary: LegacySshKeyPair,
    }

    impl LegacySshKeyPairSet {
        /// Generate a new key_set with random keys
        fn new() -> Result<Self, anyhow::Error> {
            Ok(Self {
                primary: LegacySshKeyPair::new()?,
                secondary: LegacySshKeyPair::new()?,
            })
        }
    }
}
