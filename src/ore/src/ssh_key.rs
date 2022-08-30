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

use openssl::pkey::{PKey, Private};
use serde::{Deserialize, Serialize};
use ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey, KeypairData};
use ssh_key::public::Ed25519PublicKey;
use ssh_key::{LineEnding, PrivateKey};

/// Keeps track of a public-private keypair, stored in the OpenSSH format
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct SshKeypair {
    public_key: Vec<u8>,
    private_key: Vec<u8>,
}

impl SshKeypair {
    /// Generate a new keypair for SSH connections.
    ///
    /// Ed25519 keys are generated via OpenSSL, using [`ssh_key`] to convert
    /// them into the proprietary OpenSSH format.
    pub fn new() -> Result<SshKeypair, anyhow::Error> {
        let openssl_key = PKey::<Private>::generate_ed25519()?;

        let keypair = KeypairData::Ed25519(Ed25519Keypair {
            public: Ed25519PublicKey::try_from(openssl_key.raw_public_key()?.as_slice())?,
            private: Ed25519PrivateKey::try_from(openssl_key.raw_private_key()?.as_slice())?,
        });

        let private_key_wrapper = PrivateKey::new(keypair, "materialize")?;
        let openssh_private_key = &*private_key_wrapper.to_openssh(LineEnding::LF)?;
        let openssh_public_key = private_key_wrapper.public_key().to_openssh()?;

        Ok(SshKeypair {
            public_key: openssh_public_key.as_bytes().to_vec(),
            private_key: openssh_private_key.as_bytes().to_vec(),
        })
    }

    /// Return the public key encoded in the OpenSSH format
    pub fn ssh_public_key(&self) -> &[u8] {
        &self.public_key
    }

    /// Return the private key encoded in the OpenSSH format
    pub fn ssh_private_key(&self) -> &[u8] {
        &self.private_key
    }
}

/// A keyset including two SSH keypairs, used to support key rotation.
///
/// When a keyset is rotated, the secondary keypair becomes the new
/// primary keypair, and a new secondary keypair is generated.
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
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
        let primary = std::str::from_utf8(&self.primary.public_key)
            .expect("PEM keys should always be in ASCII");
        let secondary = std::str::from_utf8(&self.secondary.public_key)
            .expect("PEM keys should always be in ASCII");
        (primary.to_string(), secondary.to_string())
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
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serialization of keyset cannot fail")
    }

    /// Deserialize via JSON
    pub fn from_bytes(data: &[u8]) -> anyhow::Result<SshKeyset> {
        Ok(serde_json::from_slice(data)?)
    }
}
