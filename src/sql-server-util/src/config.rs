// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use mz_ore::future::InTask;
use mz_repr::CatalogItemId;
use mz_ssh_util::tunnel::{SshTimeoutConfig, SshTunnelConfig};
use mz_ssh_util::tunnel_manager::SshTunnelManager;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// Materialize specific configuration for SQL Server connections.
///
/// This wraps a [`tiberius::Config`] so we can configure a tunnel over SSH, AWS
/// PrivateLink, or various other techniques, via a [`TunnelConfig`]
#[derive(Clone, Debug)]
pub struct Config {
    /// SQL Server specific configuration.
    pub(crate) inner: tiberius::Config,
    /// Details of how we'll connect to the upstream SQL Server instance.
    pub(crate) tunnel: TunnelConfig,
    /// If all of the I/O for this connection will be done in a separate task.
    ///
    /// Note: This is used to prevent accidentally doing I/O in timely threads.
    pub(crate) in_task: InTask,
}

impl Config {
    pub fn new(inner: tiberius::Config, tunnel: TunnelConfig, in_task: InTask) -> Self {
        Config {
            inner,
            tunnel,
            in_task,
        }
    }

    /// Create a new [`Config`] from an ActiveX Data Object.
    ///
    /// Generally this is only used in test environments, see [`Config::new`]
    /// for regular/production use cases.
    pub fn from_ado_string(s: &str) -> Result<Self, anyhow::Error> {
        let inner = tiberius::Config::from_ado_string(s).context("tiberius config")?;
        Ok(Config {
            inner,
            tunnel: TunnelConfig::Direct,
            in_task: InTask::No,
        })
    }
}

/// Configures an optional tunnel for use when connecting to a SQL Server database.
///
/// TODO(sql_server2): De-duplicate this with MySQL and Postgres sources.
#[derive(Debug, Clone)]
pub enum TunnelConfig {
    /// No tunnelling.
    Direct,
    /// Establish a TCP connection to the database via an SSH tunnel.
    Ssh {
        /// Config for opening the SSH tunnel.
        config: SshTunnelConfig,
        /// Global manager of SSH tunnels.
        manager: SshTunnelManager,
        /// Timeout config for the SSH tunnel.
        timeout: SshTimeoutConfig,
        // TODO(sql_server3): Remove these fields by forking the `tiberius`
        // crate and expose the `get_host` and `get_port` methods.
        //
        // See: <https://github.com/MaterializeInc/tiberius/blob/406ad2780d206617bd41689b1b638bddf4538f89/src/client/config.rs#L174-L191>
        host: String,
        port: u16,
    },
    /// Establish a TCP connection to the database via an AWS PrivateLink service.
    AwsPrivatelink {
        /// The ID of the AWS PrivateLink service.
        connection_id: CatalogItemId,
        port: u16,
    },
}

/// Level of encryption to use with a SQL Server connection.
///
/// Mirror of [`tiberius::EncryptionLevel`] but we define our own so we can
/// implement traits like [`Serialize`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Arbitrary, Serialize, Deserialize)]
pub enum EncryptionLevel {
    /// Do not use encryption at all.
    None,
    /// Only use encryption for the login procedure.
    Login,
    /// Use encryption for everything, if possible.
    Preferred,
    /// Require encryption, failing if not possible.
    Required,
}

impl From<tiberius::EncryptionLevel> for EncryptionLevel {
    fn from(value: tiberius::EncryptionLevel) -> Self {
        match value {
            tiberius::EncryptionLevel::NotSupported => EncryptionLevel::None,
            tiberius::EncryptionLevel::Off => EncryptionLevel::Login,
            tiberius::EncryptionLevel::On => EncryptionLevel::Preferred,
            tiberius::EncryptionLevel::Required => EncryptionLevel::Required,
        }
    }
}

impl From<EncryptionLevel> for tiberius::EncryptionLevel {
    fn from(value: EncryptionLevel) -> Self {
        match value {
            EncryptionLevel::None => tiberius::EncryptionLevel::NotSupported,
            EncryptionLevel::Login => tiberius::EncryptionLevel::Off,
            EncryptionLevel::Preferred => tiberius::EncryptionLevel::On,
            EncryptionLevel::Required => tiberius::EncryptionLevel::Required,
        }
    }
}

/// Policy that dictates validation of the SQL-SERVER certificate.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Arbitrary, Serialize, Deserialize)]
pub enum CertificateValidationPolicy {
    /// Don't validate the server's certificate; trust all certificates.
    TrustAll,
    /// Validate server's certificate using system certificates.
    VerifySystem,
    /// Validate server's certifiacte using provided CA certificate.
    VerifyCA,
}
