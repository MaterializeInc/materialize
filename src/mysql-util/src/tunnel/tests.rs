// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mysql_async::OptsBuilder;
use mz_ore::future::InTask;
use mz_ssh_util::tunnel::SshTimeoutConfig;
use mz_ssh_util::tunnel_manager::SshTunnelManager;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::MySqlError;
use crate::tunnel::{Config, TimeoutConfig, TunnelConfig};

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
    let len = u32::try_from(payload.len()).unwrap().to_le_bytes();
    stream
        .write_all(&[len[0], len[1], len[2], seq])
        .await
        .unwrap();
    stream.write_all(payload).await.unwrap();
}

/// Reads one packet and returns its sequence id.
async fn read_packet(stream: &mut TcpStream) -> u8 {
    let mut header = [0; 4];
    stream.read_exact(&mut header).await.unwrap();
    let len = usize::try_from(u32::from_le_bytes([header[0], header[1], header[2], 0])).unwrap();
    let mut payload = vec![0; len];
    stream.read_exact(&mut payload).await.unwrap();
    header[3]
}

/// Plays the server side of a handshake that switches the client to the
/// `parsec` authentication plugin.
async fn serve_parsec_auth_switch(mut stream: TcpStream) {
    // CLIENT_LONG_PASSWORD | CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 |
    // CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH |
    // CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
    let capabilities: u32 = 0x0000_0001
        | 0x0000_0008
        | 0x0000_0200
        | 0x0000_2000
        | 0x0000_8000
        | 0x0008_0000
        | 0x0020_0000;
    let caps = capabilities.to_le_bytes();
    let mut handshake = vec![0x0a];
    handshake.extend_from_slice(b"8.0.0\0");
    handshake.extend_from_slice(&1u32.to_le_bytes());
    handshake.extend_from_slice(b"abcdefgh");
    handshake.push(0);
    handshake.extend_from_slice(&caps[..2]);
    handshake.push(0x21);
    handshake.extend_from_slice(&2u16.to_le_bytes());
    handshake.extend_from_slice(&caps[2..]);
    handshake.push(21);
    handshake.extend_from_slice(&[0; 10]);
    handshake.extend_from_slice(b"ijklmnopqrst\0");
    handshake.extend_from_slice(b"mysql_native_password\0");
    write_packet(&mut stream, 0, &handshake).await;

    let seq = read_packet(&mut stream).await;
    let mut auth_switch = vec![0xfe];
    auth_switch.extend_from_slice(b"parsec\0");
    auth_switch.extend_from_slice(&[7; 32]);
    write_packet(&mut stream, seq + 1, &auth_switch).await;

    let seq = read_packet(&mut stream).await;
    let mut ext_salt = vec![b'P', 0];
    ext_salt.extend_from_slice(&[9; 18]);
    write_packet(&mut stream, seq + 1, &ext_salt).await;

    // Keep the connection open until the client gives up.
    let mut rest = Vec::new();
    let _ = stream.read_to_end(&mut rest).await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `socket`
async fn parsec_auth_switch_returns_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _server = mz_ore::task::spawn(|| "fake_mysql_server", async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            mz_ore::task::spawn(|| "fake_mysql_conn", serve_parsec_auth_switch(stream));
        }
    })
    .abort_on_drop();

    for in_task in [InTask::No, InTask::Yes] {
        let builder = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(port)
            .user(Some("user"))
            .pass(Some("password"));
        let config = Config::new(
            builder,
            TunnelConfig::Direct { resolved_ips: None },
            SshTimeoutConfig::default(),
            in_task,
            TimeoutConfig::default(),
            None,
        )
        .unwrap();
        let err = config
            .connect("test", &SshTunnelManager::default())
            .await
            .expect_err("parsec is not supported");
        assert!(
            matches!(&err, MySqlError::ConnectionPanicked(msg) if msg.contains("parsec")),
            "got {err:?}"
        );
    }
}
