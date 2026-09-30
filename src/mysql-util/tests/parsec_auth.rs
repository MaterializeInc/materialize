// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io;
use std::panic;

use mysql_async::OptsBuilder;
use mz_mysql_util::{Config, MySqlError, TimeoutConfig, TunnelConfig};
use mz_ore::future::InTask;
use mz_ore::panic::install_enhanced_handler;
use mz_ssh_util::tunnel::SshTimeoutConfig;
use mz_ssh_util::tunnel_manager::SshTunnelManager;
use scopeguard::defer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// NOTE: Do not add more tests to this file. The test installs the enhanced
// panic handler, which aborts the process on any panic outside a catch scope,
// and would take down concurrently running tests with it.

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) -> io::Result<()> {
    let len = u32::try_from(payload.len()).unwrap().to_le_bytes();
    stream.write_all(&[len[0], len[1], len[2], seq]).await?;
    stream.write_all(payload).await
}

/// Reads one packet and returns its sequence id.
async fn read_packet(stream: &mut TcpStream) -> io::Result<u8> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).await?;
    let len = usize::try_from(u32::from_le_bytes([header[0], header[1], header[2], 0])).unwrap();
    let mut payload = vec![0; len];
    stream.read_exact(&mut payload).await?;
    Ok(header[3])
}

/// Plays the server side of a handshake that switches the client to the
/// `parsec` authentication plugin.
async fn serve_parsec_auth_switch(mut stream: TcpStream) -> io::Result<()> {
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
    write_packet(&mut stream, 0, &handshake).await?;

    let seq = read_packet(&mut stream).await?;
    let mut auth_switch = vec![0xfe];
    auth_switch.extend_from_slice(b"parsec\0");
    auth_switch.extend_from_slice(&[7; 32]);
    write_packet(&mut stream, seq + 1, &auth_switch).await?;

    let seq = read_packet(&mut stream).await?;
    let mut ext_salt = vec![b'P', 0];
    ext_salt.extend_from_slice(&[9; 18]);
    write_packet(&mut stream, seq + 1, &ext_salt).await?;

    // Keep the connection open until the client gives up.
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).await?;
    Ok(())
}

#[tokio::test] // allow(test-attribute)
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `socket`
async fn parsec_auth_switch_returns_error() {
    let old_hook = panic::take_hook();
    defer! {
        panic::set_hook(old_hook);
    }
    // Production installs this handler, which turns an uncaught panic into a
    // process abort. Without it, a panic in a spawned task reaches the caller
    // through its `JoinHandle`, and a catch in the wrong task would pass.
    install_enhanced_handler();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _server = mz_ore::task::spawn(|| "fake_mysql_server", async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            mz_ore::task::spawn(|| "fake_mysql_conn", async move {
                let _ = serve_parsec_auth_switch(stream).await;
            });
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
