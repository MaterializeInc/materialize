// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pin the relay envelope consumed by participating deployment versions.

use mz_proto::ProtoDuration;
use prost::Message;
use tonic::server::NamedService;

use super::*;

#[mz_ore::test]
fn compatible_relay_envelope() {
    // These frames pin the supported relay envelope. The opaque payload is
    // deliberately not a Persist state diff: the relay must not interpret it.
    let shard_id = "s00000000-0000-0000-0000-000000000000";
    let shard_hex = hex::encode(shard_id);
    let cases = [
        (
            format!("0a0408011002122d0a25{shard_hex}102a1a0200ff"),
            proto_pub_sub_message::Message::PushDiff(ProtoPushDiff {
                shard_id: shard_id.into(),
                seqno: 42,
                diff: vec![0, 255].into(),
            }),
        ),
        (
            format!("0a04080110021a270a25{shard_hex}"),
            proto_pub_sub_message::Message::Subscribe(ProtoSubscribe {
                shard_id: shard_id.into(),
            }),
        ),
        (
            format!("0a040801100222270a25{shard_hex}"),
            proto_pub_sub_message::Message::Unsubscribe(ProtoUnsubscribe {
                shard_id: shard_id.into(),
            }),
        ),
    ];
    for (frame, message) in cases {
        let mut frame = hex::decode(frame).expect("valid fixture");
        let expected = ProtoPubSubMessage {
            timestamp: Some(ProtoDuration { secs: 1, nanos: 2 }),
            message: Some(message),
        };
        assert_eq!(
            ProtoPubSubMessage::decode(frame.as_slice()).unwrap(),
            expected
        );
        assert_eq!(expected.encode_to_vec(), frame);

        // An unknown optional field cannot prevent an older peer from consuming
        // the understood part of the envelope.
        frame.extend_from_slice(&[0x28, 0x01]);
        assert_eq!(
            ProtoPubSubMessage::decode(frame.as_slice()).unwrap(),
            expected
        );
    }
    assert_eq!(
        <ProtoPersistPubSubServer<PersistGrpcPubSubServer> as NamedService>::NAME,
        "mz_persist_client.internal.service.ProtoPersistPubSub"
    );
}
