// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt::{self, Write};
use std::num::ParseIntError;
use std::str::FromStr;

use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_proto::{ProtoType, RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_kafka_util.addr.rs"));

/// Represents the addresses of several Kafka brokers.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaAddrs(Vec<(String, u16)>);

impl FromStr for KafkaAddrs {
    type Err = KafkaAddrsParseError;

    fn from_str(s: &str) -> Result<KafkaAddrs, Self::Err> {
        let mut addrs = vec![];
        for s in s.split(',') {
            let mut parts = s.splitn(2, ':');
            let host = parts.next().expect("splitn returns at least one part");
            let port = match parts.next() {
                None => 9092,
                Some(port) => port.parse().map_err(KafkaAddrsParseError::InvalidPort)?,
            };
            addrs.push((host.to_owned(), port));
        }
        Ok(KafkaAddrs(addrs))
    }
}

impl fmt::Display for KafkaAddrs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, (host, port)) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_char(',')?;
            }
            write!(f, "{}:{}", host, port)?;
        }
        Ok(())
    }
}

impl RustType<proto_kafka_addrs::ProtoKafkaAddr> for (String, u16) {
    fn into_proto(&self) -> proto_kafka_addrs::ProtoKafkaAddr {
        proto_kafka_addrs::ProtoKafkaAddr {
            host: self.0.clone(),
            port: self.1.into_proto(),
        }
    }

    fn from_proto(proto: proto_kafka_addrs::ProtoKafkaAddr) -> Result<Self, TryFromProtoError> {
        Ok((proto.host, proto.port.into_rust()?))
    }
}

impl RustType<ProtoKafkaAddrs> for KafkaAddrs {
    fn into_proto(&self) -> ProtoKafkaAddrs {
        ProtoKafkaAddrs {
            addrs: self.0.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKafkaAddrs) -> Result<Self, TryFromProtoError> {
        Ok(KafkaAddrs(proto.addrs.into_rust()?))
    }
}

/// An error while parsing a Kafka address.
#[derive(Clone, Debug)]
pub enum KafkaAddrsParseError {
    /// The Kafka address contained an invalid port.
    InvalidPort(ParseIntError),
}

impl fmt::Display for KafkaAddrsParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KafkaAddrsParseError::InvalidPort(e) => write!(
                f,
                "unable to parse Kafka broker address: invalid port: {}",
                e
            ),
        }
    }
}

impl Error for KafkaAddrsParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ok() -> Result<(), Box<dyn Error>> {
        struct TestCase {
            input: &'static str,
            addrs: Vec<(String, u16)>,
            output: &'static str,
        }

        let test_cases = vec![
            TestCase {
                input: "localhost",
                addrs: vec![("localhost".into(), 9092)],
                output: "localhost:9092",
            },
            TestCase {
                input: "foohost",
                addrs: vec![("foohost".into(), 9092)],
                output: "foohost:9092",
            },
            TestCase {
                input: "1.2.3.4",
                addrs: vec![("1.2.3.4".into(), 9092)],
                output: "1.2.3.4:9092",
            },
            TestCase {
                input: "localhost:42",
                addrs: vec![("localhost".into(), 42)],
                output: "localhost:42",
            },
            TestCase {
                input: "1.2.3.4:1234",
                addrs: vec![("1.2.3.4".into(), 1234)],
                output: "1.2.3.4:1234",
            },
            TestCase {
                input: "host1,host2",
                addrs: vec![("host1".into(), 9092), ("host2".into(), 9092)],
                output: "host1:9092,host2:9092",
            },
            TestCase {
                input: "host1:42,host2:42",
                addrs: vec![("host1".into(), 42), ("host2".into(), 42)],
                output: "host1:42,host2:42",
            },
            TestCase {
                input: "host1,host2:42",
                addrs: vec![("host1".into(), 9092), ("host2".into(), 42)],
                output: "host1:9092,host2:42",
            },
        ];

        for tc in test_cases {
            let addrs: KafkaAddrs = tc.input.parse()?;
            assert_eq!(addrs.0, tc.addrs);
            assert_eq!(addrs.to_string(), tc.output);
        }

        Ok(())
    }

    #[test]
    fn test_parse_err() {
        assert_eq!(
            "host:badport"
                .parse::<KafkaAddrs>()
                .unwrap_err()
                .to_string(),
            "unable to parse Kafka broker address: invalid port: invalid digit found in string",
        )
    }
}
