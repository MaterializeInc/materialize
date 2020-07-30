// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Represents the address of a Kafka broker.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaAddr {
    host: String,
    port: u16,
}

impl FromStr for KafkaAddr {
    type Err = KafkaAddrParseError;

    fn from_str(s: &str) -> Result<KafkaAddr, Self::Err> {
        let mut parts = s.splitn(2, ':');
        let host = parts.next().unwrap();
        let port = match parts.next() {
            None => 9092,
            Some(port) => port.parse().map_err(KafkaAddrParseError::InvalidPort)?,
        };
        Ok(KafkaAddr {
            host: host.to_owned(),
            port,
        })
    }
}

impl fmt::Display for KafkaAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

#[derive(Clone, Debug)]
pub enum KafkaAddrParseError {
    InvalidPort(ParseIntError),
}

impl fmt::Display for KafkaAddrParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KafkaAddrParseError::InvalidPort(e) => write!(
                f,
                "unable to parse Kafka broker address: invalid port: {}",
                e
            ),
        }
    }
}

impl Error for KafkaAddrParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ok() -> Result<(), Box<dyn Error>> {
        let test_cases = vec![
            ("localhost", ("localhost", 9092)),
            ("foohost", ("foohost", 9092)),
            ("1.2.3.4", ("1.2.3.4", 9092)),
            ("localhost:42", ("localhost", 42)),
            ("1.2.3.4:1234", ("1.2.3.4", 1234)),
        ];

        for (input, (host, port)) in test_cases {
            let addr: KafkaAddr = input.parse()?;
            assert_eq!(addr.host, host);
            assert_eq!(addr.port, port);
        }

        Ok(())
    }

    #[test]
    fn test_parse_err() {
        assert_eq!(
            "host:badport".parse::<KafkaAddr>().unwrap_err().to_string(),
            "unable to parse Kafka broker address: invalid port: invalid digit found in string",
        )
    }
}
