// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! librdkafka's built-in partitioners.
//!
//! The hashes and the null-key and empty-key handling match
//! `rd_kafka_msg_partitioner_*` in librdkafka's `rdkafka_msg.c` exactly, so
//! keyed messages land on the same partitions as with librdkafka (and, for
//! `murmur2_random`, as with the Java producer's default partitioner).
//!
//! librdkafka does not call the partitioner for every message without an
//! explicit partition: with sticky partitioning enabled, keyless messages go to
//! a per-topic sticky partition instead. [`Partitioner::is_sticky`] and
//! [`StickyPartition`] model that.

use std::str::FromStr;
use std::time::{Duration, Instant};

use mz_ore::cast::{CastFrom, ReinterpretCast};
use rand::RngExt;

use crate::internal::compression::CodecError;

/// A librdkafka `partitioner` setting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum Partitioner {
    /// Random distribution.
    Random,
    /// CRC32 of the key. Null and empty keys go to partition 0.
    Consistent,
    /// CRC32 of the key. Null and empty keys are distributed randomly.
    #[default]
    ConsistentRandom,
    /// Java-compatible murmur2 of the key. Null keys hash like empty keys.
    Murmur2,
    /// Java-compatible murmur2 of the key. Null keys are distributed randomly,
    /// empty keys are hashed. This is the Java producer's default partitioner
    /// for keyed messages.
    Murmur2Random,
    /// FNV-1a of the key, compatible with Sarama's `NewHashPartitioner`. Null
    /// keys hash like empty keys.
    Fnv1a,
    /// FNV-1a of the key. Null keys are distributed randomly, empty keys are
    /// hashed.
    Fnv1aRandom,
}

impl Partitioner {
    const ALL: [Partitioner; 7] = [
        Partitioner::Random,
        Partitioner::Consistent,
        Partitioner::ConsistentRandom,
        Partitioner::Murmur2,
        Partitioner::Murmur2Random,
        Partitioner::Fnv1a,
        Partitioner::Fnv1aRandom,
    ];

    /// The librdkafka name of the partitioner.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Partitioner::Random => "random",
            Partitioner::Consistent => "consistent",
            Partitioner::ConsistentRandom => "consistent_random",
            Partitioner::Murmur2 => "murmur2",
            Partitioner::Murmur2Random => "murmur2_random",
            Partitioner::Fnv1a => "fnv1a",
            Partitioner::Fnv1aRandom => "fnv1a_random",
        }
    }

    #[cfg(test)]
    fn partition(&self, key: Option<&[u8]>, partition_cnt: i32) -> i32 {
        self.partition_with(key, partition_cnt, |_| true)
    }

    /// Picks a partition for `key` among `partition_cnt` partitions. Returns
    /// -1 (librdkafka's unassigned partition) if `partition_cnt` is not
    /// positive.
    ///
    /// `is_available` says whether a partition currently has a leader. Only
    /// random choices consult it: like librdkafka, an unavailable random pick
    /// is replaced by a second, unchecked pick.
    pub(crate) fn partition_with(
        &self,
        key: Option<&[u8]>,
        partition_cnt: i32,
        is_available: impl Fn(i32) -> bool,
    ) -> i32 {
        let Ok(cnt) = u32::try_from(partition_cnt) else {
            return -1;
        };
        if cnt == 0 {
            return -1;
        }
        let modulo = |hash: u32| i32::try_from(hash % cnt).expect("less than partition_cnt");
        let bytes = key.unwrap_or_default();
        match self {
            Partitioner::Random => random_partition(partition_cnt, is_available),
            Partitioner::Consistent => modulo(crc32fast::hash(bytes)),
            Partitioner::ConsistentRandom if bytes.is_empty() => {
                random_partition(partition_cnt, is_available)
            }
            Partitioner::ConsistentRandom => modulo(crc32fast::hash(bytes)),
            Partitioner::Murmur2Random | Partitioner::Fnv1aRandom if key.is_none() => {
                random_partition(partition_cnt, is_available)
            }
            Partitioner::Murmur2 | Partitioner::Murmur2Random => {
                modulo(murmur2(bytes) & 0x7fff_ffff)
            }
            Partitioner::Fnv1a | Partitioner::Fnv1aRandom => modulo(fnv1a(bytes)),
        }
    }

    /// Whether librdkafka assigns a message with this key to the topic's
    /// [`StickyPartition`] instead of calling the partitioner.
    ///
    /// `sticky_linger` is `sticky.partitioning.linger.ms`, and zero disables
    /// sticky partitioning. NOTE: librdkafka defaults it to `min(2 * linger.ms, 900s)`
    /// when it is not set explicitly.
    pub(crate) fn is_sticky(&self, key: Option<&[u8]>, sticky_linger: Duration) -> bool {
        // librdkafka's `random_partitioner` flag is false exactly for these.
        let sticky_capable = !sticky_linger.is_zero()
            && !matches!(
                self,
                Partitioner::Consistent | Partitioner::Murmur2 | Partitioner::Fnv1a
            );
        let empty_key_sticks =
            *self == Partitioner::ConsistentRandom && key.is_some_and(|k| k.is_empty());
        sticky_capable && (key.is_none() || empty_key_sticks)
    }
}

impl FromStr for Partitioner {
    type Err = CodecError;

    /// Parses a librdkafka `partitioner` value. Like librdkafka, names are
    /// case-sensitive.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Partitioner::ALL
            .into_iter()
            .find(|p| p.name() == s)
            .ok_or_else(|| CodecError::InvalidPartitionerName(s.to_owned()))
    }
}

/// A topic's sticky partition for keyless messages, as in librdkafka's
/// `rd_kafka_msg_sticky_partition`.
#[derive(Debug, Default)]
pub(crate) struct StickyPartition {
    current: Option<(i32, Instant)>,
}

impl StickyPartition {
    /// Returns the sticky partition, first switching to a new random one if
    /// more than `linger` has passed since the last switch or the current one
    /// is unavailable.
    pub(crate) fn partition(
        &mut self,
        partition_cnt: i32,
        linger: Duration,
        now: Instant,
        is_available: impl Fn(i32) -> bool,
    ) -> i32 {
        if partition_cnt <= 0 {
            return -1;
        }
        match self.current {
            Some((partition, since))
                if partition < partition_cnt
                    && is_available(partition)
                    && now.saturating_duration_since(since) <= linger =>
            {
                partition
            }
            _ => {
                let partition = random_partition(partition_cnt, is_available);
                self.current = Some((partition, now));
                partition
            }
        }
    }
}

/// librdkafka's `rd_kafka_msg_partitioner_random`: a uniformly random
/// partition, re-drawn once without checking if the first draw is unavailable.
fn random_partition(partition_cnt: i32, is_available: impl Fn(i32) -> bool) -> i32 {
    let mut rng = rand::rng();
    let p = rng.random_range(0..partition_cnt);
    if is_available(p) {
        p
    } else {
        rng.random_range(0..partition_cnt)
    }
}

/// The murmur2 variant of Kafka's Java `Utils.murmur2` (and librdkafka's
/// `rd_murmur2`), before the sign bit is masked off.
pub(crate) fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    // Like the C and Java implementations, only the low 32 bits of the length
    // enter the hash.
    let len = u32::try_from(u64::cast_from(data.len()) & 0xffff_ffff).expect("masked to 32 bits");
    let mut h = SEED ^ len;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes(chunk.try_into().expect("4 bytes"));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    let tail = chunks.remainder();
    if tail.len() >= 3 {
        h ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        h ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        h ^= u32::from(tail[0]);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

/// librdkafka's `rd_fnv1a`: 32-bit FNV-1a computed in signed arithmetic and
/// then made non-negative, to match Sarama.
pub(crate) fn fnv1a(data: &[u8]) -> u32 {
    const PRIME: i32 = 0x0100_0193;
    let mut h = i32::reinterpret_cast(0x811c_9dc5_u32);
    for b in data {
        h ^= i32::from(*b);
        h = h.wrapping_mul(PRIME);
    }
    // NOTE: like the C code (where it is undefined behavior that wraps in
    // practice), i32::MIN stays i32::MIN, i.e. 0x8000_0000.
    u32::reinterpret_cast(h.wrapping_abs())
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    /// Keys from librdkafka's `unittest_murmur2` and `unittest_fnv1a`. The
    /// "+n" variants there exercise unaligned reads, which do not exist here,
    /// but the expected hashes remain valid test vectors.
    const UNALIGNED: &str = "PreAmbleWillBeRemoved,ThePrePartThatIs";
    fn unittest_keys() -> Vec<&'static [u8]> {
        vec![
            b"kafka",
            b"giberish123456789",
            b"1234",
            b"234",
            b"34",
            b"4",
            UNALIGNED.as_bytes(),
            &UNALIGNED.as_bytes()[1..],
            &UNALIGNED.as_bytes()[2..],
            &UNALIGNED.as_bytes()[3..],
            b"",
        ]
    }

    #[mz_ore::test]
    fn murmur2_librdkafka_unittest_vectors() {
        let expected: [u32; 11] = [
            0xd067cf64, 0x8f552b0c, 0x9fc97b14, 0xe7c009ca, 0x873930da, 0x5a4b5ca1, 0x78424f1c,
            0x4a62b377, 0xe0e4e09e, 0x62b8b43f, 0x106e08d9,
        ];
        for (key, expected) in unittest_keys().into_iter().zip_eq(expected) {
            assert_eq!(murmur2(key), expected, "{:?}", String::from_utf8_lossy(key));
        }
    }

    /// Vectors from Kafka's Java `UtilsTest.testMurmur2`.
    #[mz_ore::test]
    fn murmur2_java_vectors() {
        let cases: [(&[u8], i32); 6] = [
            (b"21", -973932308),
            (b"foobar", -790332482),
            (b"a-little-bit-long-string", -985981536),
            (b"a-little-bit-longer-string", -1486304829),
            (
                b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
                -58897971,
            ),
            (b"abc", 479470107),
        ];
        for (key, expected) in cases {
            assert_eq!(
                i32::reinterpret_cast(murmur2(key)),
                expected,
                "{:?}",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[mz_ore::test]
    fn fnv1a_librdkafka_unittest_vectors() {
        let expected: [u32; 11] = [
            0x0d33c4e1, 0x77a58295, 0x023bdd03, 0x2dea3cd2, 0x740fa83e, 0x310ca263, 0x65cbd69c,
            0x6e49c79a, 0x69eed356, 0x6abcc023, 0x7ee3623b,
        ];
        for (key, expected) in unittest_keys().into_iter().zip_eq(expected) {
            assert_eq!(fnv1a(key), expected, "{:?}", String::from_utf8_lossy(key));
        }
    }

    /// Expectations from librdkafka's `tests/0048-partitioner.c`, where -1
    /// means "random".
    #[mz_ore::test]
    fn librdkafka_0048_partitioner_expectations() {
        let keys: [Option<&[u8]>; 5] = [
            None,
            Some(b""),
            Some(b"23456"),
            Some(b"this is another string with more length to it perhaps"),
            Some(b"hejsan"),
        ];
        for part_cnt in [7_u32, 17] {
            let m = |h: u32| i32::try_from(h % part_cnt).unwrap();
            let cases: [(Partitioner, [i32; 5]); 7] = [
                (Partitioner::Random, [-1, -1, -1, -1, -1]),
                (
                    Partitioner::Consistent,
                    [m(0), m(0), m(0xb1b451d7), m(0xb0150df7), m(0xd077037e)],
                ),
                (
                    Partitioner::ConsistentRandom,
                    [-1, -1, m(0xb1b451d7), m(0xb0150df7), m(0xd077037e)],
                ),
                (
                    Partitioner::Murmur2,
                    [
                        m(0x106e08d9),
                        m(0x106e08d9),
                        m(0x058d780f),
                        m(0x4f7703da),
                        m(0x5ec19395),
                    ],
                ),
                (
                    Partitioner::Murmur2Random,
                    [
                        -1,
                        m(0x106e08d9),
                        m(0x058d780f),
                        m(0x4f7703da),
                        m(0x5ec19395),
                    ],
                ),
                (
                    Partitioner::Fnv1a,
                    [
                        m(0x7ee3623b),
                        m(0x7ee3623b),
                        m(0x27e6f469),
                        m(0x155e3e5f),
                        m(0x17b1e27a),
                    ],
                ),
                (
                    Partitioner::Fnv1aRandom,
                    [
                        -1,
                        m(0x7ee3623b),
                        m(0x27e6f469),
                        m(0x155e3e5f),
                        m(0x17b1e27a),
                    ],
                ),
            ];
            let cnt = i32::try_from(part_cnt).unwrap();
            for (partitioner, expected) in cases {
                for (key, expected) in keys.into_iter().zip_eq(expected) {
                    for _ in 0..20 {
                        let p = partitioner.partition(key, cnt);
                        if expected == -1 {
                            assert!((0..cnt).contains(&p));
                        } else {
                            assert_eq!(p, expected, "{partitioner:?} {key:?} {part_cnt}");
                        }
                    }
                }
            }
        }
    }

    /// Partitions that librdkafka 2.12.1 (each hashing partitioner) and the
    /// Java producer (default partitioner, keyed records) picked among 17,
    /// recorded from delivery reports and a consumer against Kafka 4.2. Lines
    /// are `<partitioner or "java"> <key as hex, "-" if empty> <partition>`.
    #[mz_ore::test]
    fn matches_librdkafka_and_java() {
        let data = include_str!("partitioner/testdata/partitions.txt");
        let mut checked = std::collections::BTreeMap::<&str, usize>::new();
        for line in data.lines() {
            let [name, key, partition] = line.split(' ').collect::<Vec<_>>()[..] else {
                panic!("bad line {line}");
            };
            let key = if key == "-" {
                vec![]
            } else {
                (0..key.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&key[i..i + 2], 16).unwrap())
                    .collect()
            };
            let partitioner = match name {
                "java" => Partitioner::Murmur2Random,
                name => name.parse().unwrap(),
            };
            assert_eq!(
                partitioner.partition(Some(&key), 17),
                partition.parse::<i32>().unwrap(),
                "{line}"
            );
            *checked.entry(name).or_default() += 1;
        }
        assert_eq!(checked.len(), 7);
        assert!(checked.values().all(|n| *n >= 40), "{checked:?}");
    }

    #[mz_ore::test]
    fn random_is_spread() {
        let seen: std::collections::BTreeSet<i32> = (0..1000)
            .map(|_| Partitioner::Random.partition(Some(b"k"), 4))
            .collect();
        assert_eq!(seen.len(), 4);
    }

    #[mz_ore::test]
    fn random_redraws_unavailable_partition() {
        // With partition 0 unavailable, the second draw is unchecked, so 0
        // still shows up but less often than the 1/4 of a single draw.
        let zeros = (0..20_000)
            .filter(|_| Partitioner::Random.partition_with(None, 4, |p| p != 0) == 0)
            .count();
        assert!((500..2_000).contains(&zeros), "{zeros}");
    }

    #[mz_ore::test]
    fn no_partitions() {
        for p in Partitioner::ALL {
            assert_eq!(p.partition(Some(b"k"), 0), -1);
            assert_eq!(p.partition(None, -5), -1);
        }
        assert_eq!(
            StickyPartition::default().partition(0, Duration::ZERO, Instant::now(), |_| true),
            -1
        );
    }

    #[mz_ore::test]
    fn names() {
        for p in Partitioner::ALL {
            assert_eq!(p.name().parse::<Partitioner>(), Ok(p));
        }
        assert_eq!(Partitioner::default(), Partitioner::ConsistentRandom);
        let err = "Murmur2".parse::<Partitioner>().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid value for configuration property \"partitioner\": Murmur2"
        );
    }

    #[mz_ore::test]
    fn sticky_rules() {
        let linger = Duration::from_millis(10);
        let empty: Option<&[u8]> = Some(b"");
        let key: Option<&[u8]> = Some(b"k");
        for p in Partitioner::ALL {
            let hashing_only = matches!(
                p,
                Partitioner::Consistent | Partitioner::Murmur2 | Partitioner::Fnv1a
            );
            assert_eq!(p.is_sticky(None, linger), !hashing_only, "{p:?}");
            assert!(!p.is_sticky(None, Duration::ZERO), "{p:?}");
            assert!(!p.is_sticky(key, linger), "{p:?}");
            assert_eq!(
                p.is_sticky(empty, linger),
                p == Partitioner::ConsistentRandom,
                "{p:?}"
            );
        }
    }

    #[mz_ore::test]
    fn sticky_partition_lingers() {
        let linger = Duration::from_millis(10);
        let t0 = Instant::now();
        let mut sticky = StickyPartition::default();
        let first = sticky.partition(1000, linger, t0, |_| true);
        assert_eq!(sticky.partition(1000, linger, t0 + linger, |_| true), first);
        // Unavailable: switch immediately.
        let other = sticky.partition(1000, linger, t0 + linger, |p| p != first);
        assert!((0..1000).contains(&other));
        // After the linger time a new partition is drawn (it may equal the
        // old one by chance, so only check the switch time moved).
        let t1 = t0 + linger * 3;
        let next = sticky.partition(1000, linger, t1, |_| true);
        assert_eq!(sticky.partition(1000, linger, t1 + linger, |_| true), next);
    }
}
