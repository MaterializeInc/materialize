// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The eager partition assignors `range` and `roundrobin`, run by the group
//! leader. They produce the same assignments as the Java client's and
//! librdkafka's assignors of the same names, so that groups mixing clients
//! agree.

use std::collections::BTreeMap;

/// A group member's subscription.
#[derive(Debug, Clone)]
pub(super) struct Member {
    pub member_id: String,
    pub topics: Vec<String>,
}

/// Partitions per topic, per member id.
pub(super) type Assignment = BTreeMap<String, BTreeMap<String, Vec<i32>>>;

fn empty(members: &[Member]) -> Assignment {
    members
        .iter()
        .map(|m| (m.member_id.clone(), BTreeMap::new()))
        .collect()
}

/// Assigns each topic's partitions in contiguous ranges to the members
/// subscribed to it, ordered by member id; the first `n % c` members get
/// one extra partition.
pub(super) fn range(members: &[Member], partition_counts: &BTreeMap<String, i32>) -> Assignment {
    let mut assignment = empty(members);
    for (topic, &count) in partition_counts {
        let mut consumers: Vec<&str> = members
            .iter()
            .filter(|m| m.topics.contains(topic))
            .map(|m| m.member_id.as_str())
            .collect();
        if consumers.is_empty() || count <= 0 {
            continue;
        }
        consumers.sort();
        let n = i32::try_from(consumers.len()).expect("member count fits i32");
        let per = count / n;
        let extra = count % n;
        for (i, member) in consumers.into_iter().enumerate() {
            let i = i32::try_from(i).expect("member count fits i32");
            let start = per * i + i.min(extra);
            let len = per + i32::from(i < extra);
            if len > 0 {
                assignment
                    .get_mut(member)
                    .expect("all members present")
                    .insert(topic.clone(), (start..start + len).collect());
            }
        }
    }
    assignment
}

/// Assigns all partitions, ordered by topic and partition, to the members
/// ordered by member id in turn, skipping members not subscribed to the
/// partition's topic.
pub(super) fn round_robin(
    members: &[Member],
    partition_counts: &BTreeMap<String, i32>,
) -> Assignment {
    let mut assignment = empty(members);
    let mut sorted: Vec<&Member> = members.iter().collect();
    sorted.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    if sorted.is_empty() {
        return assignment;
    }
    let mut next = 0;
    for (topic, &count) in partition_counts {
        if !sorted.iter().any(|m| m.topics.contains(topic)) {
            continue;
        }
        for partition in 0..count {
            loop {
                let member = sorted[next % sorted.len()];
                next += 1;
                if member.topics.contains(topic) {
                    assignment
                        .get_mut(&member.member_id)
                        .expect("all members present")
                        .entry(topic.clone())
                        .or_default()
                        .push(partition);
                    break;
                }
            }
        }
    }
    assignment
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, topics: &[&str]) -> Member {
        Member {
            member_id: id.into(),
            topics: topics.iter().map(|t| t.to_string()).collect(),
        }
    }

    fn counts(c: &[(&str, i32)]) -> BTreeMap<String, i32> {
        c.iter().map(|(t, n)| (t.to_string(), *n)).collect()
    }

    fn of<'a>(a: &'a Assignment, member: &str, topic: &str) -> &'a [i32] {
        a[member].get(topic).map_or(&[], |p| p.as_slice())
    }

    #[mz_ore::test]
    fn range_assignor() {
        let members = [member("b", &["t", "u"]), member("a", &["t"])];
        let a = range(&members, &counts(&[("t", 5), ("u", 2)]));
        assert_eq!(of(&a, "a", "t"), &[0, 1, 2]);
        assert_eq!(of(&a, "b", "t"), &[3, 4]);
        assert_eq!(of(&a, "b", "u"), &[0, 1]);
        assert!(of(&a, "a", "u").is_empty());
        // More members than partitions.
        let members = [
            member("a", &["t"]),
            member("b", &["t"]),
            member("c", &["t"]),
        ];
        let a = range(&members, &counts(&[("t", 2)]));
        assert_eq!(of(&a, "a", "t"), &[0]);
        assert_eq!(of(&a, "b", "t"), &[1]);
        assert!(a["c"].is_empty());
    }

    #[mz_ore::test]
    fn round_robin_assignor() {
        let members = [member("b", &["t", "u"]), member("a", &["t", "u"])];
        let a = round_robin(&members, &counts(&[("t", 3), ("u", 2)]));
        assert_eq!(of(&a, "a", "t"), &[0, 2]);
        assert_eq!(of(&a, "b", "t"), &[1]);
        assert_eq!(of(&a, "a", "u"), &[1]);
        assert_eq!(of(&a, "b", "u"), &[0]);
        // Members not subscribed to a topic are skipped.
        let members = [member("a", &["t"]), member("b", &["u"])];
        let a = round_robin(&members, &counts(&[("t", 2), ("u", 1)]));
        assert_eq!(of(&a, "a", "t"), &[0, 1]);
        assert_eq!(of(&a, "b", "u"), &[0]);
    }
}
