// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::refresh_schedule::RefreshSchedule;
use crate::Timestamp;
use mz_proto::{RustType, TryFromProtoError};
use proptest::arbitrary::{any, Arbitrary};
use proptest::prelude::BoxedStrategy;
use proptest::strategy::{Just, Strategy, Union};
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_repr.definity.rs"));

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Indefiniteness {
    /// Potentially valid for all times.
    Definite,
    /// Valid up to a some nested time, rounded according to the refresh schedule.
    RefreshSchedule(Option<RefreshSchedule>, Vec<Self>),
    /// Valid up to the wall-clock time.
    Wallclock,
}

impl Indefiniteness {
    pub fn normalize(&mut self) {
        use Indefiniteness::*;
        match self {
            RefreshSchedule(None, existing) if existing.len() == 1 => {
                *self = existing.remove(0);
            }
            _ => {}
        }
    }

    pub fn unify(&mut self, other: &Self) {
        use Indefiniteness::*;
        match (self, other) {
            (Definite, Definite) => {}
            (this @ Definite, inner @ RefreshSchedule(_, _)) => {
                *this = RefreshSchedule(None, vec![inner.clone()]);
            }
            (this, Wallclock) => *this = Wallclock,
            (RefreshSchedule(_, existing), inner @ RefreshSchedule(_, _)) => {
                existing.push(inner.clone());
            }
            (RefreshSchedule(_, _), Definite) => {}
            (Wallclock, _) => {}
        }
    }

    pub fn apply(&self, wall_clock: Timestamp) -> Option<Timestamp> {
        println!("apply: {:?} {:?}", self, wall_clock);
        let result = match self {
            Indefiniteness::Definite => None,
            Indefiniteness::RefreshSchedule(schedule, inner) => {
                let result = inner.iter().map(|inner| inner.apply(wall_clock)).min()??;
                if let Some(schedule) = schedule {
                    schedule
                        .round_up_timestamp(result)
                        .as_ref()
                        .and_then(Timestamp::try_step_forward)
                } else {
                    Some(result)
                }
            }
            Indefiniteness::Wallclock => Some(wall_clock),
        };
        println!("result: {:?}", result);
        result
    }
}

impl RustType<ProtoDefinity> for Indefiniteness {
    fn into_proto(&self) -> ProtoDefinity {
        ProtoDefinity {
            kind: Some(match self {
                Indefiniteness::Definite => proto_definity::Kind::Definite(()),
                Indefiniteness::RefreshSchedule(schedule, inner) => {
                    proto_definity::Kind::RefreshSchedule(ProtoRefreshSchedule {
                        refresh_schedule: schedule.as_ref().map(|s| s.into_proto()),
                        definity: inner.into_proto(),
                    })
                }
                Indefiniteness::Wallclock => proto_definity::Kind::Wallclock(()),
            }),
        }
    }

    fn from_proto(proto: ProtoDefinity) -> Result<Self, TryFromProtoError> {
        let inner = match proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoDefinity::kind"))?
        {
            proto_definity::Kind::Definite(()) => Indefiniteness::Definite,
            proto_definity::Kind::RefreshSchedule(ProtoRefreshSchedule {
                refresh_schedule,
                definity,
            }) => Indefiniteness::RefreshSchedule(
                refresh_schedule
                    .map(|s| RefreshSchedule::from_proto(s))
                    .transpose()?,
                definity
                    .into_iter()
                    .map(Indefiniteness::from_proto)
                    .collect::<Result<_, _>>()?,
            ),
            proto_definity::Kind::Wallclock(()) => Indefiniteness::Wallclock,
        };
        Ok(inner)
    }
}

impl Arbitrary for Indefiniteness {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
            Just(Indefiniteness::Definite).boxed(),
            any::<RefreshSchedule>()
                .prop_map(|s| {
                    Indefiniteness::RefreshSchedule(Some(s), vec![Indefiniteness::Wallclock])
                })
                .boxed(),
            Just(Indefiniteness::Wallclock).boxed(),
        ])
        .boxed()
    }
}
