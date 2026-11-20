// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The PostgreSQL `inet` and `cidr` network address types.
//!
//! Both SQL types share [`Inet`]. A `cidr` value is an [`Inet`] whose bits
//! right of the mask are zero, which the SQL layer enforces on input and cast.
//! Parsing and formatting are ports of PostgreSQL's `inet_net_pton.c`,
//! `inet_net_ntop.c` and `inet_cidr_ntop.c`, including their quirks, so text
//! round-trips byte-for-byte with PostgreSQL.

use std::cmp::Ordering;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use serde::{Deserialize, Serialize};

/// Width of [`Inet::encode_ordered`]: family, two IPv6 addresses, mask length.
pub const INET_ORDERED_BYTES: usize = 34;

/// PostgreSQL's `PGSQL_AF_INET`, used in the binary wire format.
pub const PGSQL_AF_INET: u8 = 2;
/// PostgreSQL's `PGSQL_AF_INET6`, used in the binary wire format.
pub const PGSQL_AF_INET6: u8 = 3;

/// An IPv4 or IPv6 host address with a netmask length.
///
/// Ordering matches PostgreSQL's `network_cmp_internal`: IPv4 before IPv6,
/// then network part, then mask length, then the full address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Inet {
    addr: IpAddr,
    masklen: u8,
}

/// Errors from [`Inet`] operations. Display strings match PostgreSQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InetError {
    InvalidMaskLength(i32),
    DifferentFamilies(&'static str),
    OutOfRange,
}

impl fmt::Display for InetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InetError::InvalidMaskLength(bits) => write!(f, "invalid mask length: {bits}"),
            InetError::DifferentFamilies(op) => f.write_str(op),
            InetError::OutOfRange => f.write_str("result is out of range"),
        }
    }
}

impl std::error::Error for InetError {}

impl Inet {
    /// Constructs an `Inet`, or `None` if `masklen` exceeds the address width.
    pub fn new(addr: IpAddr, masklen: u8) -> Option<Inet> {
        (masklen <= max_bits(&addr)).then_some(Inet { addr, masklen })
    }

    /// A host address with a full-length mask.
    pub fn host(addr: IpAddr) -> Inet {
        Inet {
            addr,
            masklen: max_bits(&addr),
        }
    }

    pub fn addr(&self) -> IpAddr {
        self.addr
    }

    pub fn masklen(&self) -> u8 {
        self.masklen
    }

    pub fn is_ipv6(&self) -> bool {
        self.addr.is_ipv6()
    }

    pub fn max_masklen(&self) -> u8 {
        max_bits(&self.addr)
    }

    /// 4 for IPv4, 6 for IPv6, as returned by `family()`.
    pub fn family(&self) -> i32 {
        if self.is_ipv6() { 6 } else { 4 }
    }

    fn bits(&self) -> u128 {
        match self.addr {
            IpAddr::V4(a) => u128::from(a.to_bits()),
            IpAddr::V6(a) => a.to_bits(),
        }
    }

    fn with_bits(&self, bits: u128, masklen: u8) -> Inet {
        let addr = match self.addr {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::from_bits(
                u32::try_from(bits).expect("fits in u32"),
            )),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::from_bits(bits)),
        };
        Inet { addr, masklen }
    }

    fn all_ones(&self) -> u128 {
        if self.is_ipv6() {
            u128::MAX
        } else {
            u128::from(u32::MAX)
        }
    }

    fn netmask_bits(&self) -> u128 {
        prefix_mask(self.max_masklen(), self.masklen)
    }

    /// True if no bits right of the mask are set, which `cidr` requires.
    pub fn is_valid_cidr(&self) -> bool {
        self.bits() & !self.netmask_bits() & self.all_ones() == 0
    }

    /// The address with host bits cleared and the same mask length. This is
    /// `network(inet)` and the `inet -> cidr` cast.
    pub fn network(&self) -> Inet {
        self.with_bits(self.bits() & self.netmask_bits(), self.masklen)
    }

    pub fn broadcast(&self) -> Inet {
        self.with_bits(
            self.bits() | (!self.netmask_bits() & self.all_ones()),
            self.masklen,
        )
    }

    pub fn netmask(&self) -> Inet {
        self.with_bits(self.netmask_bits(), self.max_masklen())
    }

    pub fn hostmask(&self) -> Inet {
        self.with_bits(!self.netmask_bits() & self.all_ones(), self.max_masklen())
    }

    /// Validates a mask length argument, mapping -1 to the full width as
    /// PostgreSQL's `set_masklen` does.
    fn check_masklen(&self, bits: i32) -> Result<u8, InetError> {
        let bits = if bits == -1 {
            i32::from(self.max_masklen())
        } else {
            bits
        };
        match u8::try_from(bits) {
            Ok(b) if b <= self.max_masklen() => Ok(b),
            _ => Err(InetError::InvalidMaskLength(bits)),
        }
    }

    /// `set_masklen(inet, int)`: keeps the host bits.
    pub fn set_masklen(&self, bits: i32) -> Result<Inet, InetError> {
        let masklen = self.check_masklen(bits)?;
        Ok(Inet {
            addr: self.addr,
            masklen,
        })
    }

    /// `set_masklen(cidr, int)`: clears the bits right of the new mask.
    pub fn set_masklen_cidr(&self, bits: i32) -> Result<Inet, InetError> {
        let masklen = self.check_masklen(bits)?;
        Ok(Inet {
            addr: self.addr,
            masklen,
        }
        .network())
    }

    /// True if the first `n` bits of both addresses are equal.
    fn prefix_eq(&self, other: &Inet, n: u8) -> bool {
        let mask = prefix_mask(self.max_masklen(), n);
        self.bits() & mask == other.bits() & mask
    }

    /// `self << other`: `self` is strictly contained in `other`.
    pub fn is_strict_subnet_of(&self, other: &Inet) -> bool {
        self.is_ipv6() == other.is_ipv6()
            && self.masklen > other.masklen
            && self.prefix_eq(other, other.masklen)
    }

    /// `self <<= other`: `self` is contained in or equal to `other`.
    pub fn is_subnet_of(&self, other: &Inet) -> bool {
        self.is_ipv6() == other.is_ipv6()
            && self.masklen >= other.masklen
            && self.prefix_eq(other, other.masklen)
    }

    /// `self && other`: either contains the other.
    pub fn overlaps(&self, other: &Inet) -> bool {
        self.is_ipv6() == other.is_ipv6() && self.prefix_eq(other, self.masklen.min(other.masklen))
    }

    /// `inet_merge`: the smallest network containing both inputs.
    pub fn merge(&self, other: &Inet) -> Result<Inet, InetError> {
        if self.is_ipv6() != other.is_ipv6() {
            return Err(InetError::DifferentFamilies(
                "cannot merge addresses from different families",
            ));
        }
        let max = self.max_masklen();
        let shift = 128 - u32::from(max);
        let diff = ((self.bits() ^ other.bits()) << shift).leading_zeros();
        let common = u8::try_from(diff.min(u32::from(max))).expect("at most 128");
        Ok(Inet {
            addr: self.addr,
            masklen: common.min(self.masklen.min(other.masklen)),
        }
        .network())
    }

    pub fn bitnot(&self) -> Inet {
        self.with_bits(!self.bits() & self.all_ones(), self.masklen)
    }

    pub fn bitand(&self, other: &Inet) -> Result<Inet, InetError> {
        if self.is_ipv6() != other.is_ipv6() {
            return Err(InetError::DifferentFamilies(
                "cannot AND inet values of different sizes",
            ));
        }
        Ok(self.with_bits(self.bits() & other.bits(), self.masklen.max(other.masklen)))
    }

    pub fn bitor(&self, other: &Inet) -> Result<Inet, InetError> {
        if self.is_ipv6() != other.is_ipv6() {
            return Err(InetError::DifferentFamilies(
                "cannot OR inet values of different sizes",
            ));
        }
        Ok(self.with_bits(self.bits() | other.bits(), self.masklen.max(other.masklen)))
    }

    /// `inet + bigint`. Errors if the result leaves the address space.
    pub fn add(&self, addend: i64) -> Result<Inet, InetError> {
        let bits = self
            .bits()
            .checked_add_signed(i128::from(addend))
            .filter(|b| *b <= self.all_ones())
            .ok_or(InetError::OutOfRange)?;
        Ok(self.with_bits(bits, self.masklen))
    }

    /// `inet - bigint`. Negation wraps for `i64::MIN`, as in PostgreSQL.
    pub fn sub_int(&self, subtrahend: i64) -> Result<Inet, InetError> {
        self.add(subtrahend.wrapping_neg())
    }

    /// `inet - inet`. Errors if the difference does not fit in a `bigint`.
    pub fn sub(&self, other: &Inet) -> Result<i64, InetError> {
        if self.is_ipv6() != other.is_ipv6() {
            return Err(InetError::DifferentFamilies(
                "cannot subtract inet values of different sizes",
            ));
        }
        let (a, b) = (self.bits(), other.bits());
        let res = if a >= b {
            i64::try_from(a - b).ok()
        } else {
            i128::try_from(b - a)
                .ok()
                .and_then(|d| i64::try_from(-d).ok())
        };
        res.ok_or(InetError::OutOfRange)
    }

    /// Address bytes in network order: 4 for IPv4, 16 for IPv6.
    fn octets(&self) -> ([u8; 16], usize) {
        let mut buf = [0; 16];
        match self.addr {
            IpAddr::V4(a) => {
                buf[..4].copy_from_slice(&a.octets());
                (buf, 4)
            }
            IpAddr::V6(a) => (a.octets(), 16),
        }
    }

    /// PostgreSQL's binary wire format (`inet_send`/`cidr_send`).
    pub fn encode_pg_binary(&self, is_cidr: bool, buf: &mut Vec<u8>) {
        let (octets, n) = self.octets();
        let family = if self.is_ipv6() {
            PGSQL_AF_INET6
        } else {
            PGSQL_AF_INET
        };
        let n_u8 = u8::try_from(n).expect("4 or 16");
        buf.extend_from_slice(&[family, self.masklen, u8::from(is_cidr), n_u8]);
        buf.extend_from_slice(&octets[..n]);
    }

    /// Decodes PostgreSQL's binary wire format. The `is_cidr` byte is
    /// ignored, as in `network_recv`.
    pub fn decode_pg_binary(buf: &[u8], is_cidr: bool) -> Result<Inet, String> {
        let type_name = if is_cidr { "cidr" } else { "inet" };
        let [family, bits, _is_cidr, nb, rest @ ..] = buf else {
            return Err(format!("invalid external \"{type_name}\" value"));
        };
        let addr = match (*family, *nb, rest.len()) {
            (PGSQL_AF_INET, 4, 4) => IpAddr::V4(Ipv4Addr::new(rest[0], rest[1], rest[2], rest[3])),
            (PGSQL_AF_INET6, 16, 16) => {
                IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(rest).expect("len 16")))
            }
            (PGSQL_AF_INET | PGSQL_AF_INET6, _, _) => {
                return Err(format!("invalid length in external \"{type_name}\" value"));
            }
            _ => {
                return Err(format!(
                    "invalid address family in external \"{type_name}\" value"
                ));
            }
        };
        let inet = Inet::new(addr, *bits)
            .ok_or_else(|| format!("invalid bits in external \"{type_name}\" value"))?;
        if is_cidr && !inet.is_valid_cidr() {
            return Err(
                "invalid external \"cidr\" value: Value has bits set to right of mask".into(),
            );
        }
        Ok(inet)
    }

    /// Length of [`Inet::encode_packed`] for this value.
    pub fn packed_len(&self) -> usize {
        2 + self.octets().1
    }

    /// Compact encoding: `family | masklen | address`, 6 or 18 bytes. The
    /// encoding is the first [`Inet::packed_len`] bytes of the result.
    pub fn encode_packed(&self) -> [u8; 18] {
        let (octets, n) = self.octets();
        let mut buf = [0; 18];
        buf[0] = u8::from(self.is_ipv6());
        buf[1] = self.masklen;
        buf[2..2 + n].copy_from_slice(&octets[..n]);
        buf
    }

    /// Decodes [`Inet::encode_packed`], returning the value and the number of
    /// bytes consumed.
    pub fn decode_packed(buf: &[u8]) -> Result<(Inet, usize), String> {
        let (family, masklen, rest) = match buf {
            [family, masklen, rest @ ..] => (*family, *masklen, rest),
            _ => return Err("packed inet too short".into()),
        };
        let (addr, n) = match family {
            0 if rest.len() >= 4 => (
                IpAddr::V4(Ipv4Addr::new(rest[0], rest[1], rest[2], rest[3])),
                4,
            ),
            1 if rest.len() >= 16 => (
                IpAddr::V6(Ipv6Addr::from(
                    <[u8; 16]>::try_from(&rest[..16]).expect("len 16"),
                )),
                16,
            ),
            _ => return Err(format!("invalid packed inet family {family}")),
        };
        let inet = Inet::new(addr, masklen)
            .ok_or_else(|| format!("invalid packed inet masklen {masklen}"))?;
        Ok((inet, 2 + n))
    }

    /// Order-preserving encoding: `family | network | masklen | address`,
    /// zero-padded to [`INET_ORDERED_BYTES`]. Byte-wise comparison of two
    /// encodings equals [`Ord`] on the values, which persist stats rely on.
    /// The fixed width keeps persist from truncating the stats bounds.
    pub fn encode_ordered(&self) -> [u8; INET_ORDERED_BYTES] {
        let (net, n) = self.network().octets();
        let (addr, _) = self.octets();
        let mut buf = [0; INET_ORDERED_BYTES];
        buf[0] = u8::from(self.is_ipv6());
        buf[1..1 + n].copy_from_slice(&net[..n]);
        buf[1 + n] = self.masklen;
        buf[2 + n..2 + 2 * n].copy_from_slice(&addr[..n]);
        buf
    }

    /// Decodes [`Inet::encode_ordered`].
    pub fn decode_ordered(buf: &[u8]) -> Result<Inet, String> {
        let n = match buf.first() {
            Some(0) if buf.len() == INET_ORDERED_BYTES => 4,
            Some(1) if buf.len() == INET_ORDERED_BYTES => 16,
            _ => {
                return Err(format!(
                    "invalid ordered inet encoding of {} bytes",
                    buf.len()
                ));
            }
        };
        let masklen = buf[1 + n];
        let addr = &buf[2 + n..2 + 2 * n];
        let addr = if n == 4 {
            IpAddr::V4(Ipv4Addr::new(addr[0], addr[1], addr[2], addr[3]))
        } else {
            IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(addr).expect("len 16")))
        };
        Inet::new(addr, masklen).ok_or_else(|| format!("invalid ordered inet masklen {masklen}"))
    }

    /// `inet` text output: the mask is omitted when it is the full width.
    pub fn fmt_inet(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        self.fmt_addr(f)?;
        if self.masklen != self.max_masklen() {
            write!(f, "/{}", self.masklen)?;
        }
        Ok(())
    }

    /// `cidr` output and `text(inet)`: the mask is always printed.
    pub fn fmt_cidr(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        self.fmt_addr(f)?;
        write!(f, "/{}", self.masklen)
    }

    /// `host(inet)`: the address alone.
    pub fn fmt_addr(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        match self.addr {
            IpAddr::V4(a) => write!(f, "{a}"),
            IpAddr::V6(a) => fmt_ipv6(f, &a.octets()),
        }
    }

    /// `abbrev(cidr)`: PostgreSQL's `inet_cidr_ntop`, which drops trailing
    /// zero octets of the network.
    pub fn fmt_cidr_abbrev(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        let (octets, _) = self.octets();
        match self.addr {
            IpAddr::V4(_) => fmt_cidr_abbrev_v4(f, &octets, self.masklen),
            IpAddr::V6(_) => fmt_cidr_abbrev_v6(f, &octets, self.masklen),
        }
    }
}

impl Ord for Inet {
    fn cmp(&self, other: &Inet) -> Ordering {
        // Lexicographic order on (family, network, masklen, address) equals
        // `network_cmp_internal`'s order: comparing networks each masked to
        // their own length agrees with comparing the common prefix, and ties
        // are then broken on mask length and full address in the same order.
        (
            self.is_ipv6(),
            self.network().bits(),
            self.masklen,
            self.bits(),
        )
            .cmp(&(
                other.is_ipv6(),
                other.network().bits(),
                other.masklen,
                other.bits(),
            ))
    }
}

impl PartialOrd for Inet {
    fn partial_cmp(&self, other: &Inet) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Inet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_inet(f)
    }
}

/// One of the `Inet::fmt_*` text renderings.
pub type InetFormatter = fn(&Inet, &mut dyn fmt::Write) -> fmt::Result;

/// Displays an [`Inet`] with a chosen [`InetFormatter`].
#[derive(Debug)]
pub struct InetDisplay(Inet, InetFormatter);

impl fmt::Display for InetDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (self.1)(&self.0, f)
    }
}

impl Inet {
    /// Renders this value with `f`, for example `inet.display(Inet::fmt_cidr)`.
    pub fn display(self, f: InetFormatter) -> InetDisplay {
        InetDisplay(self, f)
    }
}

fn max_bits(addr: &IpAddr) -> u8 {
    match addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    }
}

/// A mask with the top `n` of `width` bits set, right-aligned in a `u128`.
fn prefix_mask(width: u8, n: u8) -> u128 {
    if n == 0 {
        return 0;
    }
    let full = if width == 128 {
        u128::MAX
    } else {
        (1u128 << width) - 1
    };
    (u128::MAX << (width - n)) & full
}

/// Port of PostgreSQL's `inet_net_ntop_ipv6` without the mask suffix. It
/// differs from [`Ipv6Addr`]'s `Display` in printing IPv4-compatible
/// addresses (`::1.2.3.4`) in dotted form.
fn fmt_ipv6(f: &mut dyn fmt::Write, src: &[u8; 16]) -> fmt::Result {
    let words: [u16; 8] = std::array::from_fn(|i| u16::from_be_bytes([src[2 * i], src[2 * i + 1]]));
    let (best_base, best_len) = longest_zero_run(&words);
    let mut i = 0;
    while i < 8 {
        if let Some(base) = best_base {
            if i >= base && i < base + best_len {
                if i == base {
                    f.write_char(':')?;
                }
                i += 1;
                continue;
            }
        }
        if i != 0 {
            f.write_char(':')?;
        }
        if i == 6
            && best_base == Some(0)
            && (best_len == 6
                || (best_len == 7 && words[7] != 0x0001)
                || (best_len == 5 && words[5] == 0xffff))
        {
            write!(f, "{}.{}.{}.{}", src[12], src[13], src[14], src[15])?;
            return Ok(());
        }
        write!(f, "{:x}", words[i])?;
        i += 1;
    }
    if let Some(base) = best_base {
        if base + best_len == 8 {
            f.write_char(':')?;
        }
    }
    Ok(())
}

/// The first longest run of zero words, if it is at least two words long.
fn longest_zero_run(words: &[u16; 8]) -> (Option<usize>, usize) {
    let (mut best, mut best_len) = (None, 0);
    let mut cur: Option<usize> = None;
    for (i, w) in words.iter().enumerate() {
        if *w == 0 {
            cur.get_or_insert(i);
        } else if let Some(base) = cur.take() {
            if best.is_none() || i - base > best_len {
                (best, best_len) = (Some(base), i - base);
            }
        }
    }
    if let Some(base) = cur {
        if best.is_none() || 8 - base > best_len {
            (best, best_len) = (Some(base), 8 - base);
        }
    }
    if best_len < 2 {
        (None, 0)
    } else {
        (best, best_len)
    }
}

fn fmt_cidr_abbrev_v4(f: &mut dyn fmt::Write, src: &[u8; 16], bits: u8) -> fmt::Result {
    let mut wrote = false;
    if bits == 0 {
        f.write_char('0')?;
        wrote = true;
    }
    let whole = usize::from(bits / 8);
    for (i, octet) in src[..whole].iter().enumerate() {
        write!(f, "{octet}")?;
        if i + 1 < whole {
            f.write_char('.')?;
        }
        wrote = true;
    }
    let partial = bits % 8;
    if partial > 0 {
        if wrote {
            f.write_char('.')?;
        }
        let m = (0xffu8 << (8 - partial)) & 0xff;
        write!(f, "{}", src[whole] & m)?;
    }
    write!(f, "/{bits}")
}

/// Port of PostgreSQL's `inet_cidr_ntop_ipv6`, including how it tracks zero
/// runs, which differs from `inet_net_ntop_ipv6`.
fn fmt_cidr_abbrev_v6(f: &mut dyn fmt::Write, src: &[u8; 16], bits: u8) -> fmt::Result {
    if bits == 0 {
        return write!(f, "::/0");
    }
    let mut s = [0u8; 16];
    let p = usize::from(bits).div_ceil(8);
    s[..p].copy_from_slice(&src[..p]);
    if bits % 8 != 0 {
        s[p - 1] &= 0xffu8 << (8 - bits % 8);
    }
    let mut words = usize::from(bits).div_ceil(16);
    if words == 1 {
        words = 2;
    }
    let (mut zero_s, mut zero_l, mut tmp_zero_s, mut tmp_zero_l) = (0, 0, 0, 0);
    for i in 0..words {
        if s[2 * i] | s[2 * i + 1] == 0 {
            if tmp_zero_l == 0 {
                tmp_zero_s = i;
            }
            tmp_zero_l += 1;
        } else if tmp_zero_l != 0 && zero_l < tmp_zero_l {
            zero_s = tmp_zero_s;
            zero_l = tmp_zero_l;
            tmp_zero_l = 0;
        }
    }
    if tmp_zero_l != 0 && zero_l < tmp_zero_l {
        zero_s = tmp_zero_s;
        zero_l = tmp_zero_l;
    }
    let is_ipv4 = zero_l != words
        && zero_s == 0
        && (zero_l == 6
            || (zero_l == 5 && s[10] == 0xff && s[11] == 0xff)
            || (zero_l == 7 && s[14] != 0 && s[15] != 1));
    let mut wrote = false;
    let mut si = 0;
    for p in 0..words {
        if zero_l != 0 && p >= zero_s && p < zero_s + zero_l {
            if p == zero_s {
                f.write_char(':')?;
                wrote = true;
            }
            if p == words - 1 {
                f.write_char(':')?;
                wrote = true;
            }
            si += 2;
            continue;
        }
        if is_ipv4 && p > 5 {
            f.write_char(if p == 6 { ':' } else { '.' })?;
            write!(f, "{}", s[si])?;
            si += 1;
            if p != 7 || bits > 120 {
                write!(f, ".{}", s[si])?;
                si += 1;
            }
        } else {
            if wrote {
                f.write_char(':')?;
            }
            write!(f, "{:x}", u16::from_be_bytes([s[si], s[si + 1]]))?;
            si += 2;
        }
        wrote = true;
    }
    write!(f, "/{bits}")
}

/// Parses `inet` text input. Returns `None` on invalid syntax.
pub fn parse_inet(s: &str) -> Option<Inet> {
    parse(s, false)
}

/// Parses `cidr` text input, accepting PostgreSQL's abbreviated and classful
/// forms. Returns `Err(false)` on invalid syntax and `Err(true)` if bits right
/// of the mask are set.
pub fn parse_cidr(s: &str) -> Result<Inet, bool> {
    let inet = parse(s, true).ok_or(false)?;
    if inet.is_valid_cidr() {
        Ok(inet)
    } else {
        Err(true)
    }
}

/// Port of PostgreSQL's `network_in` dispatch: anything with a colon is IPv6.
fn parse(s: &str, is_cidr: bool) -> Option<Inet> {
    let s = s.as_bytes();
    // The ports below treat NUL as end of input, as the C code does.
    if s.contains(&0) {
        return None;
    }
    if s.contains(&b':') {
        let (addr, bits) = pton_ipv6(s)?;
        let bits = u8::try_from(bits).ok().filter(|b| *b <= 128)?;
        Inet::new(IpAddr::V6(Ipv6Addr::from(addr)), bits)
    } else {
        let (addr, bits) = if is_cidr {
            cidr_pton_ipv4(s)?
        } else {
            net_pton_ipv4(s)?
        };
        let bits = u8::try_from(bits).ok().filter(|b| *b <= 32)?;
        Inet::new(IpAddr::V4(Ipv4Addr::from(addr)), bits)
    }
}

/// A NUL-terminated cursor over input, mirroring the C code's `*src++`.
struct Cursor<'a> {
    s: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(s: &'a [u8]) -> Self {
        Cursor { s, pos: 0 }
    }
    fn peek_at(&self, off: usize) -> u8 {
        self.s.get(self.pos + off).copied().unwrap_or(0)
    }
    fn next(&mut self) -> u8 {
        let c = self.peek_at(0);
        self.pos += 1;
        c
    }
    fn rest(&self) -> &'a [u8] {
        self.s.get(self.pos..).unwrap_or(&[])
    }
}

/// Port of `inet_cidr_pton_ipv4`.
fn cidr_pton_ipv4(src: &[u8]) -> Option<([u8; 4], u32)> {
    let mut dst = [0u8; 4];
    let mut n = 0usize;
    let mut src = Cursor::new(src);
    let mut ch = src.next();
    if ch == b'0' && matches!(src.peek_at(0), b'x' | b'X') && src.peek_at(1).is_ascii_hexdigit() {
        src.next();
        let mut dirty = 0;
        let mut tmp = 0u8;
        loop {
            ch = src.next();
            if ch == 0 || !ch.is_ascii_hexdigit() {
                break;
            }
            let v = hex_val(ch);
            tmp = if dirty == 0 { v } else { (tmp << 4) | v };
            dirty += 1;
            if dirty == 2 {
                *dst.get_mut(n)? = tmp;
                n += 1;
                dirty = 0;
            }
        }
        if dirty != 0 {
            *dst.get_mut(n)? = tmp << 4;
            n += 1;
        }
    } else if ch.is_ascii_digit() {
        loop {
            let mut tmp = 0u32;
            loop {
                tmp = tmp * 10 + u32::from(ch - b'0');
                if tmp > 255 {
                    return None;
                }
                ch = src.next();
                if ch == 0 || !ch.is_ascii_digit() {
                    break;
                }
            }
            *dst.get_mut(n)? = u8::try_from(tmp).expect("<= 255");
            n += 1;
            if ch == 0 || ch == b'/' {
                break;
            }
            if ch != b'.' {
                return None;
            }
            ch = src.next();
            if !ch.is_ascii_digit() {
                return None;
            }
        }
    } else {
        return None;
    }

    let mut bits: Option<u32> = None;
    if ch == b'/' && src.peek_at(0).is_ascii_digit() && n > 0 {
        let mut b = 0u32;
        ch = src.next();
        loop {
            b = b.saturating_mul(10).saturating_add(u32::from(ch - b'0'));
            ch = src.next();
            if ch == 0 || !ch.is_ascii_digit() {
                break;
            }
        }
        if ch != 0 || b > 32 {
            return None;
        }
        bits = Some(b);
    }
    if ch != 0 || n == 0 {
        return None;
    }
    let octets_bits = u32::try_from(n * 8).expect("<= 32");
    let bits = bits.unwrap_or_else(|| {
        let first = dst[0];
        let mut b = match first {
            240.. => 32,
            224.. => 8,
            192.. => 24,
            128.. => 16,
            _ => 8,
        };
        if b < octets_bits {
            b = octets_bits;
        }
        if b == 8 && first == 224 {
            b = 4;
        }
        b
    });
    Some((dst, bits))
}

/// Port of `inet_net_pton_ipv4`, the stricter `inet` parser.
fn net_pton_ipv4(src: &[u8]) -> Option<([u8; 4], u32)> {
    let mut dst = [0u8; 4];
    let mut n = 0usize;
    let mut src = Cursor::new(src);
    let mut ch;
    loop {
        ch = src.next();
        if !ch.is_ascii_digit() {
            break;
        }
        let mut tmp = 0u32;
        loop {
            tmp = tmp * 10 + u32::from(ch - b'0');
            if tmp > 255 {
                return None;
            }
            ch = src.next();
            if ch == 0 || !ch.is_ascii_digit() {
                break;
            }
        }
        *dst.get_mut(n)? = u8::try_from(tmp).expect("<= 255");
        n += 1;
        if ch == 0 || ch == b'/' {
            break;
        }
        if ch != b'.' {
            return None;
        }
    }

    let mut bits: Option<u32> = None;
    if ch == b'/' && src.peek_at(0).is_ascii_digit() && n > 0 {
        let mut b = 0u32;
        ch = src.next();
        loop {
            b = b.saturating_mul(10).saturating_add(u32::from(ch - b'0'));
            ch = src.next();
            if ch == 0 || !ch.is_ascii_digit() {
                break;
            }
        }
        if ch != 0 || b > 32 {
            return None;
        }
        bits = Some(b);
    }
    if ch != 0 {
        return None;
    }
    let bits = match bits {
        Some(b) => b,
        None if n == 4 => 32,
        None => return None,
    };
    if n == 0 || usize::try_from(bits / 8).expect("<= 4") > n {
        return None;
    }
    Some((dst, bits))
}

/// Port of `inet_cidr_pton_ipv6`, used for both `inet` and `cidr`.
fn pton_ipv6(src: &[u8]) -> Option<([u8; 16], u32)> {
    let mut tmp = [0u8; 16];
    let mut tp = 0usize;
    let mut colonp: Option<usize> = None;
    let mut src = Cursor::new(src);
    if src.peek_at(0) == b':' {
        src.next();
        if src.peek_at(0) != b':' {
            return None;
        }
    }
    let mut curtok = src.pos;
    let mut saw_xdigit = false;
    let mut val: u32 = 0;
    let mut digits = 0;
    let mut bits: Option<u32> = None;
    loop {
        let ch = src.next();
        if ch == 0 {
            break;
        }
        if ch.is_ascii_hexdigit() {
            val = (val << 4) | u32::from(hex_val(ch));
            digits += 1;
            if digits > 4 {
                return None;
            }
            saw_xdigit = true;
            continue;
        }
        if ch == b':' {
            curtok = src.pos;
            if !saw_xdigit {
                if colonp.is_some() {
                    return None;
                }
                colonp = Some(tp);
                continue;
            } else if src.peek_at(0) == 0 {
                return None;
            }
            if tp + 2 > 16 {
                return None;
            }
            tmp[tp..tp + 2].copy_from_slice(&u16::try_from(val).expect("4 digits").to_be_bytes());
            tp += 2;
            saw_xdigit = false;
            digits = 0;
            val = 0;
            continue;
        }
        if ch == b'.' && tp + 4 <= 16 {
            let tok = src.s.get(curtok..).unwrap_or(&[]);
            if let Some(b) = getv4(tok, &mut tmp[tp..tp + 4]) {
                bits = b;
                tp += 4;
                saw_xdigit = false;
                break;
            }
        }
        if ch == b'/' {
            if let Some(b) = getbits(src.rest()) {
                bits = Some(b);
                break;
            }
        }
        return None;
    }
    if saw_xdigit {
        if tp + 2 > 16 {
            return None;
        }
        tmp[tp..tp + 2].copy_from_slice(&u16::try_from(val).expect("4 digits").to_be_bytes());
        tp += 2;
    }
    if let Some(colonp) = colonp {
        if tp == 16 {
            return None;
        }
        let n = tp - colonp;
        for i in 1..=n {
            tmp[16 - i] = tmp[colonp + n - i];
            tmp[colonp + n - i] = 0;
        }
        tp = 16;
    }
    if tp != 16 {
        return None;
    }
    Some((tmp, bits.unwrap_or(128)))
}

/// Port of `getbits`: a decimal mask length with no leading zeros, <= 128.
fn getbits(src: &[u8]) -> Option<u32> {
    if src.is_empty() {
        return None;
    }
    let mut val = 0u32;
    for (n, ch) in src.iter().enumerate() {
        if !ch.is_ascii_digit() || (n != 0 && val == 0) {
            return None;
        }
        val = val * 10 + u32::from(ch - b'0');
        if val > 128 {
            return None;
        }
    }
    Some(val)
}

/// Port of `getv4`: an embedded dotted IPv4 address with an optional mask.
/// Returns the mask length if one followed, wrapped in `Some` on success.
fn getv4(src: &[u8], dst: &mut [u8]) -> Option<Option<u32>> {
    let mut d = 0usize;
    let mut val = 0u32;
    let mut n = 0;
    for (i, ch) in src.iter().enumerate() {
        if ch.is_ascii_digit() {
            if n != 0 && val == 0 {
                return None;
            }
            n += 1;
            val = val * 10 + u32::from(ch - b'0');
            if val > 255 {
                return None;
            }
            continue;
        }
        if *ch == b'.' || *ch == b'/' {
            if d > 3 {
                return None;
            }
            dst[d] = u8::try_from(val).expect("<= 255");
            d += 1;
            if *ch == b'/' {
                return getbits(&src[i + 1..]).map(Some);
            }
            val = 0;
            n = 0;
            continue;
        }
        return None;
    }
    if n == 0 || d > 3 {
        return None;
    }
    dst[d] = u8::try_from(val).expect("<= 255");
    Some(None)
}

fn hex_val(ch: u8) -> u8 {
    match ch {
        b'0'..=b'9' => ch - b'0',
        b'a'..=b'f' => ch - b'a' + 10,
        b'A'..=b'F' => ch - b'A' + 10,
        _ => unreachable!("caller checked is_ascii_hexdigit"),
    }
}

#[cfg(any(test, feature = "proptest"))]
mod arbitrary {
    use proptest::prelude::*;

    use super::*;

    impl Arbitrary for Inet {
        type Parameters = ();
        type Strategy = BoxedStrategy<Inet>;

        fn arbitrary_with(_: ()) -> Self::Strategy {
            prop_oneof![
                (any::<u32>(), 0u8..=32).prop_map(|(a, m)| Inet::new(
                    IpAddr::V4(Ipv4Addr::from(a)),
                    m
                )
                .unwrap()),
                (any::<u128>(), 0u8..=128).prop_map(|(a, m)| Inet::new(
                    IpAddr::V6(Ipv6Addr::from(a)),
                    m
                )
                .unwrap()),
            ]
            .boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use proptest::prelude::*;

    use super::*;

    fn inet(s: &str) -> Inet {
        parse_inet(s).unwrap_or_else(|| panic!("invalid inet {s}"))
    }

    fn show(i: &Inet, f: InetFormatter) -> String {
        i.display(f).to_string()
    }

    /// Direct port of `network_cmp_internal`, the reference for [`Ord`].
    fn pg_cmp(a: &Inet, b: &Inet) -> Ordering {
        if a.is_ipv6() != b.is_ipv6() {
            return a.is_ipv6().cmp(&b.is_ipv6());
        }
        let n = a.masklen.min(b.masklen);
        let mask = prefix_mask(a.max_masklen(), n);
        (a.bits() & mask)
            .cmp(&(b.bits() & mask))
            .then(a.masklen.cmp(&b.masklen))
            .then(a.bits().cmp(&b.bits()))
    }

    #[mz_ore::test]
    fn parse_and_format_match_postgres() {
        // Expected values were produced by PostgreSQL 18.
        let cases: &[(&str, bool, Option<&str>)] = &[
            ("10/8", true, Some("10.0.0.0/8")),
            ("192.168", true, Some("192.168.0.0/24")),
            ("224", true, Some("224.0.0.0/4")),
            ("128", true, Some("128.0.0.0/16")),
            ("240", true, Some("240.0.0.0/32")),
            ("10.0/8", true, Some("10.0.0.0/8")),
            ("1:2::/32", true, Some("1:2::/32")),
            ("::1", true, Some("::1/128")),
            ("10.1.2.3/8", true, None),
            ("1:2:3::/32", true, None),
            ("::1.2.3.4", false, Some("::1.2.3.4")),
            ("::ffff:0:0", false, Some("::ffff:0.0.0.0")),
            ("10.1.2.3/8", false, Some("10.1.2.3/8")),
            ("010.001.002.003", false, Some("10.1.2.3")),
            ("1.2.3.4/08", false, Some("1.2.3.4/8")),
            ("FE80::1", false, Some("fe80::1")),
            ("::1/0", false, Some("::1/0")),
            ("10.1/16", false, Some("10.1.0.0/16")),
            ("1.2.3/24", false, Some("1.2.3.0/24")),
            ("10.1", false, None),
            ("10", false, None),
            (" 1.2.3.4 ", false, None),
            ("1.2.3.4/33", false, None),
            ("1.2.3.4/", false, None),
            ("0x0a.1.2.3", false, None),
            ("1.2.3.4 /8", false, None),
            ("1.2.3.256", false, None),
            ("1.2.3.4.5", false, None),
            ("", false, None),
            ("1.2.3.4/-1", false, None),
            ("1.2.3.4/+8", false, None),
        ];
        for (input, is_cidr, expected) in cases {
            let got = if *is_cidr {
                parse_cidr(input).ok().map(|i| show(&i, Inet::fmt_cidr))
            } else {
                parse_inet(input).map(|i| show(&i, Inet::fmt_inet))
            };
            assert_eq!(got.as_deref(), *expected, "input {input:?} cidr={is_cidr}");
        }
    }

    #[mz_ore::test]
    fn abbrev_matches_postgres() {
        let c = parse_cidr("10.1.0.0/16").unwrap();
        assert_eq!(show(&c, Inet::fmt_cidr_abbrev), "10.1/16");
        assert_eq!(show(&inet("10.1.0.0/16"), Inet::fmt_inet), "10.1.0.0/16");
    }

    #[mz_ore::test]
    fn arithmetic_errors() {
        assert_eq!(inet("255.255.255.255").add(1), Err(InetError::OutOfRange));
        assert_eq!(inet("0.0.0.0").add(-1), Err(InetError::OutOfRange));
        assert_ok!(inet("::").add(i64::MAX));
        assert_eq!(inet("::").sub(&inet("ffff::")), Err(InetError::OutOfRange));
        assert_eq!(
            inet("0.0.0.0").sub(&inet("255.255.255.255")),
            Ok(-4294967295)
        );
        assert!(inet("1.2.3.4").bitand(&inet("::1")).is_err());
    }

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn ord_matches_postgres(a: Inet, b: Inet) {
            prop_assert_eq!(a.cmp(&b), pg_cmp(&a, &b));
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn ordered_encoding_preserves_order(a: Inet, b: Inet) {
            let (ea, eb) = (a.encode_ordered(), b.encode_ordered());
            prop_assert_eq!(ea.cmp(&eb), a.cmp(&b));
            prop_assert_eq!(Inet::decode_ordered(&ea), Ok(a));
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn encodings_roundtrip(a: Inet) {
            let buf = a.encode_packed();
            prop_assert_eq!(Inet::decode_packed(&buf), Ok((a, a.packed_len())));

            let mut buf = vec![];
            a.encode_pg_binary(false, &mut buf);
            prop_assert_eq!(Inet::decode_pg_binary(&buf, false), Ok(a));

            prop_assert_eq!(parse_inet(&show(&a, Inet::fmt_inet)), Some(a));
            prop_assert_eq!(parse_inet(&show(&a, Inet::fmt_cidr)), Some(a));
            let net = a.network();
            prop_assert_eq!(parse_cidr(&show(&net, Inet::fmt_cidr)), Ok(net));
        }
    }
}
