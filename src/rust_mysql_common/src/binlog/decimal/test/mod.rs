// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use proptest::prelude::*;

use std::fmt;

use self::libstrings_bindings::*;

// Use `build.rs` to rebuild this.
#[allow(non_camel_case_types)]
// this is due to rust-lang/rust-bindgen#1651
#[allow(deref_nullptr)]
mod libstrings_bindings;

impl fmt::Debug for decimal_t {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("decimal_t")
            .field("intg", &self.intg)
            .field("frac", &self.frac)
            .field("len", &self.len)
            .field("sign", &self.sign)
            .field("buf", &self.as_ref())
            .finish()
    }
}

impl decimal_t {
    pub fn new() -> Self {
        let buf = vec![0x80000000_u32 as i32; 16];
        let mut buf = buf.into_boxed_slice();
        let buf_ptr = buf.as_mut_ptr();
        std::mem::forget(buf);
        Self {
            intg: 0,
            frac: 0,
            len: 16,
            sign: false,
            buf: buf_ptr,
        }
    }

    pub fn as_ref(&self) -> &[i32] {
        unsafe { std::slice::from_raw_parts(self.buf, self.len as usize) }
    }

    pub fn rust_string2decimal(string: &str) -> Result<decimal_t, i32> {
        let mut to = Self::new();
        let result = unsafe {
            let end = &mut (string.as_ptr().add(string.len()) as *const i8);
            c_string2decimal(string.as_ptr() as *const i8, &mut to, end)
        };
        to.len = slice_end(to.as_ref(), 0x80000000_u32 as i32).len() as i32;
        if result == 0 {
            Ok(to)
        } else {
            Err(result)
        }
    }

    pub fn rust_decimal2bin(&self, to: &mut [u8]) -> Result<(), i32> {
        let result =
            unsafe { c_decimal2bin(self, to.as_mut_ptr(), self.intg + self.frac, self.frac) };
        if result == 0 {
            Ok(())
        } else {
            Err(result)
        }
    }

    pub fn rust_bin2decimal(
        from: &[u8],
        precision: i32,
        scale: i32,
        keep_prec: bool,
    ) -> Result<Self, i32> {
        let mut to = Self::new();
        let result = unsafe { c_bin2decimal(from.as_ptr(), &mut to, precision, scale, keep_prec) };
        to.len = slice_end(to.as_ref(), 0x80000000_u32 as i32).len() as i32;
        if result == 0 {
            Ok(to)
        } else {
            Err(result)
        }
    }

    pub fn rust_decimal2string(&self, to: &mut [u8]) -> Result<usize, i32> {
        let mut out_len = to.len() as i32;
        let result = unsafe { c_decimal2string(self, to.as_mut_ptr() as *mut _, &mut out_len) };
        if result == 0 {
            Ok(out_len as usize)
        } else {
            Err(result)
        }
    }
}

fn slice_end<T: PartialEq>(s: &[T], x: T) -> &[T] {
    let end = s.iter().position(|y| *y == x).unwrap_or(s.len());
    &s[..end]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1 << 14, .. ProptestConfig::default()
    })]
    #[test]
    fn decimal_roundtrip(
        sign: bool,
        frac: bool,
        point in 0_usize..65,
        num in r"[0-9]{1,65}",
    ) {
        let num = {
            let mut out = String::new();
            if sign {
                out.push('-');
            }
            if frac {
                let (integral, fractional) = num.split_at(std::cmp::min(point, num.len()));
                out.push_str(integral);
                out.push('.');
                out.push_str(fractional);
            } else {
                out.push_str(&num);
            }
            out
        };

        let num = dbg!(&num);

        // test string2decimal
        let dec = dbg!(super::Decimal::parse_bytes(num.as_bytes()).unwrap());
        let mysql_dec = dbg!(decimal_t::rust_string2decimal(num).unwrap());
        assert_eq!(dec.intg, mysql_dec.intg as usize);
        assert_eq!(dec.frac, mysql_dec.frac as usize);
        assert_eq!(dec.sign, mysql_dec.sign);
        assert_eq!(dec.buf, mysql_dec.as_ref());

        // test decimal2bin
        let mut bin = Vec::new();
        let mut mysql_bin = vec![0b11111111_u8; 32];
        dec.write_bin(&mut bin).unwrap();
        let size = unsafe {
            c_decimal_bin_size(mysql_dec.intg + mysql_dec.frac, mysql_dec.frac) as usize
        };
        mysql_dec.rust_decimal2bin(&mut mysql_bin).unwrap();
        assert_eq!(dbg!(&bin[..]), dbg!(&mysql_bin[..size]));

        // test bin2decimal
        let dec2 = dbg!(super::Decimal::read_bin(&bin[..], dec.intg + dec.frac, dec.frac, false).unwrap());
        let mysql_dec2 = dbg!(decimal_t::rust_bin2decimal(
            &mysql_bin,
            mysql_dec.intg + mysql_dec.frac,
            mysql_dec.frac,
            false,
        )
        .unwrap());
        assert_eq!(dec2.intg, mysql_dec2.intg as usize);
        assert_eq!(dec2.frac, mysql_dec2.frac as usize);
        assert_eq!(dec2.sign, mysql_dec2.sign);
        assert_eq!(dec2.buf, mysql_dec2.as_ref());

        // test decimal2string
        let string = dec2.to_string();
        let mut mysql_string = vec![0_u8; 128];
        let out_len = mysql_dec2.rust_decimal2string(&mut mysql_string).unwrap();
        assert_eq!(
            string,
            std::str::from_utf8(&mysql_string[..out_len]).unwrap(),
        );

        // test bin2decimal keep_prec
        let dec2 = dbg!(super::Decimal::read_bin(&bin[..], dec.intg + dec.frac, dec.frac, true).unwrap());
        let mysql_dec2 = dbg!(decimal_t::rust_bin2decimal(
            &mysql_bin,
            mysql_dec.intg + mysql_dec.frac,
            mysql_dec.frac,
            true,
        )
        .unwrap());
        assert_eq!(dec2.intg, mysql_dec2.intg as usize);
        assert_eq!(dec2.frac, mysql_dec2.frac as usize);
        assert_eq!(dec2.sign, mysql_dec2.sign);
        assert_eq!(dec2.buf, mysql_dec2.as_ref());

        assert_eq!(dec, dec2);
    }
}
