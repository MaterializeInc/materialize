use serde_json::{Map, Value};

pub trait MapHelper {
    fn string(&self, key: &str) -> Option<String>;

    fn name(&self) -> Option<String> {
        self.string("name")
    }

    fn doc(&self) -> Option<String> {
        self.string("doc")
    }
}

impl MapHelper for Map<String, Value> {
    fn string(&self, key: &str) -> Option<String> {
        self.get(key)
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    }
}

pub fn zigzag_i32(n: i32) -> Vec<u8> {
    variable_length(((n << 1) ^ (n >> 31)) as i64)
}

pub fn zigzag_i64(n: i64) -> Vec<u8> {
    variable_length((n << 1) ^ (n >> 31))
}

fn variable_length(mut z: i64) -> Vec<u8> {
    let mut result = Vec::new();

    loop {
        if z <= 0x7F {
            result.push((z & 0x7F) as u8);
            break
        } else {
            result.push((0x80 | (z & 0x7F)) as u8);
            z >>= 7;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zigzag() {
        assert_eq!(zigzag_i32(42i32), zigzag_i64(42i64))
    }
}