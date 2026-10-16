//! Standalone byte formats exposed by `datalevin.bits`.

use std::fmt;

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidBase64 {
    TooShort,
    Character(i8),
    WrongEnding,
    InsufficientBits,
    TrailingInput(usize),
}

impl fmt::Display for InvalidBase64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort => {
                f.write_str("Input byte[] should at least have 2 bytes for base64 bytes")
            }
            Self::Character(byte) => {
                let byte = i16::from(*byte);
                if byte < 0 {
                    write!(f, "Illegal base64 character -{:x}", -byte)
                } else {
                    write!(f, "Illegal base64 character {byte:x}")
                }
            }
            Self::WrongEnding => f.write_str("Input byte array has wrong 4-byte ending unit"),
            Self::InsufficientBits => f.write_str("Last unit does not have enough valid bits"),
            Self::TrailingInput(index) => {
                write!(f, "Input byte array has incorrect ending byte at {index}")
            }
        }
    }
}

impl std::error::Error for InvalidBase64 {}

/// Standard Base64 alphabet, without padding, as used by Datalevin's reader tags.
pub fn encode_base64(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let word = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for shift in [18, 12, 6, 0].into_iter().take(chunk.len() + 1) {
            encoded.push(BASE64[((word >> shift) & 63) as usize] as char);
        }
    }
    encoded
}

/// Accept padded or unpadded basic Base64, including unused trailing bits.
/// Whitespace and the URL-safe alphabet are rejected, matching the JVM decoder.
pub fn decode_base64(encoded: &str) -> Result<Vec<u8>, InvalidBase64> {
    // Base64.Decoder.decode(String) converts through ISO-8859-1, replacing
    // unrepresentable characters with '?', before checking length and syntax.
    let input: Vec<u8> = encoded
        .chars()
        .map(|c| u8::try_from(u32::from(c)).unwrap_or(b'?'))
        .collect();
    if input.len() == 1 {
        return Err(InvalidBase64::TooShort);
    }
    let mut bytes = Vec::with_capacity(input.len() / 4 * 3 + 2);
    let mut word = 0u32;
    let mut bits = 0;
    let mut sextets = 0;
    let mut index = 0;
    while index < input.len() {
        let byte = input[index];
        index += 1;
        if byte == b'=' {
            if sextets == 0 || (sextets == 2 && input.get(index) != Some(&b'=')) {
                return Err(InvalidBase64::WrongEnding);
            }
            if sextets == 2 {
                index += 1;
            }
            if sextets == 1 {
                return Err(InvalidBase64::InsufficientBits);
            }
            if index != input.len() {
                return Err(InvalidBase64::TrailingInput(index));
            }
            break;
        }
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return Err(InvalidBase64::Character(byte as i8)),
        };
        word = (word << 6) | u32::from(value);
        bits += 6;
        sextets = (sextets + 1) % 4;
        if bits >= 8 {
            bits -= 8;
            bytes.push((word >> bits) as u8);
        }
    }
    if sextets == 1 {
        return Err(InvalidBase64::InsufficientBits);
    }
    Ok(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BigIntegerError {
    EmptyInteger,
    EmptyEncoding,
    OutOfRange,
}

impl fmt::Display for BigIntegerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyInteger => "Zero length BigInteger",
            Self::EmptyEncoding => "1 > 0",
            Self::OutOfRange => {
                "Assert failed: Does not support integer beyond the range of [-2^1015, 2^1015-1]\n(< n 128)"
            }
        })
    }
}

impl std::error::Error for BigIntegerError {}

fn canonical_integer(mut bytes: &[u8]) -> Result<&[u8], BigIntegerError> {
    if bytes.is_empty() {
        return Err(BigIntegerError::EmptyInteger);
    }
    while bytes.len() > 1
        && ((bytes[0] == 0 && bytes[1] < 128) || (bytes[0] == 255 && bytes[1] >= 128))
    {
        bytes = &bytes[1..];
    }
    Ok(bytes)
}

/// Prefix a signed big-endian integer with its sortable sign/length byte.
pub fn encode_bigint(bytes: &[u8]) -> Result<Vec<u8>, BigIntegerError> {
    let bytes = canonical_integer(bytes)?;
    if bytes.len() >= 128 {
        return Err(BigIntegerError::OutOfRange);
    }
    let prefix = if bytes[0] >= 128 {
        127 - bytes.len() as u8
    } else {
        128 + bytes.len() as u8
    };
    let mut encoded = Vec::with_capacity(bytes.len() + 1);
    encoded.push(prefix);
    encoded.extend_from_slice(bytes);
    Ok(encoded)
}

/// Return canonical signed bytes. The standalone Clojure decoder ignores the
/// prefix and accepts oversized payloads; its encoder applies the size limit.
pub fn decode_bigint(encoded: &[u8]) -> Result<Vec<u8>, BigIntegerError> {
    let bytes = encoded.get(1..).ok_or(BigIntegerError::EmptyEncoding)?;
    Ok(canonical_integer(bytes)?.to_vec())
}

/// Signed primitive storage fields, without an index type header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegerWidth {
    Byte = 1,
    Short = 2,
    Int = 4,
    Long = 8,
}

impl IntegerWidth {
    /// Encode the low bits. Language bindings apply their numeric cast policy
    /// before calling this codec, including checked int/short conversions.
    pub fn encode(self, value: i64, little_endian: bool) -> Vec<u8> {
        let width = self as usize;
        if little_endian {
            value.to_le_bytes()[..width].to_vec()
        } else {
            value.to_be_bytes()[8 - width..].to_vec()
        }
    }

    /// Decode exactly one primitive field with sign extension.
    pub fn decode(self, bytes: &[u8], little_endian: bool) -> Option<i64> {
        let width = self as usize;
        if bytes.len() != width {
            return None;
        }
        let sign_byte = if little_endian {
            bytes[width - 1]
        } else {
            bytes[0]
        };
        let mut extended = [if sign_byte < 128 { 0 } else { 255 }; 8];
        if little_endian {
            extended[..width].copy_from_slice(bytes);
            Some(i64::from_le_bytes(extended))
        } else {
            extended[8 - width..].copy_from_slice(bytes);
            Some(i64::from_be_bytes(extended))
        }
    }
}
