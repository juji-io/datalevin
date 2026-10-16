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

/// Number of signed integer payload bytes following its sortable length byte.
pub fn bigint_length(prefix: u8) -> usize {
    usize::from(if prefix >= 128 {
        prefix ^ 128
    } else {
        127 - prefix
    })
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

/// Fixed-size scalar fields used by the typed buffer API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    Long,
    Float,
    Double,
    Boolean,
    Instant,
    LegacyInstant,
    Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScalarValue {
    Integer(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Uuid([u8; 16]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarError {
    NaN(ScalarType),
    InvalidBoolean,
    WrongValue,
    WrongLength,
}

impl ScalarType {
    pub fn header(self, negative: bool) -> u8 {
        match self {
            Self::Long => {
                if negative {
                    0xc0
                } else {
                    0xc1
                }
            }
            Self::Float => 0xf5,
            Self::Double => 0xf6,
            Self::Boolean => 0xfd,
            Self::Instant | Self::LegacyInstant => 0xf7,
            Self::Uuid => 0xf9,
        }
    }

    /// Encode only the payload. Buffer bindings write the header first and
    /// copy UUID words separately to retain the original partial-write behavior.
    pub fn encode(self, value: ScalarValue, little_endian: bool) -> Result<Vec<u8>, ScalarError> {
        let (width, integer) = match (self, value) {
            (Self::Long | Self::LegacyInstant, ScalarValue::Integer(value)) => {
                (IntegerWidth::Long, value)
            }
            (Self::Instant, ScalarValue::Integer(value)) => (IntegerWidth::Long, value ^ i64::MIN),
            (Self::Float, ScalarValue::Float(value)) => {
                if value.is_nan() {
                    return Err(ScalarError::NaN(self));
                }
                let bits = if value == 0.0 { 0 } else { value.to_bits() };
                let sortable = if value < 0.0 { !bits } else { bits ^ (1 << 31) };
                (IntegerWidth::Int, i64::from(sortable))
            }
            (Self::Double, ScalarValue::Double(value)) => {
                if value.is_nan() {
                    return Err(ScalarError::NaN(self));
                }
                let bits = if value == 0.0 { 0 } else { value.to_bits() };
                let sortable = if value < 0.0 { !bits } else { bits ^ (1 << 63) };
                (IntegerWidth::Long, sortable as i64)
            }
            (Self::Boolean, ScalarValue::Boolean(value)) => {
                return Ok(vec![if value { 2 } else { 1 }]);
            }
            (Self::Uuid, ScalarValue::Uuid(mut bytes)) => {
                if little_endian {
                    bytes[..8].reverse();
                    bytes[8..].reverse();
                }
                return Ok(bytes.to_vec());
            }
            _ => return Err(ScalarError::WrongValue),
        };
        Ok(width.encode(integer, little_endian))
    }

    /// Decode a payload after the binding has consumed its type header. The
    /// existing API does not validate that header, nor reject decoded NaNs.
    pub fn decode(self, bytes: &[u8], little_endian: bool) -> Result<ScalarValue, ScalarError> {
        match self {
            Self::Boolean => match bytes {
                [1] => Ok(ScalarValue::Boolean(false)),
                [2] => Ok(ScalarValue::Boolean(true)),
                [_] => Err(ScalarError::InvalidBoolean),
                _ => Err(ScalarError::WrongLength),
            },
            Self::Uuid => {
                let mut bytes: [u8; 16] = bytes.try_into().map_err(|_| ScalarError::WrongLength)?;
                if little_endian {
                    bytes[..8].reverse();
                    bytes[8..].reverse();
                }
                Ok(ScalarValue::Uuid(bytes))
            }
            _ => {
                let width = if self == Self::Float {
                    IntegerWidth::Int
                } else {
                    IntegerWidth::Long
                };
                let integer = width
                    .decode(bytes, little_endian)
                    .ok_or(ScalarError::WrongLength)?;
                Ok(match self {
                    Self::Long | Self::LegacyInstant => ScalarValue::Integer(integer),
                    Self::Instant => ScalarValue::Integer(integer ^ i64::MIN),
                    Self::Float => {
                        let bits = integer as u32;
                        ScalarValue::Float(f32::from_bits(if bits >> 31 == 1 {
                            bits ^ (1 << 31)
                        } else {
                            !bits
                        }))
                    }
                    Self::Double => {
                        let bits = integer as u64;
                        ScalarValue::Double(f64::from_bits(if bits >> 63 == 1 {
                            bits ^ (1 << 63)
                        } else {
                            !bits
                        }))
                    }
                    _ => unreachable!(),
                })
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariableType {
    String,
    Bytes,
    Keyword,
    Symbol,
    BigInteger,
    BigDecimal,
}

impl VariableType {
    pub fn header(self) -> u8 {
        match self {
            Self::String => 0xfa,
            Self::Bytes => 0xfe,
            Self::Keyword => 0xfb,
            Self::Symbol => 0xfc,
            Self::BigInteger => 0xf1,
            Self::BigDecimal => 0xf2,
        }
    }
}

/// Fixed storage sizes exposed by bits/type-size. Internal fixed-width fields
/// such as :short and :int-int intentionally retain the original nil result.
pub fn type_size(kind: &str) -> Option<usize> {
    Some(match kind {
        "int" => 4,
        "long" | "double" | "instant" => 9,
        "id" => 8,
        "float" => 5,
        "byte" => 1,
        "boolean" => 2,
        "uuid" => 17,
        _ => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalError {
    Scalar(ScalarError),
    Integer(BigIntegerError),
    WrongLength,
    InvalidNumber,
}

fn decimal_approximation(unscaled: &[u8], scale: i32) -> Result<f64, DecimalError> {
    let unscaled = canonical_integer(unscaled).map_err(DecimalError::Integer)?;
    let negative = unscaled[0] >= 128;
    let mut magnitude = unscaled.to_vec();
    if negative {
        let mut carry = 1u16;
        for byte in magnitude.iter_mut().rev() {
            let value = u16::from(!*byte) + carry;
            *byte = value as u8;
            carry = value >> 8;
        }
    }
    // Decimal limbs avoid building powers of ten at extreme scales. Rust's
    // decimal parser rounds to binary64, including underflow and infinity.
    let mut limbs = vec![0u32];
    for byte in magnitude {
        let mut carry = u64::from(byte);
        for limb in &mut limbs {
            let value = u64::from(*limb) * 256 + carry;
            *limb = (value % 1_000_000_000) as u32;
            carry = value / 1_000_000_000;
        }
        if carry != 0 {
            limbs.push(carry as u32);
        }
    }
    let mut decimal = if negative {
        String::from("-")
    } else {
        String::new()
    };
    let mut parts = limbs.into_iter().rev();
    decimal.push_str(&parts.next().unwrap().to_string());
    for part in parts {
        decimal.push_str(&format!("{part:09}"));
    }
    decimal.push_str(&format!("e{}", -i64::from(scale)));
    decimal.parse().map_err(|_| DecimalError::InvalidNumber)
}

/// Produce ordered field writes, validating each field when it is reached.
/// Consumers stop on their first write/codec error, retaining earlier writes.
pub fn decimal_fields(
    unscaled: &[u8],
    scale: i32,
    little_endian: bool,
) -> impl Iterator<Item = Result<Vec<u8>, DecimalError>> + '_ {
    std::iter::once_with(move || {
        let approximate = decimal_approximation(unscaled, scale)?;
        ScalarType::Double
            .encode(ScalarValue::Double(approximate), little_endian)
            .map_err(DecimalError::Scalar)
    })
    .chain(std::iter::once_with(move || {
        encode_bigint(unscaled).map_err(DecimalError::Integer)
    }))
    .chain(std::iter::once_with(|| Ok(vec![0])))
    .chain(std::iter::once_with(move || {
        Ok(IntegerWidth::Int.encode(i64::from(scale), little_endian))
    }))
}

/// Decode the unscaled value and scale. The double approximation and separator
/// are consumed without validation, matching the existing storage reader.
pub fn decode_decimal(bytes: &[u8], little_endian: bool) -> Result<(Vec<u8>, i32), DecimalError> {
    let prefix = *bytes.get(8).ok_or(DecimalError::WrongLength)?;
    let length = bigint_length(prefix);
    if bytes.len() != length + 14 {
        return Err(DecimalError::WrongLength);
    }
    let unscaled = decode_bigint(&bytes[8..9 + length]).map_err(DecimalError::Integer)?;
    let scale = IntegerWidth::Int
        .decode(&bytes[10 + length..], little_endian)
        .ok_or(DecimalError::WrongLength)? as i32;
    Ok((unscaled, scale))
}

/// Match String.getBytes(UTF_8): unpaired UTF-16 surrogates become '?'.
pub fn encode_utf16(units: &[u16]) -> Vec<u8> {
    char::decode_utf16(units.iter().copied())
        .map(|value| value.unwrap_or('?'))
        .collect::<String>()
        .into_bytes()
}

/// Match Java's replacement decoder. In particular, a three-byte surrogate
/// encoding is one malformed unit, unlike Rust's standard UTF-8 lossy decoder.
pub fn decode_utf8(bytes: &[u8]) -> String {
    let mut decoded = String::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let first = bytes[index];
        let width = match first {
            0..=127 => {
                decoded.push(char::from(first));
                index += 1;
                continue;
            }
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => {
                decoded.push('\u{fffd}');
                index += 1;
                continue;
            }
        };
        let mut length = 1;
        let mut codepoint = u32::from(first & (0x7f >> width));
        while length < width {
            let Some(&next) = bytes.get(index + length) else {
                break;
            };
            if !(0x80..=0xbf).contains(&next)
                || (length == 1
                    && ((first == 0xe0 && next < 0xa0)
                        || (first == 0xf0 && next < 0x90)
                        || (first == 0xf4 && next >= 0x90)))
            {
                break;
            }
            codepoint = (codepoint << 6) | u32::from(next & 63);
            length += 1;
        }
        decoded.push(if length == width {
            char::from_u32(codepoint).unwrap_or('\u{fffd}')
        } else {
            '\u{fffd}'
        });
        index += length;
    }
    decoded
}

/// Namespace/name storage uses one NUL separator, including unqualified names.
pub fn encode_named(name: &[u16], namespace: Option<&[u16]>) -> Vec<u8> {
    let mut bytes = namespace.map(encode_utf16).unwrap_or_default();
    bytes.push(0);
    bytes.extend(encode_utf16(name));
    bytes
}

/// Only the first separator is replaced, then one leading slash is removed.
pub fn decode_named(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    let mut bytes = bytes.to_vec();
    if let Some(separator) = bytes.iter().position(|&byte| byte == 0) {
        bytes[separator] = b'/';
    }
    Some(decode_utf8(if bytes[0] == b'/' {
        &bytes[1..]
    } else {
        &bytes
    }))
}
