//! Standalone byte formats exposed by `datalevin.bits`.

use crate::nippy::Value;
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

/// Attribute names have no header and are limited by LMDB's 511-byte key size.
/// The binding strips the first UTF-16 unit from the value's string form.
pub fn encode_attr(units: &[u16]) -> Result<Vec<u8>, usize> {
    let bytes = encode_utf16(units);
    if bytes.len() > 511 {
        Err(bytes.len())
    } else {
        Ok(bytes)
    }
}

pub fn decode_attr(bytes: &[u8]) -> String {
    decode_utf8(bytes)
}

/// Each word is a separate buffer write, including the signed word-count
/// prefix. Arrays of up to three elements retain their uncompressed layout.
pub fn encode_ints(input: &[i32], sorted: bool, little: bool) -> Vec<[u8; 4]> {
    let compressed = input.len() > 3;
    let words = if compressed {
        if sorted {
            crate::nippy::integers::compress_sorted(input)
        } else {
            crate::nippy::integers::compress(input)
        }
    } else {
        input.to_vec()
    };
    let count = words.len() as i32;
    std::iter::once(if compressed { -count } else { count })
        .chain(words)
        .map(|word| {
            if little {
                word.to_le_bytes()
            } else {
                word.to_be_bytes()
            }
        })
        .collect()
}

pub fn ints_header(bytes: [u8; 4], little: bool) -> (u32, bool) {
    let count = if little {
        i32::from_le_bytes(bytes)
    } else {
        i32::from_be_bytes(bytes)
    };
    (count.unsigned_abs(), count < 0)
}

#[derive(Debug)]
pub enum IntArrayError {
    NegativeSize(i32),
    EmptySortedBlock,
    Limit,
    Compressed(crate::nippy::ErrorKind),
    Length,
}

/// The caller consumes each complete word before decoding, preserving buffer
/// underflow progress. Bound the expanded output before allocating it.
pub fn decode_ints(
    bytes: &[u8],
    compressed: bool,
    sorted: bool,
    little: bool,
    max_values: usize,
) -> Result<Vec<i32>, IntArrayError> {
    if !bytes.len().is_multiple_of(4) {
        return Err(IntArrayError::Length);
    }
    let words: Vec<i32> = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .copied()
        .map(|word| {
            if little {
                i32::from_le_bytes(word)
            } else {
                i32::from_be_bytes(word)
            }
        })
        .collect();
    if !compressed {
        return Ok(words);
    }
    let count = *words.first().ok_or(IntArrayError::Length)?;
    if count < 0 {
        return Err(IntArrayError::NegativeSize(count));
    }
    if count as usize > max_values {
        return Err(IntArrayError::Limit);
    }
    // IntegratedBinaryPacking reads the last output even for a zero-length
    // block when a compressed body remains; retain the JVM reader's error.
    if sorted && count == 0 && words.len() > 1 {
        return Err(IntArrayError::EmptySortedBlock);
    }
    crate::nippy::integers::decompress_storage(&words, sorted).map_err(IntArrayError::Compressed)
}

pub fn bitmap(values: &[i32]) -> roaring::RoaringBitmap {
    let mut bitmap: roaring::RoaringBitmap = values.iter().map(|&value| value as u32).collect();
    bitmap.optimize();
    bitmap
}

/// Treat JVM longs as unsigned 64-bit bitmap members, preserving their bit patterns.
pub fn bitmap64(values: &[i64]) -> roaring::RoaringTreemap {
    values.iter().map(|&value| value as u64).collect()
}

pub fn bitmap_and(
    left: &roaring::RoaringBitmap,
    right: &roaring::RoaringBitmap,
) -> roaring::RoaringBitmap {
    left & right
}

pub fn bitmap_or(
    left: &roaring::RoaringBitmap,
    right: &roaring::RoaringBitmap,
) -> roaring::RoaringBitmap {
    left | right
}

pub fn bitmaps_and(bitmaps: &[&roaring::RoaringBitmap]) -> Option<roaring::RoaringBitmap> {
    let (first, rest) = bitmaps.split_first()?;
    Some(
        rest.iter()
            .fold((*first).clone(), |result, bitmap| result & *bitmap),
    )
}

pub fn bitmaps_or(bitmaps: &[&roaring::RoaringBitmap]) -> Option<roaring::RoaringBitmap> {
    let (first, rest) = bitmaps.split_first()?;
    Some(
        rest.iter()
            .fold((*first).clone(), |result, bitmap| result | *bitmap),
    )
}

pub struct BitmapWrites {
    pub header: Vec<Vec<u8>>,
    pub containers: Vec<Vec<Vec<u8>>>,
}

fn bitmap_field(bytes: &[u8], position: &mut usize, width: usize) -> Vec<u8> {
    let field = bytes[*position..*position + width].to_vec();
    *position += width;
    field
}

/// Portable Roaring bytes, split at the JVM writer's atomic buffer operations.
/// Container views commit their positions only after the whole container.
pub fn encode_bitmap(bitmap: &roaring::RoaringBitmap) -> BitmapWrites {
    let mut bytes = Vec::with_capacity(bitmap.serialized_size());
    bitmap
        .serialize_into(&mut bytes)
        .expect("writing into a vector");
    let cookie = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    let runs = cookie & 0xffff == 12347;
    let count = if runs {
        (cookie >> 16) as usize + 1
    } else {
        u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize
    };
    let flags = if runs {
        &bytes[4..4 + count.div_ceil(8)]
    } else {
        &[]
    };
    let mut position = 0;
    let mut header = vec![bitmap_field(&bytes, &mut position, 4)];
    if runs {
        for _ in flags {
            header.push(bitmap_field(&bytes, &mut position, 1));
        }
    } else {
        header.push(bitmap_field(&bytes, &mut position, 4));
    }
    let mut cardinalities = Vec::with_capacity(count);
    for _ in 0..count {
        header.push(bitmap_field(&bytes, &mut position, 2));
        let cardinal = bitmap_field(&bytes, &mut position, 2);
        cardinalities
            .push(usize::from(u16::from_le_bytes(cardinal.as_slice().try_into().unwrap())) + 1);
        header.push(cardinal);
    }
    if !runs || count >= 4 {
        for _ in 0..count {
            header.push(bitmap_field(&bytes, &mut position, 4));
        }
    }
    let containers = cardinalities
        .into_iter()
        .enumerate()
        .map(|(index, cardinal)| {
            if runs && flags[index / 8] & (1 << (index % 8)) != 0 {
                let prefix = bitmap_field(&bytes, &mut position, 2);
                let count = usize::from(u16::from_le_bytes(prefix.as_slice().try_into().unwrap()));
                vec![prefix, bitmap_field(&bytes, &mut position, count * 4)]
            } else {
                vec![bitmap_field(
                    &bytes,
                    &mut position,
                    if cardinal > 4096 { 8192 } else { cardinal * 2 },
                )]
            }
        })
        .collect();
    BitmapWrites { header, containers }
}

#[derive(Debug)]
pub enum BitmapError {
    Buffer,
    Cookie(i32),
    Count(i32),
    Limit,
    Invalid,
}

impl From<TupleError> for BitmapError {
    fn from(_: TupleError) -> Self {
        Self::Buffer
    }
}

/// The JVM reader uses a private little-endian slice and ignores offsets.
/// Return read/seek operations for bounds errors without advancing its owner.
pub fn decode_bitmap(
    bytes: &[u8],
    max_members: usize,
) -> (Vec<TupleRead>, Result<Value, BitmapError>) {
    let mut cursor = TupleCursor {
        bytes,
        position: 0,
        limit: bytes.len(),
        mark: None,
        reads: Vec::new(),
    };
    let result = (|| {
        let cookie = u32::from_le_bytes(cursor.read(4)?.try_into().unwrap());
        let runs = cookie & 0xffff == 12347;
        if !runs && cookie != 12346 {
            return Err(BitmapError::Cookie(cookie as i32));
        }
        let count = if runs {
            (cookie >> 16) as i32 + 1
        } else {
            i32::from_le_bytes(cursor.read(4)?.try_into().unwrap())
        };
        if !(0..=65536).contains(&count) {
            return Err(BitmapError::Count(count));
        }
        let count = count as usize;
        let flags = if runs {
            cursor.read(count.div_ceil(8) as i64)?.to_vec()
        } else {
            Vec::new()
        };
        let mut cardinalities = Vec::with_capacity(count);
        let mut members = 0;
        for _ in 0..count {
            cursor.read(2)?;
            let cardinal = usize::from(u16::from_le_bytes(cursor.read(2)?.try_into().unwrap())) + 1;
            cardinalities.push(cardinal);
            members += cardinal;
        }
        if !runs || count >= 4 {
            cursor.seek((cursor.position + count * 4) as i64)?;
        }
        for (index, cardinal) in cardinalities.into_iter().enumerate() {
            let size = if runs && flags[index / 8] & (1 << (index % 8)) != 0 {
                usize::from(u16::from_le_bytes(cursor.read(2)?.try_into().unwrap())) * 4
            } else if cardinal > 4096 {
                8192
            } else {
                cardinal * 2
            };
            cursor.read(size as i64)?;
        }
        if members > max_members {
            return Err(BitmapError::Limit);
        }
        let bitmap = roaring::RoaringBitmap::deserialize_from(&bytes[..cursor.position])
            .map_err(|_| BitmapError::Invalid)?;
        if bitmap.len() as usize != members {
            return Err(BitmapError::Invalid);
        }
        Ok(Value::Bitmap(bitmap))
    })();
    (cursor.reads, result)
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

/// A binding may defer a failed JVM cast/lookup until its component is reached.
/// This keeps earlier writes observable even when a later input is invalid.
pub enum TupleElement {
    Value(Value),
    HostError { index: usize, lookup: bool },
}

#[derive(Debug)]
pub enum TupleError {
    Empty,
    Host(usize),
    TooLarge(usize),
    Scalar(ScalarError),
    Integer(BigIntegerError),
    DecimalRange,
    Named,
    Header(u8),
    Buffer,
    WrongValue,
}

pub type TupleWrite = Result<Vec<u8>, TupleError>;

fn tuple_scalar(kind: &str) -> Option<ScalarType> {
    Some(match kind {
        "long" => ScalarType::Long,
        "float" => ScalarType::Float,
        "double" => ScalarType::Double,
        "boolean" => ScalarType::Boolean,
        "instant" => ScalarType::Instant,
        "uuid" => ScalarType::Uuid,
        _ => return None,
    })
}

fn tuple_fixed(kind: &str) -> bool {
    kind == "int" || tuple_scalar(kind).is_some()
}

pub fn tuple_header(types: &[&str]) -> Option<u8> {
    if types.iter().any(|kind| {
        !tuple_fixed(kind)
            && !matches!(
                *kind,
                "string" | "bytes" | "keyword" | "symbol" | "bigint" | "bigdec"
            )
    }) {
        return None;
    }
    Some(if types.len() == 1 { 0xf3 } else { 0xf4 })
}

fn tuple_extreme(value: &Value) -> Option<bool> {
    match value {
        Value::Keyword(value) if value.as_ref() == "db.value/sysMin" => Some(false),
        Value::Keyword(value) if value.as_ref() == "db.value/sysMax" => Some(true),
        _ => None,
    }
}

fn component_header(kind: &str, value: &TupleElement) -> Result<u8, TupleError> {
    if let TupleElement::HostError {
        index,
        lookup: true,
    } = value
    {
        return Err(TupleError::Host(*index));
    }
    if kind == "long" {
        return match value {
            TupleElement::HostError { index, .. } => Err(TupleError::Host(*index)),
            TupleElement::Value(Value::Long(value)) => Ok(ScalarType::Long.header(*value < 0)),
            TupleElement::Value(value) => tuple_extreme(value)
                .map(|maximum| ScalarType::Long.header(!maximum))
                .ok_or(TupleError::WrongValue),
        };
    }
    Ok(if kind == "int" {
        0xc2
    } else if let Some(kind) = tuple_scalar(kind) {
        kind.header(false)
    } else {
        match kind {
            "string" => VariableType::String,
            "bytes" => VariableType::Bytes,
            "keyword" => VariableType::Keyword,
            "symbol" => VariableType::Symbol,
            "bigint" => VariableType::BigInteger,
            "bigdec" => VariableType::BigDecimal,
            _ => return Err(TupleError::WrongValue),
        }
        .header()
    })
}

fn component_writes(
    kind: &str,
    header: u8,
    element: &TupleElement,
    little: bool,
) -> Result<Vec<Vec<u8>>, TupleError> {
    let value = match element {
        TupleElement::Value(value) => value,
        TupleElement::HostError { index, .. } => return Err(TupleError::Host(*index)),
    };
    let extreme = tuple_extreme(value);
    if !tuple_fixed(kind) {
        if let Some(maximum) = extreme {
            return Ok(vec![if maximum { vec![255; 255] } else { vec![0] }]);
        }
        let bytes = match (kind, value) {
            ("string", Value::CharArray(units)) => encode_utf16(units),
            ("bytes", Value::Bytes(bytes)) => bytes.clone(),
            ("keyword" | "symbol", Value::Vector(parts)) => {
                let [Value::CharArray(name), namespace] = parts.as_slice() else {
                    return Err(TupleError::WrongValue);
                };
                let namespace = match namespace {
                    Value::Null => None,
                    Value::CharArray(units) => Some(units.as_slice()),
                    _ => return Err(TupleError::WrongValue),
                };
                encode_named(name, namespace)
            }
            ("bigint", Value::BigInteger(bytes)) => {
                encode_bigint(bytes).map_err(TupleError::Integer)?
            }
            ("bigdec", Value::BigDecimal { unscaled, scale }) => {
                // The tuple helper uses a temporary BIG_ENDIAN buffer and
                // validates its integer before calculating the approximation.
                if canonical_integer(unscaled)
                    .map_err(TupleError::Integer)?
                    .len()
                    >= 128
                {
                    return Err(TupleError::DecimalRange);
                }
                let mut bytes = Vec::new();
                for field in decimal_fields(unscaled, *scale, false) {
                    bytes.extend(field.map_err(|error| match error {
                        DecimalError::Integer(error) => TupleError::Integer(error),
                        DecimalError::Scalar(error) => TupleError::Scalar(error),
                        _ => TupleError::WrongValue,
                    })?);
                }
                bytes
            }
            _ => return Err(TupleError::WrongValue),
        };
        return Ok(vec![bytes]);
    }
    if kind == "int" {
        let value = match (extreme, value) {
            (Some(maximum), _) => {
                if maximum {
                    i32::MAX
                } else {
                    i32::MIN
                }
            }
            (None, Value::Integer(value)) => *value,
            (None, Value::Long(value)) => {
                i32::try_from(*value).map_err(|_| TupleError::WrongValue)?
            }
            _ => return Err(TupleError::WrongValue),
        };
        return Ok(vec![
            IntegerWidth::Int.encode(i64::from(value ^ i32::MIN), little),
        ]);
    }
    let scalar = tuple_scalar(kind).ok_or(TupleError::WrongValue)?;
    let value = if let Some(maximum) = extreme {
        match scalar {
            ScalarType::Long => ScalarValue::Integer(match (header == 0xc0, maximum) {
                (true, false) => i64::MIN,
                (true, true) => -1,
                (false, false) => 0,
                (false, true) => i64::MAX,
            }),
            ScalarType::Float => ScalarValue::Float(if maximum {
                f32::INFINITY
            } else {
                f32::NEG_INFINITY
            }),
            ScalarType::Double => ScalarValue::Double(if maximum {
                f64::INFINITY
            } else {
                f64::NEG_INFINITY
            }),
            ScalarType::Instant => ScalarValue::Integer(if maximum { i64::MAX } else { i64::MIN }),
            ScalarType::Boolean => ScalarValue::Boolean(maximum),
            ScalarType::Uuid => ScalarValue::Uuid([if maximum { 255 } else { 0 }; 16]),
            _ => return Err(TupleError::WrongValue),
        }
    } else {
        match value {
            Value::Long(value) => ScalarValue::Integer(*value),
            Value::Float(value) => ScalarValue::Float(f32::from_bits(*value)),
            Value::Double(value) => ScalarValue::Double(f64::from_bits(*value)),
            Value::Bool(value) => ScalarValue::Boolean(*value),
            Value::Date(value) => ScalarValue::Integer(*value),
            Value::Uuid(value) => ScalarValue::Uuid(*value),
            _ => return Err(TupleError::WrongValue),
        }
    };
    let bytes = scalar.encode(value, little).map_err(TupleError::Scalar)?;
    Ok(if scalar == ScalarType::Uuid {
        vec![bytes[..8].to_vec(), bytes[8..].to_vec()]
    } else {
        vec![bytes]
    })
}

/// Encode the tuple body as atomic field writes. The binding writes the outer
/// tuple header first and stops applying fields at its first buffer/codec error.
pub fn encode_tuple(
    types: &[&str],
    values: &[TupleElement],
    count: usize,
    little: bool,
) -> Vec<TupleWrite> {
    let mut writes = Vec::new();
    let result = (|| {
        let homogeneous = types.len() == 1;
        let expected = if homogeneous { count } else { types.len() };
        if tuple_header(types).is_none() || values.len() != expected {
            return Err(TupleError::WrongValue);
        }
        if homogeneous && count == 0 {
            return Err(TupleError::Empty);
        }
        let count = if homogeneous { count } else { types.len() };
        let header = if homogeneous {
            let header = component_header(types[0], &values[0])?;
            writes.push(Ok(vec![header]));
            header
        } else {
            0
        };
        writes.push(Ok(vec![count as u8]));
        let mut sizes = Vec::new();
        for (index, element) in values.iter().take(count).enumerate() {
            let kind = types[if homogeneous { 0 } else { index }];
            if let TupleElement::HostError {
                index,
                lookup: true,
            } = element
            {
                return Err(TupleError::Host(*index));
            }
            let nil = !homogeneous && matches!(element, TupleElement::Value(Value::Null));
            let header = if homogeneous {
                header
            } else {
                let header = if nil {
                    0
                } else {
                    component_header(kind, element)?
                };
                writes.push(Ok(vec![header]));
                header
            };
            let fixed = nil || tuple_fixed(kind);
            let mut size = usize::from(!homogeneous);
            if !nil {
                for bytes in component_writes(kind, header, element, little)? {
                    size += bytes.len();
                    writes.push(Ok(bytes));
                }
            }
            if !homogeneous && !fixed {
                writes.push(Ok(vec![0]));
                size += 1;
            }
            if (!homogeneous || !fixed) && size > 257 {
                return Err(TupleError::TooLarge(index));
            }
            if homogeneous && !fixed {
                writes.push(Ok(vec![0]));
            }
            sizes.push(size as u8);
        }
        if !homogeneous || !tuple_fixed(types[0]) {
            writes.push(Ok(vec![255]));
            writes.extend(sizes.into_iter().map(|size| Ok(vec![size])));
        }
        Ok(())
    })();
    if let Err(error) = result {
        writes.push(Err(error));
    }
    writes
}

/// Replay these JVM buffer operations even on decode failure. Bounds checks,
/// mark invalidation and exception messages then come from the actual buffer.
pub enum TupleRead {
    Bytes(i64),
    Position(i64),
    Absolute(i64, usize),
    Mark,
    Reset,
    Limit(i64),
}

struct TupleCursor<'a> {
    bytes: &'a [u8],
    position: usize,
    limit: usize,
    mark: Option<usize>,
    reads: Vec<TupleRead>,
}

impl<'a> TupleCursor<'a> {
    fn read(&mut self, size: i64) -> Result<&'a [u8], TupleError> {
        self.reads.push(TupleRead::Bytes(size));
        if size < 0 || size as u64 > (self.limit - self.position) as u64 {
            return Err(TupleError::Buffer);
        }
        let start = self.position;
        self.position += size as usize;
        Ok(&self.bytes[start..self.position])
    }
    fn seek(&mut self, position: i64) -> Result<(), TupleError> {
        self.reads.push(TupleRead::Position(position));
        if position < 0 || position as u64 > self.limit as u64 {
            return Err(TupleError::Buffer);
        }
        self.position = position as usize;
        if self.mark.is_some_and(|mark| mark > self.position) {
            self.mark = None;
        }
        Ok(())
    }
    fn at(&mut self, position: i64, width: usize) -> Result<&'a [u8], TupleError> {
        self.reads.push(TupleRead::Absolute(position, width));
        if position < 0 || position as u64 + width as u64 > self.limit as u64 {
            return Err(TupleError::Buffer);
        }
        Ok(&self.bytes[position as usize..position as usize + width])
    }
    fn mark(&mut self) {
        self.reads.push(TupleRead::Mark);
        self.mark = Some(self.position);
    }
    fn reset(&mut self) -> Result<(), TupleError> {
        self.reads.push(TupleRead::Reset);
        self.position = self.mark.ok_or(TupleError::Buffer)?;
        Ok(())
    }
    fn set_limit(&mut self, limit: i64) -> Result<(), TupleError> {
        self.reads.push(TupleRead::Limit(limit));
        if limit < 0 || limit as u64 > self.bytes.len() as u64 {
            return Err(TupleError::Buffer);
        }
        self.limit = limit as usize;
        self.position = self.position.min(self.limit);
        if self.mark.is_some_and(|mark| mark > self.limit) {
            self.mark = None;
        }
        Ok(())
    }
    fn byte(&mut self) -> Result<u8, TupleError> {
        Ok(self.read(1)?[0])
    }
    fn value(&mut self, header: u8, post: i64, little: bool) -> Result<Value, TupleError> {
        if matches!(header, 0xf3 | 0xf4) {
            return self.tuple_body(header == 0xf3, little, post);
        }
        let kind = header_type(header)?;
        if kind == "int" {
            let integer = IntegerWidth::Int.decode(self.read(4)?, little).unwrap() as i32;
            return Ok(Value::Integer(integer ^ i32::MIN));
        }
        if let Some(kind) = tuple_scalar(kind) {
            let width = match kind {
                ScalarType::Boolean => 1,
                ScalarType::Float => 4,
                ScalarType::Uuid => 16,
                _ => 8,
            };
            let mut bytes = self.read(if width == 16 { 8 } else { width })?.to_vec();
            if width == 16 {
                bytes.extend(self.read(8)?);
            }
            return kind
                .decode(&bytes, little)
                .map(|value| match value {
                    ScalarValue::Integer(value) => {
                        if kind == ScalarType::Instant {
                            Value::Date(value)
                        } else {
                            Value::Long(value)
                        }
                    }
                    ScalarValue::Float(value) => Value::Float(value.to_bits()),
                    ScalarValue::Double(value) => Value::Double(value.to_bits()),
                    ScalarValue::Boolean(value) => Value::Bool(value),
                    ScalarValue::Uuid(bytes) => Value::Uuid(bytes),
                })
                .map_err(TupleError::Scalar);
        }
        if matches!(kind, "bigint" | "bigdec") {
            let approximate = if kind == "bigdec" {
                self.read(8)?.to_vec()
            } else {
                vec![]
            };
            let prefix = self.byte()?;
            let mut integer = vec![prefix];
            integer.extend(self.read(bigint_length(prefix) as i64)?);
            let unscaled = decode_bigint(&integer).map_err(TupleError::Integer)?;
            return if kind == "bigint" {
                Ok(Value::BigInteger(unscaled))
            } else {
                let mut bytes = approximate;
                bytes.extend(integer);
                bytes.extend(self.read(1)?);
                bytes.extend(self.read(4)?);
                let (unscaled, scale) =
                    decode_decimal(&bytes, little).map_err(|_| TupleError::WrongValue)?;
                Ok(Value::BigDecimal { unscaled, scale })
            };
        }
        let size = self.limit as i64 - self.position as i64 - post;
        let bytes = self.read(size)?;
        Ok(match kind {
            "bytes" => Value::Bytes(bytes.to_vec()),
            "string" => Value::Text(decode_utf8(bytes)),
            "keyword" | "symbol" => {
                let name = decode_named(bytes).ok_or(TupleError::Named)?;
                if kind == "keyword" {
                    Value::Keyword(name.into())
                } else {
                    Value::Symbol(name)
                }
            }
            _ => return Err(TupleError::WrongValue),
        })
    }
    fn tuple(&mut self, homogeneous: bool, little: bool) -> Result<Value, TupleError> {
        self.byte()?; // The public reader consumes but does not validate this tag.
        self.tuple_body(homogeneous, little, 0)
    }
    fn tuple_body(
        &mut self,
        homogeneous: bool,
        little: bool,
        outer_post: i64,
    ) -> Result<Value, TupleError> {
        let header = if homogeneous { self.byte()? } else { 0 };
        let count = usize::from(self.byte()?);
        let mut values = Vec::new();
        if homogeneous && tuple_fixed(header_type(header)?) {
            for _ in 0..count {
                values.push(self.value(header, 0, little)?);
            }
        } else {
            let start = self.position;
            self.seek(self.limit as i64 - count as i64 - outer_post)?;
            let mut sizes = Vec::new();
            for _ in 0..count {
                sizes.push(i64::from(self.byte()?));
            }
            let end = self.position;
            let mut posts = vec![0i64; count];
            let mut suffix = 0;
            for index in (0..count).rev() {
                posts[index] = suffix + count as i64 + outer_post + if homogeneous { 2 } else { 1 };
                suffix += sizes[index] + i64::from(homogeneous);
            }
            self.seek(start as i64)?;
            for post in posts {
                let header = if homogeneous { header } else { self.byte()? };
                let fixed = header == 0 || tuple_fixed(header_type(header)?);
                values.push(if header == 0 {
                    Value::Null
                } else {
                    self.value(header, post + i64::from(!homogeneous && !fixed), little)?
                });
                if !fixed {
                    self.byte()?;
                }
            }
            self.seek(end as i64)?;
        }
        Ok(Value::Vector(values))
    }
}

fn header_type(header: u8) -> Result<&'static str, TupleError> {
    Ok(match header {
        0xc0 | 0xc1 | 0xf8 => "long",
        0xc2 => "int",
        0xf1 => "bigint",
        0xf2 => "bigdec",
        0xf5 => "float",
        0xf6 => "double",
        0xf7 => "instant",
        0xf9 => "uuid",
        0xfa => "string",
        0xfb => "keyword",
        0xfc => "symbol",
        0xfd => "boolean",
        0xfe => "bytes",
        _ => return Err(TupleError::Header(header)),
    })
}

pub fn decode_tuple(
    bytes: &[u8],
    position: usize,
    homogeneous: bool,
    little: bool,
) -> (Vec<TupleRead>, Result<Value, TupleError>) {
    let mut cursor = TupleCursor {
        bytes,
        position,
        limit: bytes.len(),
        mark: None,
        reads: Vec::new(),
    };
    let result = if position <= bytes.len() {
        cursor.tuple(homogeneous, little)
    } else {
        Err(TupleError::WrongValue)
    };
    (cursor.reads, result)
}

#[derive(Debug)]
pub enum IndexError {
    Tuple(TupleError),
    Nippy(crate::nippy::Error),
    Unsupported,
}

impl From<TupleError> for IndexError {
    fn from(value: TupleError) -> Self {
        Self::Tuple(value)
    }
}

pub enum IndexInput {
    Value(Value),
    Tuple {
        elements: Vec<TupleElement>,
        count: usize,
    },
}

pub struct Indexable {
    pub header: u8,
    pub bytes: Option<Vec<u8>>,
    pub cut: bool,
}

fn raw_index_type(kind: &str) -> &str {
    match kind {
        "db.type/ref" => "long",
        "db.type/string" => "string",
        "db.type/keyword" => "keyword",
        "db.type/symbol" => "symbol",
        "db.type/long" => "long",
        "db.type/float" => "float",
        "db.type/double" => "double",
        "db.type/boolean" => "boolean",
        "db.type/instant" => "instant",
        "db.type/uuid" => "uuid",
        "db.type/bytes" => "bytes",
        "db.type/bigint" => "bigint",
        "db.type/bigdec" => "bigdec",
        _ => kind,
    }
}

/// Construct the encoded portion of an indexable. Fixed values remain deferred
/// until the AVG writer; variable payloads are encoded and truncated here.
pub fn indexable(value_type: &Value, input: &IndexInput) -> Result<Indexable, IndexError> {
    let kind = match value_type {
        Value::Keyword(kind) => kind.as_ref(),
        Value::Map(_) => return Err(IndexError::Unsupported),
        _ => "",
    };
    let tuple_types = if let Value::Vector(types) = value_type {
        let types: Option<Vec<&str>> = types
            .iter()
            .map(|value| match value {
                Value::Keyword(kind) => Some(raw_index_type(kind)),
                _ => None,
            })
            .collect();
        let types = types.ok_or(IndexError::Unsupported)?;
        if tuple_header(&types).is_none() {
            return Err(IndexError::Unsupported);
        }
        Some(types)
    } else {
        None
    };
    let value = match input {
        IndexInput::Value(value) => value,
        _ => &Value::Null,
    };
    let fixed = matches!(
        kind,
        "db.type/long"
            | "db.type/ref"
            | "db.type/float"
            | "db.type/double"
            | "db.type/boolean"
            | "db.type/instant"
            | "db.type/uuid"
    );
    let variable = matches!(
        kind,
        "db.type/string"
            | "db.type/bytes"
            | "db.type/keyword"
            | "db.type/symbol"
            | "db.type/bigint"
            | "db.type/bigdec"
    );
    let header = if let Some(types) = &tuple_types {
        tuple_header(types).unwrap()
    } else if kind == "db.type/ref" {
        0xf8
    } else if fixed || variable {
        component_header(raw_index_type(kind), &TupleElement::Value(value.clone()))?
    } else {
        0
    };
    let mut bytes = if fixed || (kind == "db.type/bytes" && matches!(value, Value::Null)) {
        None
    } else if let Some(maximum) = tuple_extreme(value) {
        Some(if kind == "db.type/bytes" {
            if maximum {
                vec![255; 498]
            } else {
                vec![0; 497]
            }
        } else if maximum {
            vec![255; 497]
        } else {
            vec![0]
        })
    } else if let Some(types) = tuple_types {
        let IndexInput::Tuple { elements, count } = input else {
            return Err(TupleError::WrongValue.into());
        };
        let mut bytes = Vec::new();
        for field in encode_tuple(&types, elements, *count, false) {
            bytes.extend(field?);
        }
        Some(bytes)
    } else if variable {
        Some(
            component_writes(
                raw_index_type(kind),
                header,
                &TupleElement::Value(value.clone()),
                false,
            )?
            .concat(),
        )
    } else {
        Some(crate::nippy::fast_freeze(value).map_err(IndexError::Nippy)?)
    };
    let cut = bytes.as_ref().is_some_and(|bytes| bytes.len() > 497);
    if cut {
        bytes.as_mut().unwrap().truncate(496);
    }
    Ok(Indexable { header, bytes, cut })
}

pub fn giant(giant_id: i64) -> bool {
    giant_id != 0
}

/// Ordered AVG writes: attribute, optional tag, value, separator/giant ID, flag.
pub fn encode_avg(
    attribute: i32,
    header: i64,
    bytes: Option<&[u8]>,
    value: &TupleElement,
    giant_id: i64,
    little: bool,
) -> Vec<TupleWrite> {
    let mut fields = vec![Ok(IntegerWidth::Int.encode(i64::from(attribute), little))];
    if header != 0 {
        fields.push(Ok(vec![header as u8]));
    }
    if let Some(bytes) = bytes {
        fields.push(Ok(bytes.to_vec()));
    } else {
        let payload = match value {
            TupleElement::HostError { index, .. } => Err(TupleError::Host(*index)),
            _ => header_type(header as u8).and_then(|kind| {
                if tuple_fixed(kind) {
                    component_writes(kind, header as u8, value, little)
                } else {
                    Err(TupleError::Header(header as u8))
                }
            }),
        };
        match payload {
            Ok(writes) => fields.extend(writes.into_iter().map(Ok)),
            Err(error) => {
                fields.push(Err(error));
                return fields;
            }
        }
    }
    if giant(giant_id) {
        fields.push(Ok(vec![255]));
        fields.push(Ok(IntegerWidth::Long.encode(giant_id, little)));
        fields.push(Ok(vec![2]));
    } else {
        fields.push(Ok(vec![0]));
        fields.push(Ok(vec![1]));
    }
    fields
}

pub enum AvgMode {
    Avg,
    Retrieved,
    Attribute,
    Giant,
    Inline,
}
pub enum AvgValue {
    Value(Value),
    Data(Vec<u8>),
    Custom(Vec<u8>),
}
pub enum AvgResult {
    Retrieved {
        attribute: Option<i32>,
        value: AvgValue,
        giant: i64,
    },
    Value(AvgValue),
}

impl TupleCursor<'_> {
    fn avg_value(&mut self, little: bool) -> Result<AvgValue, IndexError> {
        self.mark();
        let header = self.byte()?;
        if header == 0xf0 {
            let mut bytes = vec![header];
            bytes.extend(self.read(self.limit as i64 - self.position as i64 - 2)?);
            return Ok(AvgValue::Custom(bytes));
        }
        if header_type(header).is_ok() || matches!(header, 0xf3 | 0xf4) {
            return self
                .value(header, 2, little)
                .map(AvgValue::Value)
                .map_err(IndexError::Tuple);
        }
        self.reset()?;
        let limit = self.limit;
        let result = (|| {
            self.set_limit(limit as i64 - 2)?;
            let bytes = self.read((self.limit - self.position) as i64)?.to_vec();
            self.seek(self.limit as i64)?;
            Ok(AvgValue::Data(bytes))
        })();
        self.set_limit(limit as i64)?;
        result
    }
    fn avg(&mut self, mode: AvgMode, little: bool) -> Result<AvgResult, IndexError> {
        match mode {
            AvgMode::Attribute => {
                return Ok(AvgResult::Value(AvgValue::Value(Value::Long(
                    IntegerWidth::Int.decode(self.at(0, 4)?, little).unwrap(),
                ))));
            }
            AvgMode::Giant => {
                let giant = if self.at(self.limit as i64 - 1, 1)?[0] == 2 {
                    IntegerWidth::Long
                        .decode(self.at(self.limit as i64 - 9, 8)?, little)
                        .unwrap()
                } else {
                    0
                };
                return Ok(AvgResult::Value(AvgValue::Value(Value::Long(giant))));
            }
            AvgMode::Inline => {
                self.seek(4)?;
                return self.avg_value(little).map(AvgResult::Value);
            }
            _ => {}
        }
        let include_attribute = match mode {
            AvgMode::Avg => true,
            AvgMode::Retrieved => {
                let custom = self.at(4, 1)?[0] == 0xf0;
                if custom {
                    self.seek(0)?;
                }
                custom
            }
            _ => unreachable!(),
        };
        let attribute = if include_attribute {
            Some(IntegerWidth::Int.decode(self.read(4)?, little).unwrap() as i32)
        } else {
            None
        };
        self.seek(self.limit as i64 - 1)?;
        let (value, giant) = if self.byte()? == 2 {
            self.seek(self.limit as i64 - 9)?;
            let giant = IntegerWidth::Long.decode(self.read(8)?, little).unwrap();
            (AvgValue::Value(Value::Null), giant)
        } else {
            self.seek(4)?;
            (self.avg_value(little)?, 0)
        };
        Ok(AvgResult::Retrieved {
            attribute,
            value,
            giant,
        })
    }
}

pub fn decode_avg(
    bytes: &[u8],
    position: usize,
    mode: AvgMode,
    little: bool,
) -> (Vec<TupleRead>, Result<AvgResult, IndexError>) {
    let mut cursor = TupleCursor {
        bytes,
        position,
        limit: bytes.len(),
        mark: None,
        reads: Vec::new(),
    };
    let result = if position <= bytes.len() {
        cursor.avg(mode, little)
    } else {
        Err(TupleError::WrongValue.into())
    };
    (cursor.reads, result)
}
