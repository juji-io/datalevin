//! Test-driver transport and adapters for the existing Rust codec. Database
//! calls remain explicitly unsupported until a Rust engine is connected.

use datalevin_codec::bits::{
    self, BigIntegerError, IntegerWidth, ScalarError, ScalarType, ScalarValue,
};
use datalevin_codec::nippy::{
    Decoder, Error, ErrorKind, Limits, Value, fast_freeze, fast_thaw, thaw_with_limits,
};
use std::io::{self, Read, Write};

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

fn keyword(name: &str) -> Value {
    Value::Keyword(name.into())
}

fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

fn map(entries: Vec<(&str, Value)>) -> Value {
    Value::Map(
        entries
            .into_iter()
            .map(|(key, value)| (keyword(key), value))
            .collect(),
    )
}

fn field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Map(entries) => entries
            .iter()
            .find(|(k, _)| *k == keyword(key))
            .map(|(_, v)| v),
        _ => None,
    }
}

fn protocol_error(message: &str) -> Value {
    java_error("java.lang.IllegalArgumentException", message)
}

fn java_error(class: &str, message: &str) -> Value {
    map(vec![
        ("status", keyword("error")),
        (
            "error",
            map(vec![("class", text(class)), ("message", text(message))]),
        ),
    ])
}

fn bigint_error(error: BigIntegerError) -> Value {
    java_error(
        match error {
            BigIntegerError::EmptyInteger => "java.lang.NumberFormatException",
            BigIntegerError::EmptyEncoding => "java.lang.IllegalArgumentException",
            BigIntegerError::OutOfRange => "java.lang.AssertionError",
        },
        &error.to_string(),
    )
}

fn success(value: Value) -> Value {
    map(vec![
        ("status", keyword("ok")),
        (
            "result",
            map(vec![("kind", keyword("value")), ("value", value)]),
        ),
    ])
}

fn unsupported(operation: &str, message: &str) -> Value {
    map(vec![
        ("status", keyword("unsupported")),
        ("operation", keyword(operation)),
        ("message", text(message)),
    ])
}

fn codec_error(operation: &str, error: Error) -> Value {
    // Missing type/header support is a gap even if the test catches an error.
    if matches!(
        error.kind,
        ErrorKind::UnsupportedType(_) | ErrorKind::UnsupportedHeader(_)
    ) {
        return unsupported(operation, &error.to_string());
    }
    map(vec![
        ("status", keyword("error")),
        (
            "error",
            map(vec![
                ("class", text("clojure.lang.ExceptionInfo")),
                ("message", text(&error.to_string())),
                (
                    "data",
                    map(vec![
                        ("error", keyword("codec/decode")),
                        ("offset", Value::int(error.offset as i64)),
                    ]),
                ),
            ]),
        ),
    ])
}

fn deserialize(bytes: &[u8]) -> Result<Value, Error> {
    let limits = Limits {
        max_bytes: MAX_FRAME_BYTES,
        ..Limits::default()
    };
    // bits/deserialize reads one headerless value, then falls back to Nippy's
    // legacy NPY envelope. Do not require EOF: the Clojure API accepts a value
    // followed by unused bytes (unlike the codec's stricter fast_thaw API).
    match Decoder::new(bytes, limits).and_then(|mut decoder| decoder.read_value()) {
        Ok(value) => Ok(value),
        Err(_) if bytes.starts_with(b"NPY") => thaw_with_limits(bytes, limits),
        Err(error) => Err(error),
    }
}

fn value_argument(argument: &Value) -> Option<&Value> {
    field(argument, "value").filter(|_| field(argument, "kind") == Some(&keyword("value")))
}

fn integer_operation(operation: &str, arguments: &[Value]) -> Value {
    let [value, kind, order] = arguments else {
        return protocol_error("Integer codec requires value, type and byte-order arguments");
    };
    let (Some(value), Some(Value::Keyword(kind)), Some(Value::Bool(little_endian))) = (
        value_argument(value),
        value_argument(kind),
        value_argument(order),
    ) else {
        return protocol_error("Invalid integer codec argument descriptors");
    };
    let width = match kind.as_ref() {
        "byte" => IntegerWidth::Byte,
        "short" => IntegerWidth::Short,
        "int" => IntegerWidth::Int,
        "id" => IntegerWidth::Long,
        _ => return unsupported(operation, "Integer storage type is not implemented"),
    };
    if operation == "bits/encode-integer" {
        let integer = match value {
            Value::Byte(value) => i64::from(*value),
            Value::Short(value) => i64::from(*value),
            Value::Integer(value) => i64::from(*value),
            Value::Long(value) => *value,
            _ => return protocol_error("Integer encoding requires a primitive integer"),
        };
        success(Value::Bytes(width.encode(integer, *little_endian)))
    } else {
        let Value::Bytes(bytes) = value else {
            return protocol_error("Integer decoding requires a byte array");
        };
        let Some(integer) = width.decode(bytes, *little_endian) else {
            return protocol_error("Integer decoding requires exactly one field's bytes");
        };
        success(match width {
            IntegerWidth::Byte => Value::Byte(integer as i8),
            IntegerWidth::Short => Value::Short(integer as i16),
            IntegerWidth::Int => Value::Integer(integer as i32),
            IntegerWidth::Long => Value::Long(integer),
        })
    }
}

fn dispatch(request: &Value) -> Value {
    let Some(Value::Keyword(operation)) = field(request, "op") else {
        return protocol_error("Request requires a keyword :op");
    };
    let Some(Value::Vector(arguments)) = field(request, "args") else {
        return protocol_error("Request requires a vector of argument descriptors");
    };
    match operation.as_ref() {
        "bits/scalar-header" | "bits/encode-scalar" | "bits/decode-scalar" => {
            scalar_operation(operation, arguments)
        }
        "bits/encode-integer" | "bits/decode-integer" => integer_operation(operation, arguments),
        "echo" | "bits/serialize" | "bits/deserialize" | "bits/encode-base64"
        | "bits/decode-base64" | "bits/encode-bigint" | "bits/decode-bigint" => {
            let [argument] = arguments.as_slice() else {
                return protocol_error(if operation.as_ref() == "echo" {
                    "Echo requires exactly one value argument"
                } else {
                    "Codec operation requires exactly one value argument"
                });
            };
            let Some(value) = field(argument, "value")
                .filter(|_| field(argument, "kind") == Some(&keyword("value")))
            else {
                return protocol_error("Operation requires a value argument descriptor");
            };
            match operation.as_ref() {
                // Echo is transport coverage, not a codec/database capability.
                "echo" => success(value.clone()),
                "bits/serialize" => match fast_freeze(value) {
                    Ok(bytes) => success(Value::Bytes(bytes)),
                    Err(error) => codec_error(operation, error),
                },
                "bits/deserialize" => match value {
                    Value::Bytes(bytes) => match deserialize(bytes) {
                        Ok(value) => success(value),
                        Err(error) => codec_error(operation, error),
                    },
                    _ => protocol_error("bits/deserialize requires a byte array"),
                },
                "bits/encode-base64" => match value {
                    Value::Bytes(bytes) => success(Value::Text(bits::encode_base64(bytes))),
                    _ => protocol_error("bits/encode-base64 requires a byte array"),
                },
                "bits/decode-base64" => match value {
                    Value::Text(encoded) => match bits::decode_base64(encoded) {
                        Ok(bytes) => success(Value::Bytes(bytes)),
                        Err(error) => protocol_error(&error.to_string()),
                    },
                    _ => protocol_error("bits/decode-base64 requires a string"),
                },
                "bits/encode-bigint" => match value {
                    Value::BigInteger(bytes) => match bits::encode_bigint(bytes) {
                        Ok(bytes) => success(Value::Bytes(bytes)),
                        Err(error) => bigint_error(error),
                    },
                    _ => protocol_error("bits/encode-bigint requires a BigInteger"),
                },
                "bits/decode-bigint" => match value {
                    Value::Bytes(bytes) => match bits::decode_bigint(bytes) {
                        Ok(bytes) => success(Value::BigInteger(bytes)),
                        Err(error) => bigint_error(error),
                    },
                    _ => protocol_error("bits/decode-bigint requires a byte array"),
                },
                _ => unreachable!(),
            }
        }
        _ => unsupported(
            operation,
            &format!("Rust operation is not implemented: {operation}"),
        ),
    }
}

fn scalar_operation(operation: &str, arguments: &[Value]) -> Value {
    let [value, kind, option] = arguments else {
        return protocol_error("Scalar codec requires value, type and option arguments");
    };
    let (Some(value), Some(Value::Keyword(kind)), Some(Value::Bool(option))) = (
        value_argument(value),
        value_argument(kind),
        value_argument(option),
    ) else {
        return protocol_error("Invalid scalar codec argument descriptors");
    };
    let scalar = match kind.as_ref() {
        "long" => ScalarType::Long,
        "float" => ScalarType::Float,
        "double" => ScalarType::Double,
        "boolean" => ScalarType::Boolean,
        "instant" => ScalarType::Instant,
        "instant-pre-06" => ScalarType::LegacyInstant,
        "uuid" => ScalarType::Uuid,
        _ => return unsupported(operation, "Scalar storage type is not implemented"),
    };
    if operation == "bits/scalar-header" {
        return success(Value::Bytes(vec![scalar.header(*option)]));
    }
    let result = if operation == "bits/encode-scalar" {
        let value = match value {
            Value::Long(value) => ScalarValue::Integer(*value),
            Value::Float(bits) => ScalarValue::Float(f32::from_bits(*bits)),
            Value::Double(bits) => ScalarValue::Double(f64::from_bits(*bits)),
            Value::Bool(value) => ScalarValue::Boolean(*value),
            Value::Uuid(bytes) => ScalarValue::Uuid(*bytes),
            _ => return protocol_error("Invalid scalar payload value"),
        };
        scalar.encode(value, *option).map(Value::Bytes)
    } else {
        let Value::Bytes(bytes) = value else {
            return protocol_error("Scalar decoding requires a byte array");
        };
        scalar.decode(bytes, *option).map(|value| match value {
            ScalarValue::Integer(value) => {
                if scalar == ScalarType::Long {
                    Value::Long(value)
                } else {
                    Value::Date(value)
                }
            }
            ScalarValue::Float(value) => Value::Float(value.to_bits()),
            ScalarValue::Double(value) => Value::Double(value.to_bits()),
            ScalarValue::Boolean(value) => Value::Bool(value),
            ScalarValue::Uuid(bytes) => Value::Uuid(bytes),
        })
    };
    match result {
        Ok(value) => success(value),
        Err(ScalarError::NaN(scalar)) => java_error(
            "java.lang.AssertionError",
            &format!(
                "Assert failed: Cannot index NaN\n(not ({}/isNaN x))",
                if scalar == ScalarType::Float {
                    "Float"
                } else {
                    "Double"
                }
            ),
        ),
        Err(ScalarError::InvalidBoolean) => map(vec![
            ("status", keyword("error")),
            (
                "error",
                map(vec![
                    ("class", text("clojure.lang.ExceptionInfo")),
                    (
                        "message",
                        text("Illegal value, expecting a Datalevin boolean value"),
                    ),
                    ("data", map(vec![])),
                ]),
            ),
        ]),
        Err(ScalarError::WrongValue) => protocol_error("Incorrect value for scalar storage type"),
        Err(ScalarError::WrongLength) => {
            protocol_error("Scalar decoding requires exactly one payload")
        }
    }
}

fn read_frame(input: &mut impl Read) -> io::Result<Option<Value>> {
    let mut prefix = [0; 4];
    if input.read(&mut prefix[..1])? == 0 {
        return Ok(None);
    }
    input.read_exact(&mut prefix[1..])?;
    let size = u32::from_be_bytes(prefix) as usize;
    if size == 0 || size > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid adapter frame size",
        ));
    }
    let mut bytes = vec![0; size];
    input.read_exact(&mut bytes)?;
    fast_thaw(&bytes)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

fn write_frame(output: &mut impl Write, value: &Value) -> io::Result<()> {
    let bytes = fast_freeze(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "adapter response is too large",
        ));
    }
    output.write_all(&(bytes.len() as u32).to_be_bytes())?;
    output.write_all(&bytes)?;
    output.flush()
}

fn main() -> io::Result<()> {
    let mut input = io::stdin().lock();
    let mut output = io::stdout().lock();
    write_frame(
        &mut output,
        &map(vec![
            ("protocol", text("datalevin-test-adapter")),
            ("version", Value::int(1)),
            ("backend", keyword("rust")),
            ("implementation", text(env!("CARGO_PKG_NAME"))),
            ("implementation-version", text(env!("CARGO_PKG_VERSION"))),
            (
                "revision",
                option_env!("DATALEVIN_BUILD_REVISION")
                    .map(text)
                    .unwrap_or(Value::Null),
            ),
            (
                "capabilities",
                Value::Set(vec![
                    keyword("echo"),
                    keyword("bits/serialize"),
                    keyword("bits/deserialize"),
                    keyword("bits/encode-base64"),
                    keyword("bits/decode-base64"),
                    keyword("bits/encode-bigint"),
                    keyword("bits/decode-bigint"),
                    keyword("bits/encode-integer"),
                    keyword("bits/decode-integer"),
                    keyword("bits/scalar-header"),
                    keyword("bits/encode-scalar"),
                    keyword("bits/decode-scalar"),
                ]),
            ),
            ("database-implemented?", Value::Bool(false)),
        ]),
    )?;
    while let Some(request) = read_frame(&mut input)? {
        write_frame(&mut output, &dispatch(&request))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalevin_codec::nippy::{Compression, freeze};
    use std::io::Cursor;

    fn invoke(operation: &str, argument: Value) -> Value {
        dispatch(&map(vec![
            ("op", keyword(operation)),
            (
                "args",
                Value::Vector(vec![map(vec![
                    ("kind", keyword("value")),
                    ("value", argument),
                ])]),
            ),
        ]))
    }

    #[test]
    fn serialization_returns_real_codec_bytes() {
        let value = Value::Vector(vec![Value::Bool(false), Value::int(42)]);
        let response = invoke("bits/serialize", value.clone());
        let Some(Value::Bytes(bytes)) = field(field(&response, "result").unwrap(), "value") else {
            panic!("serialize did not return bytes: {response:?}");
        };
        assert_eq!(fast_thaw(bytes).unwrap(), value);
    }

    #[test]
    fn deserialization_reads_current_legacy_and_compressed_payloads() {
        let value = Value::Vector(vec![Value::Bool(false), Value::int(42)]);
        let mut bare = fast_freeze(&value).unwrap();
        bare.extend(fast_freeze(&keyword("unused")).unwrap());
        assert_eq!(
            field(
                field(&invoke("bits/deserialize", Value::Bytes(bare)), "result").unwrap(),
                "value"
            ),
            Some(&value)
        );
        for compression in [
            Compression::None,
            Compression::Lz4,
            Compression::Snappy,
            Compression::Zstd,
            Compression::Lzma2,
        ] {
            let response = invoke(
                "bits/deserialize",
                Value::Bytes(freeze(&value, compression).unwrap()),
            );
            assert_eq!(
                field(field(&response, "result").unwrap(), "value"),
                Some(&value)
            );
        }
    }

    #[test]
    fn malformed_codec_calls_fail_and_missing_formats_remain_unsupported() {
        for value in [Value::Null, Value::Bytes(vec![])] {
            assert_eq!(
                field(&invoke("bits/deserialize", value), "status"),
                Some(&keyword("error"))
            );
        }
        let response = invoke(
            "bits/deserialize",
            Value::Bytes(vec![b'N', b'P', b'Y', 255, 3]),
        );
        assert_eq!(field(&response, "status"), Some(&keyword("unsupported")));
        assert_eq!(
            field(&response, "operation"),
            Some(&keyword("bits/deserialize"))
        );
    }

    #[test]
    fn frames_preserve_query_forms_and_result_shapes() {
        let value = Value::Vector(vec![
            Value::List(vec![keyword("find"), Value::int(3)]),
            Value::Set(vec![Value::int(1), Value::int(2)]),
            Value::Bytes(vec![0, 255]),
        ]);
        let request = map(vec![
            ("op", keyword("echo")),
            (
                "args",
                Value::Vector(vec![map(vec![
                    ("kind", keyword("value")),
                    ("value", value.clone()),
                ])]),
            ),
        ]);
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        let response = dispatch(&read_frame(&mut Cursor::new(bytes)).unwrap().unwrap());
        assert_eq!(
            field(field(&response, "result").unwrap(), "value"),
            Some(&value)
        );
    }

    #[test]
    fn database_calls_are_explicitly_unsupported() {
        for operation in ["empty-db", "db-with", "q", "close-db", "future-operation"] {
            let response = dispatch(&map(vec![
                ("op", keyword(operation)),
                ("args", Value::Vector(vec![])),
            ]));
            assert_eq!(field(&response, "status"), Some(&keyword("unsupported")));
            assert_eq!(field(&response, "operation"), Some(&keyword(operation)));
        }
    }

    #[test]
    fn malformed_and_truncated_frames_are_errors() {
        for bytes in [
            vec![0],
            vec![0, 0, 0, 0],
            vec![127, 255, 255, 255],
            vec![0, 0, 0, 2, 0],
        ] {
            assert!(read_frame(&mut Cursor::new(bytes)).is_err());
        }
        assert_eq!(read_frame(&mut Cursor::new(vec![])).unwrap(), None);
        assert_eq!(
            field(&dispatch(&Value::Null), "status"),
            Some(&keyword("error"))
        );
    }
}
