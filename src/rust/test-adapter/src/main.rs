//! Test-driver transport and adapters for the existing Rust codec. Database
//! calls remain explicitly unsupported until a Rust engine is connected.

use datalevin_codec::bits::{
    self, AvgMode, AvgResult, AvgValue, BigIntegerError, DecimalError, IndexError, IndexInput,
    IntegerWidth, ScalarError, ScalarType, ScalarValue, TupleElement, TupleError, TupleRead,
    VariableType,
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
        "bits/indexable" | "bits/encode-avg" | "bits/decode-avg" | "bits/giant?" => {
            index_operation(operation, arguments)
        }
        "bits/tuple-header" | "bits/encode-tuple" | "bits/decode-tuple" => {
            tuple_operation(operation, arguments)
        }
        "bits/encode-bigdec" | "bits/decode-bigdec" => decimal_operation(operation, arguments),
        "bits/valid-data?" => validation_operation(arguments),
        "bits/type-size" => {
            let [argument] = arguments.as_slice() else {
                return protocol_error("Type size requires exactly one type");
            };
            let Some(value) = value_argument(argument) else {
                return protocol_error("Type size requires a value argument descriptor");
            };
            success(match value {
                Value::Keyword(kind) => bits::type_size(kind)
                    .map(|size| Value::int(size as i64))
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            })
        }
        "bits/variable-header"
        | "bits/encode-variable"
        | "bits/decode-variable"
        | "bits/bigint-length" => variable_operation(operation, arguments),
        "bits/scalar-header" | "bits/encode-scalar" | "bits/decode-scalar" => {
            scalar_operation(operation, arguments)
        }
        "bits/encode-integer" | "bits/decode-integer" => integer_operation(operation, arguments),
        "bits/encode-attr" | "bits/decode-attr" => attr_operation(operation, arguments),
        "bits/encode-ints" | "bits/ints-header" | "bits/decode-ints" => {
            ints_operation(operation, arguments)
        }
        "bits/bitmap" | "bits/bitmap-add" | "bits/bitmap-del" | "bits/encode-bitmap"
        | "bits/decode-bitmap" => bitmap_operation(operation, arguments),
        "bits/bitmap-and" | "bits/bitmap-or" | "bits/bitmaps-and" | "bits/bitmaps-or"
        | "bits/bitmap-empty?" => bitmap_set_operation(operation, arguments),
        "bits/bitmap64"
        | "bits/bitmap64-add"
        | "bits/bitmap64-contains?"
        | "bits/bitmap64-cardinality"
        | "bits/bitmap64-min"
        | "bits/bitmap64-max"
        | "bits/bitmap64->longs" => bitmap64_operation(operation, arguments),
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

fn tuple_error(error: TupleError) -> Value {
    match error {
        TupleError::Empty => map(vec![("empty", Value::Bool(true))]),
        TupleError::Host(index) => map(vec![("host-error", Value::int(index as i64))]),
        TupleError::TooLarge(index) => map(vec![("too-large", Value::int(index as i64))]),
        TupleError::Integer(error) => bigint_error(error),
        TupleError::DecimalRange => java_error(
            "java.lang.AssertionError",
            "Assert failed: Unscaled value of big decimal cannot go beyond the range of\n             [-2^1015, 2^1015-1]\n(< n 128)",
        ),
        TupleError::Scalar(ScalarError::NaN(kind)) => java_error(
            "java.lang.AssertionError",
            &format!(
                "Assert failed: Cannot index NaN\n(not ({}/isNaN x))",
                if kind == ScalarType::Float {
                    "Float"
                } else {
                    "Double"
                }
            ),
        ),
        TupleError::Scalar(ScalarError::InvalidBoolean) => map(vec![
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
        TupleError::Named => java_error(
            "java.lang.ArrayIndexOutOfBoundsException",
            "Index 0 out of bounds for length 0",
        ),
        TupleError::Header(header) => {
            protocol_error(&format!("No matching clause: {}", header as i8))
        }
        TupleError::Buffer => protocol_error("Tuple buffer operation failed"),
        _ => protocol_error("Invalid tuple payload"),
    }
}

fn tuple_operation(operation: &str, arguments: &[Value]) -> Value {
    if operation == "bits/decode-tuple" {
        let [bytes, position, homogeneous, order] = arguments else {
            return protocol_error("Tuple decoding requires bytes, position, shape and byte order");
        };
        let (
            Some(Value::Bytes(bytes)),
            Some(Value::Long(position)),
            Some(Value::Bool(homogeneous)),
            Some(Value::Bool(little)),
        ) = (
            value_argument(bytes),
            value_argument(position),
            value_argument(homogeneous),
            value_argument(order),
        )
        else {
            return protocol_error("Invalid tuple decoding arguments");
        };
        if *position < 0 || *position as u64 > bytes.len() as u64 {
            return protocol_error("Invalid tuple buffer position");
        }
        let (reads, result) = bits::decode_tuple(bytes, *position as usize, *homogeneous, *little);
        return success(map(vec![
            ("reads", read_operations(reads)),
            (
                "result",
                match result {
                    Ok(value) => success(value),
                    Err(error) => tuple_error(error),
                },
            ),
        ]));
    }
    let Some(Some(Value::Vector(types))) = arguments.first().map(value_argument) else {
        return protocol_error("Tuple encoding requires a vector of types");
    };
    let Some(types): Option<Vec<&str>> = types
        .iter()
        .map(|kind| match kind {
            Value::Keyword(kind) => Some(kind.as_ref()),
            _ => None,
        })
        .collect()
    else {
        return unsupported(operation, "Tuple component type is not implemented");
    };
    let Some(header) = bits::tuple_header(&types) else {
        return unsupported(operation, "Tuple component type is not implemented");
    };
    if operation == "bits/tuple-header" {
        return if arguments.len() == 1 {
            success(Value::Bytes(vec![header]))
        } else {
            protocol_error("Tuple header requires one type vector")
        };
    }
    let [_, values, count, order] = arguments else {
        return protocol_error("Tuple encoding requires types, components, count and byte order");
    };
    let (Some(Value::Vector(values)), Some(Value::Long(count)), Some(Value::Bool(little))) = (
        value_argument(values),
        value_argument(count),
        value_argument(order),
    ) else {
        return protocol_error("Invalid tuple encoding arguments");
    };
    let expected = if types.len() == 1 {
        *count
    } else {
        types.len() as i64
    };
    if *count < 0 || expected < 0 || expected as u64 != values.len() as u64 {
        return protocol_error("Tuple component count does not match the input");
    }
    let Some(values): Option<Vec<TupleElement>> = values
        .iter()
        .map(|element| {
            if let Some(Value::Long(index)) = field(element, "error") {
                if *index < 0 {
                    return None;
                }
                Some(TupleElement::HostError {
                    index: *index as usize,
                    lookup: field(element, "lookup?") == Some(&Value::Bool(true)),
                })
            } else {
                field(element, "value").cloned().map(TupleElement::Value)
            }
        })
        .collect()
    else {
        return protocol_error("Invalid tuple component descriptors");
    };
    success(Value::Vector(
        bits::encode_tuple(&types, &values, *count as usize, *little)
            .into_iter()
            .map(|field| match field {
                Ok(bytes) => success(Value::Bytes(bytes)),
                Err(error) => tuple_error(error),
            })
            .collect(),
    ))
}

fn read_operations(reads: Vec<TupleRead>) -> Value {
    Value::Vector(
        reads
            .into_iter()
            .map(|read| {
                Value::Vector(match read {
                    TupleRead::Bytes(size) => vec![keyword("bytes"), Value::Long(size)],
                    TupleRead::Position(position) => {
                        vec![keyword("position"), Value::Long(position)]
                    }
                    TupleRead::Absolute(position, width) => vec![
                        keyword("absolute"),
                        Value::Long(position),
                        Value::Long(width as i64),
                    ],
                    TupleRead::Mark => vec![keyword("mark")],
                    TupleRead::Reset => vec![keyword("reset")],
                    TupleRead::Limit(limit) => vec![keyword("limit"), Value::Long(limit)],
                })
            })
            .collect(),
    )
}

fn tuple_element(value: &Value) -> Option<TupleElement> {
    if let Some(Value::Long(index)) = field(value, "error") {
        if *index < 0 {
            return None;
        }
        Some(TupleElement::HostError {
            index: *index as usize,
            lookup: field(value, "lookup?") == Some(&Value::Bool(true)),
        })
    } else {
        field(value, "value").cloned().map(TupleElement::Value)
    }
}

fn index_error(operation: &str, error: IndexError) -> Value {
    match error {
        IndexError::Tuple(error) => tuple_error(error),
        IndexError::Nippy(error) => codec_error(operation, error),
        IndexError::Unsupported => unsupported(
            operation,
            "Custom or unknown tuple index types are not implemented",
        ),
    }
}

fn avg_value(value: AvgValue, little: bool) -> Result<Value, Value> {
    let (kind, value) = match value {
        AvgValue::Value(value) => ("value", value),
        AvgValue::Custom(bytes) => ("custom", Value::Bytes(bytes)),
        AvgValue::Data(bytes) => {
            if little {
                return Err(protocol_error(
                    "ByteBuffer must use BIG_ENDIAN order for DataInput/DataOutput semantics (have LITTLE_ENDIAN).",
                ));
            }
            let value =
                deserialize(&bytes).map_err(|error| codec_error("bits/decode-avg", error))?;
            ("value", value)
        }
    };
    Ok(map(vec![("kind", keyword(kind)), ("value", value)]))
}

fn index_operation(operation: &str, arguments: &[Value]) -> Value {
    if operation == "bits/giant?" {
        let [argument] = arguments else {
            return protocol_error("Giant check requires one ID");
        };
        let Some(Value::Long(id)) = value_argument(argument) else {
            return protocol_error("Giant ID must be a long");
        };
        return success(Value::Bool(bits::giant(*id)));
    }
    if operation == "bits/indexable" {
        let [value_type, input] = arguments else {
            return protocol_error("Index construction requires type and value");
        };
        let (Some(value_type), Some(input)) = (value_argument(value_type), value_argument(input))
        else {
            return protocol_error("Invalid index input descriptors");
        };
        let input = if let Some(Value::Vector(elements)) = field(input, "elements") {
            let Some(Value::Long(count)) = field(input, "count") else {
                return protocol_error("Tuple index requires a count");
            };
            if *count < 0 {
                return protocol_error("Tuple count cannot be negative");
            }
            let Some(elements) = elements.iter().map(tuple_element).collect() else {
                return protocol_error("Invalid tuple index components");
            };
            IndexInput::Tuple {
                elements,
                count: *count as usize,
            }
        } else if let Some(value) = field(input, "value") {
            IndexInput::Value(value.clone())
        } else {
            return protocol_error("Missing index value");
        };
        return match bits::indexable(value_type, &input) {
            Ok(index) => success(map(vec![
                ("header", Value::Long(i64::from(index.header as i8))),
                (
                    "bytes",
                    index.bytes.map(Value::Bytes).unwrap_or(Value::Null),
                ),
                ("cut?", Value::Bool(index.cut)),
            ])),
            Err(IndexError::Tuple(error)) => success(map(vec![("failure", tuple_error(error))])),
            Err(error) => index_error(operation, error),
        };
    }
    if operation == "bits/encode-avg" {
        let [attribute, header, bytes, input, giant, order] = arguments else {
            return protocol_error(
                "AVG encoding requires attribute, header, bytes, value, giant ID and byte order",
            );
        };
        let (
            Some(Value::Integer(attribute)),
            Some(Value::Long(header)),
            Some(bytes),
            Some(input),
            Some(Value::Long(giant)),
            Some(Value::Bool(little)),
        ) = (
            value_argument(attribute),
            value_argument(header),
            value_argument(bytes),
            value_argument(input),
            value_argument(giant),
            value_argument(order),
        )
        else {
            return protocol_error("Invalid AVG encoding arguments");
        };
        let bytes = match bytes {
            Value::Null => None,
            Value::Bytes(bytes) => Some(bytes.as_slice()),
            _ => return protocol_error("AVG payload must be bytes or nil"),
        };
        let Some(value) = tuple_element(input) else {
            return protocol_error("Invalid AVG component descriptor");
        };
        return success(Value::Vector(
            bits::encode_avg(*attribute, *header, bytes, &value, *giant, *little)
                .into_iter()
                .map(|field| match field {
                    Ok(bytes) => success(Value::Bytes(bytes)),
                    Err(error) => tuple_error(error),
                })
                .collect(),
        ));
    }
    let [bytes, position, mode, order] = arguments else {
        return protocol_error("AVG decoding requires bytes, position, operation and byte order");
    };
    let (
        Some(Value::Bytes(bytes)),
        Some(Value::Long(position)),
        Some(Value::Keyword(mode)),
        Some(Value::Bool(little)),
    ) = (
        value_argument(bytes),
        value_argument(position),
        value_argument(mode),
        value_argument(order),
    )
    else {
        return protocol_error("Invalid AVG decoding arguments");
    };
    if *position < 0 || *position as u64 > bytes.len() as u64 {
        return protocol_error("Invalid AVG buffer position");
    }
    let mode = match mode.as_ref() {
        "avg" => AvgMode::Avg,
        "r" => AvgMode::Retrieved,
        "aid" => AvgMode::Attribute,
        "giant-id" => AvgMode::Giant,
        "inline" => AvgMode::Inline,
        _ => return protocol_error("Unknown AVG reader"),
    };
    let (reads, result) = bits::decode_avg(bytes, *position as usize, mode, *little);
    let result = result
        .map_err(|error| index_error(operation, error))
        .and_then(|result| match result {
            AvgResult::Value(value) => avg_value(value, *little),
            AvgResult::Retrieved {
                attribute,
                value,
                giant,
            } => avg_value(value, *little).map(|value| {
                map(vec![
                    ("retrieved?", Value::Bool(true)),
                    ("a", attribute.map(Value::Integer).unwrap_or(Value::Null)),
                    ("v", value),
                    ("g", Value::Long(giant)),
                ])
            }),
        });
    success(map(vec![
        ("reads", read_operations(reads)),
        (
            "result",
            match result {
                Ok(value) => success(value),
                Err(error) => error,
            },
        ),
    ]))
}

fn validation_value(value: &Value) -> Option<bits::ValidationValue<'_>> {
    let Value::Keyword(kind) = field(value, "kind")? else {
        return None;
    };
    if kind.as_ref() == "number" {
        let number = field(value, "value")?;
        if !matches!(
            number,
            Value::Byte(_)
                | Value::Short(_)
                | Value::Integer(_)
                | Value::Long(_)
                | Value::Float(_)
                | Value::Double(_)
                | Value::BigInt(_)
                | Value::BigInteger(_)
                | Value::BigDecimal { .. }
                | Value::Ratio { .. }
        ) {
            return None;
        }
        let floating = match field(value, "floating")? {
            Value::Double(floating) => Ok(f64::from_bits(*floating)),
            error => {
                let Value::Long(index) = field(error, "host-error")? else {
                    return None;
                };
                Err(usize::try_from(*index).ok()?)
            }
        };
        let decimal = match field(value, "decimal") {
            None => None,
            Some(value @ Value::BigDecimal { .. }) => Some(Ok(value)),
            Some(error) => {
                let Value::Long(index) = field(error, "host-error")? else {
                    return None;
                };
                Some(Err(usize::try_from(*index).ok()?))
            }
        };
        return Some(bits::ValidationValue::Number {
            value: number,
            floating,
            decimal,
        });
    }
    let Value::Long(index) = field(value, "host-error")? else {
        return None;
    };
    let index = usize::try_from(*index).ok()?;
    if kind.as_ref() == "vector" {
        let values = match field(value, "items") {
            None => Vec::new(),
            Some(Value::Vector(values)) => values
                .iter()
                .map(validation_value)
                .collect::<Option<Vec<_>>>()?,
            _ => return None,
        };
        return Some(bits::ValidationValue::Vector(values, index));
    }
    if kind.as_ref() == "unsupported-number" {
        return Some(bits::ValidationValue::UnsupportedNumber);
    }
    if !matches!(
        kind.as_ref(),
        "nil"
            | "other"
            | "string"
            | "bytes"
            | "keyword"
            | "symbol"
            | "boolean"
            | "instant"
            | "uuid"
    ) {
        return None;
    }
    Some(bits::ValidationValue::Kind(kind.as_ref(), index))
}

fn validation_operation(arguments: &[Value]) -> Value {
    let [input, kind] = arguments else {
        return protocol_error("Validation requires a value and type");
    };
    let (Some(input), Some(kind)) = (value_argument(input), value_argument(kind)) else {
        return protocol_error("Invalid validation argument descriptors");
    };
    let Some(input) = validation_value(input) else {
        return protocol_error("Invalid validation value shape");
    };
    match bits::valid_data(&input, kind) {
        Ok(result) => success(result.map(Value::Bool).unwrap_or(Value::Null)),
        Err(bits::ValidationError::Host(index)) => {
            success(map(vec![("host-error", Value::Long(index as i64))]))
        }
        Err(bits::ValidationError::UnsupportedNumber) => unsupported(
            "bits/valid-data?",
            "Custom number validation is not implemented",
        ),
        Err(bits::ValidationError::InvalidNumber) => {
            protocol_error("Invalid validation numeric value")
        }
    }
}

fn bitmap64_operation(operation: &str, arguments: &[Value]) -> Value {
    let expected = if matches!(operation, "bits/bitmap64-add" | "bits/bitmap64-contains?") {
        2
    } else {
        1
    };
    if arguments.len() != expected {
        return protocol_error("64-bit bitmap operation has the wrong argument count");
    }
    let Some(Some(Value::LongArray(values))) = arguments.first().map(value_argument) else {
        return protocol_error("64-bit bitmap operation requires a long array");
    };
    let mut bitmap = bits::bitmap64(values);
    match operation {
        "bits/bitmap64-add" | "bits/bitmap64-contains?" => {
            let Some(Value::Long(member)) = value_argument(&arguments[1]) else {
                return protocol_error("64-bit bitmap member must be a long");
            };
            if operation == "bits/bitmap64-contains?" {
                return success(Value::Bool(bitmap.contains(*member as u64)));
            }
            bitmap.insert(*member as u64);
        }
        "bits/bitmap64-cardinality" => return success(Value::Long(bitmap.len() as i64)),
        "bits/bitmap64-min" | "bits/bitmap64-max" => {
            let member = if operation == "bits/bitmap64-min" {
                bitmap.min()
            } else {
                bitmap.max()
            };
            return match member {
                Some(member) => success(Value::Long(member as i64)),
                None => java_error("java.util.NoSuchElementException", "Empty HighLowContainer"),
            };
        }
        _ => {}
    }
    if bitmap.len() as usize > Limits::default().max_collection_len {
        return unsupported(
            operation,
            "64-bit bitmap cardinality exceeds the adapter transport limit",
        );
    }
    success(Value::LongArray(
        bitmap.iter().map(|member| member as i64).collect(),
    ))
}

fn bitmap_set_operation(operation: &str, arguments: &[Value]) -> Value {
    let result = match operation {
        "bits/bitmap-empty?" => {
            let [argument] = arguments else {
                return protocol_error("Bitmap empty check requires exactly one bitmap");
            };
            let Some(Value::Bitmap(bitmap)) = value_argument(argument) else {
                return protocol_error("Bitmap empty check requires a bitmap");
            };
            return success(Value::Bool(bitmap.is_empty()));
        }
        "bits/bitmap-and" | "bits/bitmap-or" => {
            let [left, right] = arguments else {
                return protocol_error("Bitmap set operation requires exactly two bitmaps");
            };
            let (Some(Value::Bitmap(left)), Some(Value::Bitmap(right))) =
                (value_argument(left), value_argument(right))
            else {
                return protocol_error("Bitmap set operation requires bitmap values");
            };
            Some(if operation == "bits/bitmap-and" {
                bits::bitmap_and(left, right)
            } else {
                bits::bitmap_or(left, right)
            })
        }
        _ => {
            let [argument] = arguments else {
                return protocol_error("Bitmap aggregation requires exactly one vector");
            };
            let Some(Value::Vector(values)) = value_argument(argument) else {
                return protocol_error("Bitmap aggregation requires a vector of bitmaps");
            };
            let Some(bitmaps) = values
                .iter()
                .map(|value| match value {
                    Value::Bitmap(bitmap) => Some(bitmap),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
            else {
                return protocol_error("Bitmap aggregation requires bitmap values");
            };
            if operation == "bits/bitmaps-and" {
                bits::bitmaps_and(&bitmaps)
            } else {
                bits::bitmaps_or(&bitmaps)
            }
        }
    };
    let Some(bitmap) = result else {
        return success(Value::Null);
    };
    if bitmap.len() as usize > Limits::default().max_collection_len {
        return unsupported(
            operation,
            "Bitmap cardinality exceeds the adapter transport limit",
        );
    }
    success(Value::Bitmap(bitmap))
}

fn bitmap_operation(operation: &str, arguments: &[Value]) -> Value {
    if operation == "bits/bitmap" {
        let [argument] = arguments else {
            return protocol_error("Bitmap construction requires one integer array");
        };
        let Some(Value::IntArray(values)) = value_argument(argument) else {
            return protocol_error("Bitmap construction requires an integer array");
        };
        return success(Value::Bitmap(bits::bitmap(values)));
    }
    if operation == "bits/decode-bitmap" {
        let [argument] = arguments else {
            return protocol_error("Bitmap decoding requires one byte array");
        };
        let Some(Value::Bytes(bytes)) = value_argument(argument) else {
            return protocol_error("Bitmap decoding requires a byte array");
        };
        let (reads, result) = bits::decode_bitmap(bytes, Limits::default().max_collection_len);
        let result = match result {
            Ok(value) => success(value),
            Err(bits::BitmapError::Buffer) => protocol_error("Bitmap buffer operation failed"),
            Err(bits::BitmapError::Cookie(cookie)) => java_error(
                "java.io.IOException",
                &format!(
                    "org.roaringbitmap.InvalidRoaringFormat: I failed to find one of the right cookies. {cookie}"
                ),
            ),
            Err(bits::BitmapError::Count(count)) => {
                if count < 0 {
                    java_error("java.lang.NegativeArraySizeException", &count.to_string())
                } else {
                    java_error(
                        "java.io.IOException",
                        "org.roaringbitmap.InvalidRoaringFormat: Size too large",
                    )
                }
            }
            Err(bits::BitmapError::Limit) => unsupported(
                operation,
                "Bitmap cardinality exceeds the adapter transport limit",
            ),
            Err(bits::BitmapError::Invalid) => unsupported(
                operation,
                "Malformed Roaring container values are not mapped yet",
            ),
        };
        return success(map(vec![
            ("reads", read_operations(reads)),
            ("result", result),
        ]));
    }
    let Some(Some(Value::Bitmap(bitmap))) = arguments.first().map(value_argument) else {
        return protocol_error("Bitmap operation requires a bitmap value");
    };
    if operation == "bits/encode-bitmap" {
        if arguments.len() != 1 {
            return protocol_error("Bitmap encoding requires exactly one bitmap");
        }
        if bitmap.serialized_size() > (MAX_FRAME_BYTES - 1024) / 4 {
            return unsupported(operation, "Bitmap writes exceed the adapter frame limit");
        }
        let writes = bits::encode_bitmap(bitmap);
        let chunks =
            |chunks: Vec<Vec<u8>>| Value::Vector(chunks.into_iter().map(Value::Bytes).collect());
        return success(map(vec![
            ("header", chunks(writes.header)),
            (
                "containers",
                Value::Vector(writes.containers.into_iter().map(chunks).collect()),
            ),
        ]));
    }
    let [_, member] = arguments else {
        return protocol_error("Bitmap mutation requires a bitmap and integer");
    };
    let Some(Value::Integer(member)) = value_argument(member) else {
        return protocol_error("Bitmap mutation requires an integer");
    };
    let mut bitmap = bitmap.clone();
    if operation == "bits/bitmap-add" {
        bitmap.insert(*member as u32);
    } else {
        bitmap.remove(*member as u32);
    }
    if bitmap.len() as usize > Limits::default().max_collection_len {
        return unsupported(
            operation,
            "Bitmap cardinality exceeds the adapter transport limit",
        );
    }
    success(Value::Bitmap(bitmap))
}

fn ints_operation(operation: &str, arguments: &[Value]) -> Value {
    if operation == "bits/ints-header" {
        let [bytes, order] = arguments else {
            return protocol_error("Integer array header requires bytes and byte order");
        };
        let (Some(Value::Bytes(bytes)), Some(Value::Bool(little))) =
            (value_argument(bytes), value_argument(order))
        else {
            return protocol_error("Invalid integer array header arguments");
        };
        let Ok(bytes) = bytes.as_slice().try_into() else {
            return protocol_error("Integer array header requires four bytes");
        };
        let (size, compressed) = bits::ints_header(bytes, *little);
        return success(map(vec![
            ("size", Value::Long(i64::from(size))),
            ("compressed?", Value::Bool(compressed)),
        ]));
    }
    if operation == "bits/encode-ints" {
        let [values, sorted, order] = arguments else {
            return protocol_error(
                "Integer array encoding requires values, sorted mode and byte order",
            );
        };
        let (Some(Value::IntArray(values)), Some(Value::Bool(sorted)), Some(Value::Bool(little))) = (
            value_argument(values),
            value_argument(sorted),
            value_argument(order),
        ) else {
            return protocol_error("Invalid integer array encoding arguments");
        };
        let words = bits::encode_ints(values, *sorted, *little);
        if words.len() > (MAX_FRAME_BYTES - 1024) / 8 {
            return unsupported(
                operation,
                "Integer array writes exceed the adapter frame limit",
            );
        }
        return success(Value::Vector(
            words
                .into_iter()
                .map(|word| Value::Bytes(word.to_vec()))
                .collect(),
        ));
    }
    let [bytes, compressed, sorted, order] = arguments else {
        return protocol_error(
            "Integer array decoding requires bytes, compression, sorted mode and byte order",
        );
    };
    let (
        Some(Value::Bytes(bytes)),
        Some(Value::Bool(compressed)),
        Some(Value::Bool(sorted)),
        Some(Value::Bool(little)),
    ) = (
        value_argument(bytes),
        value_argument(compressed),
        value_argument(sorted),
        value_argument(order),
    )
    else {
        return protocol_error("Invalid integer array decoding arguments");
    };
    match bits::decode_ints(
        bytes,
        *compressed,
        *sorted,
        *little,
        (MAX_FRAME_BYTES - 1024) / 4,
    ) {
        Ok(values) => success(Value::IntArray(values)),
        Err(bits::IntArrayError::NegativeSize(size)) => {
            java_error("java.lang.NegativeArraySizeException", &size.to_string())
        }
        Err(bits::IntArrayError::EmptySortedBlock) => java_error(
            "java.lang.ArrayIndexOutOfBoundsException",
            "Index -1 out of bounds for length 0",
        ),
        Err(bits::IntArrayError::Limit) => unsupported(
            operation,
            "Expanded integer array exceeds the adapter frame limit",
        ),
        Err(bits::IntArrayError::Compressed(error)) => unsupported(
            operation,
            &format!("Malformed JavaFastPFOR error mapping is not implemented: {error:?}"),
        ),
        Err(bits::IntArrayError::Length) => protocol_error("Invalid integer array payload length"),
    }
}

fn attr_operation(operation: &str, arguments: &[Value]) -> Value {
    let [argument] = arguments else {
        return protocol_error("Attribute codec requires exactly one value");
    };
    match (operation, value_argument(argument)) {
        ("bits/encode-attr", Some(Value::CharArray(units))) => match bits::encode_attr(units) {
            Ok(bytes) => success(Value::Bytes(bytes)),
            Err(_) => java_error(
                "java.lang.AssertionError",
                "Assert failed: Attribute cannot be longer than 511 bytes\n(<= al c/+max-key-size+)",
            ),
        },
        ("bits/decode-attr", Some(Value::Bytes(bytes))) => {
            success(keyword(&bits::decode_attr(bytes)))
        }
        _ => protocol_error("Attribute encoding requires UTF-16; decoding requires bytes"),
    }
}

fn variable_type(kind: &str) -> Option<VariableType> {
    Some(match kind {
        "string" => VariableType::String,
        "bytes" => VariableType::Bytes,
        "keyword" => VariableType::Keyword,
        "symbol" => VariableType::Symbol,
        "bigint" => VariableType::BigInteger,
        "bigdec" => VariableType::BigDecimal,
        _ => return None,
    })
}

fn variable_operation(operation: &str, arguments: &[Value]) -> Value {
    if operation == "bits/bigint-length" {
        let [argument] = arguments else {
            return protocol_error("BigInteger length requires one prefix");
        };
        let Some(Value::Bytes(prefix)) = value_argument(argument) else {
            return protocol_error("BigInteger length requires a byte array");
        };
        let [prefix] = prefix.as_slice() else {
            return protocol_error("BigInteger length requires exactly one byte");
        };
        return success(Value::int(bits::bigint_length(*prefix) as i64));
    }
    if operation == "bits/variable-header" {
        let [argument] = arguments else {
            return protocol_error("Variable header requires one type");
        };
        let Some(Value::Keyword(kind)) = value_argument(argument) else {
            return protocol_error("Variable header requires a keyword");
        };
        return match variable_type(kind) {
            Some(kind) => success(Value::Bytes(vec![kind.header()])),
            None => unsupported(operation, "Variable storage type is not implemented"),
        };
    }
    let [value, kind] = arguments else {
        return protocol_error("Variable codec requires value and type arguments");
    };
    let (Some(value), Some(Value::Keyword(kind))) = (value_argument(value), value_argument(kind))
    else {
        return protocol_error("Invalid variable codec argument descriptors");
    };
    let Some(kind) = variable_type(kind) else {
        return unsupported(operation, "Variable storage type is not implemented");
    };
    if operation == "bits/encode-variable" {
        let bytes = match (kind, value) {
            (VariableType::String, Value::CharArray(units)) => bits::encode_utf16(units),
            (VariableType::Bytes, Value::Bytes(bytes)) => bytes.clone(),
            (VariableType::BigInteger, Value::Bytes(bytes)) => match bits::encode_bigint(bytes) {
                Ok(bytes) => bytes,
                Err(error) => return bigint_error(error),
            },
            (VariableType::Keyword | VariableType::Symbol, Value::Vector(parts)) => {
                let [Value::CharArray(name), namespace] = parts.as_slice() else {
                    return protocol_error("Named values require name and namespace UTF-16 arrays");
                };
                let namespace = match namespace {
                    Value::Null => None,
                    Value::CharArray(units) => Some(units.as_slice()),
                    _ => return protocol_error("Named namespace requires UTF-16 or nil"),
                };
                bits::encode_named(name, namespace)
            }
            _ => return protocol_error("Incorrect value for variable storage type"),
        };
        success(Value::Bytes(bytes))
    } else {
        let Value::Bytes(bytes) = value else {
            return protocol_error("Variable decoding requires a byte array");
        };
        success(match kind {
            VariableType::String => Value::Text(bits::decode_utf8(bytes)),
            VariableType::Bytes => Value::Bytes(bytes.clone()),
            VariableType::BigInteger => match bits::decode_bigint(bytes) {
                Ok(bytes) => Value::BigInteger(bytes),
                Err(error) => return bigint_error(error),
            },
            VariableType::BigDecimal => {
                return protocol_error("BigDecimal requires its composite field codec");
            }
            VariableType::Keyword | VariableType::Symbol => {
                let Some(name) = bits::decode_named(bytes) else {
                    return java_error(
                        "java.lang.ArrayIndexOutOfBoundsException",
                        "Index 0 out of bounds for length 0",
                    );
                };
                if kind == VariableType::Keyword {
                    keyword(&name)
                } else {
                    Value::Symbol(name)
                }
            }
        })
    }
}

fn decimal_error(error: DecimalError) -> Value {
    match error {
        DecimalError::Integer(error) => bigint_error(error),
        DecimalError::Scalar(ScalarError::NaN(_)) => java_error(
            "java.lang.AssertionError",
            "Assert failed: Cannot index NaN\n(not (Double/isNaN x))",
        ),
        _ => protocol_error("Invalid BigDecimal payload"),
    }
}

fn decimal_operation(operation: &str, arguments: &[Value]) -> Value {
    if operation == "bits/encode-bigdec" {
        let [unscaled, scale, order] = arguments else {
            return protocol_error(
                "BigDecimal encoding requires unscaled bytes, scale and byte order",
            );
        };
        let (
            Some(Value::Bytes(unscaled)),
            Some(Value::Integer(scale)),
            Some(Value::Bool(little_endian)),
        ) = (
            value_argument(unscaled),
            value_argument(scale),
            value_argument(order),
        )
        else {
            return protocol_error("Invalid BigDecimal encoding arguments");
        };
        let mut writes = Vec::new();
        for field in bits::decimal_fields(unscaled, *scale, *little_endian) {
            match field {
                Ok(bytes) => writes.push(success(Value::Bytes(bytes))),
                Err(error) => {
                    writes.push(decimal_error(error));
                    break;
                }
            }
        }
        success(Value::Vector(writes))
    } else {
        let [bytes, order] = arguments else {
            return protocol_error("BigDecimal decoding requires payload bytes and byte order");
        };
        let (Some(Value::Bytes(bytes)), Some(Value::Bool(little_endian))) =
            (value_argument(bytes), value_argument(order))
        else {
            return protocol_error("Invalid BigDecimal decoding arguments");
        };
        match bits::decode_decimal(bytes, *little_endian) {
            Ok((unscaled, scale)) => success(Value::BigDecimal { unscaled, scale }),
            Err(error) => decimal_error(error),
        }
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
                    keyword("bits/variable-header"),
                    keyword("bits/encode-variable"),
                    keyword("bits/decode-variable"),
                    keyword("bits/encode-attr"),
                    keyword("bits/decode-attr"),
                    keyword("bits/encode-ints"),
                    keyword("bits/ints-header"),
                    keyword("bits/decode-ints"),
                    keyword("bits/bitmap"),
                    keyword("bits/bitmap-add"),
                    keyword("bits/bitmap-del"),
                    keyword("bits/bitmap-and"),
                    keyword("bits/bitmap-or"),
                    keyword("bits/bitmaps-and"),
                    keyword("bits/bitmaps-or"),
                    keyword("bits/bitmap-empty?"),
                    keyword("bits/bitmap64"),
                    keyword("bits/bitmap64-add"),
                    keyword("bits/bitmap64-contains?"),
                    keyword("bits/bitmap64-cardinality"),
                    keyword("bits/bitmap64-min"),
                    keyword("bits/bitmap64-max"),
                    keyword("bits/bitmap64->longs"),
                    keyword("bits/encode-bitmap"),
                    keyword("bits/decode-bitmap"),
                    keyword("bits/bigint-length"),
                    keyword("bits/encode-bigdec"),
                    keyword("bits/decode-bigdec"),
                    keyword("bits/type-size"),
                    keyword("bits/valid-data?"),
                    keyword("bits/tuple-header"),
                    keyword("bits/encode-tuple"),
                    keyword("bits/decode-tuple"),
                    keyword("bits/indexable"),
                    keyword("bits/encode-avg"),
                    keyword("bits/decode-avg"),
                    keyword("bits/giant?"),
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
        for operation in [
            "bits/bitmap-and",
            "bits/bitmap-or",
            "bits/bitmaps-and",
            "bits/bitmaps-or",
            "bits/bitmap-empty?",
            "bits/bitmap64",
            "bits/bitmap64-add",
            "bits/bitmap64-contains?",
            "bits/bitmap64-cardinality",
            "bits/bitmap64-min",
            "bits/bitmap64-max",
            "bits/bitmap64->longs",
            "bits/valid-data?",
        ] {
            assert_eq!(
                field(&invoke(operation, Value::Null), "status"),
                Some(&keyword("error"))
            );
            let request = map(vec![
                ("op", keyword(operation)),
                ("args", Value::Vector(vec![])),
            ]);
            assert_eq!(
                field(&dispatch(&request), "status"),
                Some(&keyword("error"))
            );
        }
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
