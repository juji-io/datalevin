//! Compression generations and immutable dictionaries used when opening stores.

use crate::{hu, nippy::Value};
use sha2::{Digest, Sha256};
use std::{fs, io, path::Path};

pub enum Error {
    Behavior { message: &'static str, data: Value },
    Dictionary(hu::DictionaryError),
    Io(io::Error),
    ZstdSetup,
}

pub struct Opened {
    pub manifest: Value,
    pub key_codec: Option<hu::Dictionary>,
    pub value_dictionary: Option<Vec<u8>>,
    pub key_compress: Value,
    pub val_compress: Value,
}

fn kw(name: &str) -> Value {
    Value::Keyword(name.into())
}

fn bare(value: &Value) -> &Value {
    match value {
        Value::Meta { value, .. } => bare(value),
        _ => value,
    }
}

fn entries(value: &Value) -> &[(Value, Value)] {
    match bare(value) {
        Value::Map(entries) | Value::SortedMap(entries) => entries,
        _ => &[],
    }
}

fn get(value: &Value, name: &str) -> Value {
    entries(value)
        .iter()
        .find(|(key, _)| bare(key) == &kw(name))
        .map(|(_, value)| value.clone())
        .unwrap_or(Value::Null)
}

fn contains(value: &Value, name: &str) -> bool {
    entries(value).iter().any(|(key, _)| bare(key) == &kw(name))
}

fn assoc(mut value: Value, name: &str, field: Value) -> Value {
    if let Value::Meta { value, .. } = &mut value {
        **value = assoc((**value).clone(), name, field);
    } else if let Value::Map(entries) | Value::SortedMap(entries) = &mut value {
        if let Some((_, value)) = entries.iter_mut().find(|(key, _)| bare(key) == &kw(name)) {
            *value = field;
        } else {
            entries.push((kw(name), field));
        }
    } else {
        value = map(vec![(name, field)]);
    }
    value
}

fn map(fields: Vec<(&str, Value)>) -> Value {
    Value::Map(
        fields
            .into_iter()
            .map(|(name, value)| (kw(name), value))
            .collect(),
    )
}

fn truthy(value: &Value) -> bool {
    !matches!(bare(value), Value::Null | Value::Bool(false))
}

fn error(category: &str, message: &'static str, fields: Vec<(&str, Value)>) -> Error {
    Error::Behavior {
        message,
        data: assoc(map(fields), "error", kw(category)),
    }
}

fn mode(options: &Value, option: &str, supported: &str) -> Result<Value, Error> {
    let method = get(options, option);
    if bare(&method) == &Value::Null || bare(&method) == &kw("none") {
        Ok(kw("none"))
    } else if bare(&method) == &kw(supported) {
        Ok(method)
    } else {
        Err(error(
            "compression/unsupported",
            "Unsupported compression method",
            vec![("option", kw(option)), ("method", method)],
        ))
    }
}

fn valid_descriptor(descriptor: &Value, supported: &str) -> bool {
    let method = get(descriptor, "method");
    if method == kw("none") {
        return true;
    }
    method == kw(supported)
        && matches!(bare(&get(descriptor, "sha256")), Value::Text(hash)
        if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
}

fn has_flag(flags: &Value, flag: &str) -> bool {
    match bare(flags) {
        Value::Vector(flags)
        | Value::List(flags)
        | Value::Seq(flags)
        | Value::Set(flags)
        | Value::SortedSet(flags) => flags.iter().any(|value| bare(value) == &kw(flag)),
        _ => false,
    }
}

fn load_dictionary(
    dir: &Path,
    stream: &str,
    descriptor: Value,
) -> Result<(Value, Option<Vec<u8>>), Error> {
    if get(&descriptor, "method") == kw("none") {
        return Ok((descriptor, None));
    }
    let file = dir.join(if stream == "key" {
        "keycode.bin"
    } else {
        "valcode.bin"
    });
    let file_name = Value::Text(file.to_string_lossy().into_owned());
    if !file.is_file() {
        return Err(error(
            "compression/missing-dictionary",
            "Required compression dictionary is missing",
            vec![("stream", kw(stream)), ("file", file_name)],
        ));
    }
    // Digest and construct from one owned read, so replacing the path cannot
    // cause a different dictionary to be used after generation validation.
    let bytes = fs::read(file).map_err(Error::Io)?;
    let actual = Value::Text(format!("{:x}", Sha256::digest(&bytes)));
    let expected = get(&descriptor, "sha256");
    if truthy(&expected) && expected != actual {
        return Err(error(
            "compression/dictionary-mismatch",
            "Compression dictionary checksum does not match the stored generation",
            vec![
                ("stream", kw(stream)),
                ("file", file_name),
                ("expected", expected),
                ("actual", actual),
            ],
        ));
    }
    Ok((assoc(descriptor, "sha256", actual), Some(bytes)))
}

/// Validate modes and persisted metadata before reading either dictionary.
/// The binding supplies a fresh UUID for stores without a stored generation.
pub fn open(
    dir: &Path,
    options: &Value,
    loaded_info: &Value,
    generation: String,
) -> Result<Opened, Error> {
    let key_mode = mode(options, "key-compress", "hu")?;
    let val_mode = mode(options, "val-compress", "zstd")?;
    let stored = get(loaded_info, "compression");
    if !entries(loaded_info).is_empty() && bare(&stored) == &Value::Null {
        // The reference validates both legacy options before deciding whether
        // any requested/stored mode requires rebuilding the environment.
        let legacy_key_mode = mode(loaded_info, "key-compress", "hu")?;
        let legacy_val_mode = mode(loaded_info, "val-compress", "zstd")?;
        let changed = key_mode != kw("none")
            || val_mode != kw("none")
            || legacy_key_mode != kw("none")
            || legacy_val_mode != kw("none");
        if changed {
            return Err(error(
                "compression/rebuild-required",
                "Changing compression requires rebuilding into a new environment",
                vec![],
            ));
        }
    }
    let manifest = if truthy(&stored) {
        if !matches!(bare(&get(&stored, "generation")), Value::Text(generation) if !generation.is_empty())
            || !valid_descriptor(&get(&stored, "key"), "hu")
            || !valid_descriptor(&get(&stored, "value"), "zstd")
        {
            return Err(error(
                "compression/invalid-manifest",
                "Invalid or unsupported compression manifest",
                vec![],
            ));
        }
        for (option, stream, requested) in [
            ("key-compress", "key", key_mode.clone()),
            ("val-compress", "value", val_mode.clone()),
        ] {
            let method = get(&get(&stored, stream), "method");
            if contains(options, option) && requested != method {
                return Err(error(
                    "compression/mode-conflict",
                    "Compression option conflicts with the stored generation; rebuild required",
                    vec![
                        ("option", kw(option)),
                        ("requested", requested),
                        ("stored", method),
                    ],
                ));
            }
        }
        stored
    } else {
        map(vec![
            ("generation", Value::Text(generation)),
            ("key", map(vec![("method", key_mode)])),
            ("value", map(vec![("method", val_mode)])),
        ])
    };
    if get(&get(&manifest, "value"), "method") == kw("zstd") {
        for (dbi, options) in entries(&get(loaded_info, "dbis")) {
            let flags = get(options, "flags");
            if has_flag(&flags, "dupsort") && !has_flag(&flags, "dupfixed") {
                return Err(error(
                    "compression/ordered-duplicates",
                    "Value compression is not supported on ordered duplicate values",
                    vec![("dbi", dbi.clone())],
                ));
            }
        }
    }
    let (key_descriptor, key_bytes) = load_dictionary(dir, "key", get(&manifest, "key"))?;
    let key_codec = key_bytes
        .as_deref()
        .map(hu::Dictionary::load)
        .transpose()
        .map_err(Error::Dictionary)?;
    let (value_descriptor, value_dictionary) =
        load_dictionary(dir, "value", get(&manifest, "value"))?;
    if value_dictionary
        .as_deref()
        .is_some_and(|bytes| !super::valid_dictionary(bytes))
    {
        return Err(Error::ZstdSetup);
    }
    let key_method = get(&key_descriptor, "method");
    let val_method = get(&value_descriptor, "method");
    Ok(Opened {
        manifest: assoc(
            assoc(manifest, "key", key_descriptor),
            "value",
            value_descriptor,
        ),
        key_codec,
        value_dictionary,
        key_compress: if key_method == kw("none") {
            Value::Null
        } else {
            key_method
        },
        val_compress: if val_method == kw("none") {
            Value::Null
        } else {
            val_method
        },
    })
}
