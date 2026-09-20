//! Reads the GVAS JSON dialect into uesave's property tree.
//!
//! The dialect is plain UE GVAS structurally: every property is an object with a
//! `type` discriminator and a `value`. Game-specific `RawData` payloads are
//! dispatched to the game through [`crate::Game::game_struct_from_gvas_json`].

use crate::Error;

/// Builds an [`Error::GvasJson`] tagged with the property path that produced it.
pub(crate) fn err(path: &str, message: impl Into<String>) -> Error {
    Error::GvasJson { path: path.to_string(), message: message.into() }
}

/// Appends a segment to a dotted property path, matching how the binary reader
/// threads paths. The game hook and uesave's hint table are both keyed on this.
pub(crate) fn join(path: &str, segment: &str) -> String {
    if path.is_empty() { segment.to_string() } else { format!("{path}.{segment}") }
}

use crate::{
    Byte, ByteArray, FGuid, Game, MapEntry, Properties, Property, PropertyKey, Result,
    SaveGameArchiveType, StructValue, ValueVec,
};
use serde_json::Value;

/// The `type` discriminator every dialect property carries.
fn type_of<'a>(value: &'a Value, path: &str) -> Result<&'a str> {
    value
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| err(path, "property object has no \"type\""))
}

fn field<'a>(value: &'a Value, key: &str, path: &str) -> Result<&'a Value> {
    value.get(key).ok_or_else(|| err(path, format!("property has no {key:?}")))
}

fn as_str(value: &Value, path: &str) -> Result<String> {
    value.as_str().map(str::to_string).ok_or_else(|| err(path, "expected a string"))
}

/// Pulls a sibling property out of a dialect object, removing it. Used where uesave
/// folds a neighbouring property into a game struct rather than keeping it separate.
pub(crate) fn take_sibling(entry: &mut Value, key: &str) -> Option<Value> {
    entry.as_object_mut()?.remove(key)
}

/// Reads a dialect object of `{name: property}` into [`Properties`].
pub fn properties_from_json<G: Game>(
    value: &Value,
    path: &str,
) -> Result<Properties<SaveGameArchiveType<G>>> {
    // `PalMapConcreteModelModule` absorbs its sibling `CustomVersionData` into
    // `custom_version_data`. Base camp module entries have the identical shape (a
    // decoded `RawData` beside a `CustomVersionData` sibling) but their struct has
    // nowhere to put it, so the lift is gated on the path rather than the shape:
    // only a concrete model's ModuleMap folds the sibling in, and it is then
    // removed here so the walker below does not also emit it as its own property.
    let mut owned;
    let value = if path.ends_with("ConcreteModel.ModuleMap") {
        owned = value.clone();
        // Only decoded payloads have somewhere to receive the lifted bytes: an
        // opaque payload still holding "values" takes the ByteProperty arm's
        // values branch below and would ignore an injected key, silently
        // dropping the sibling's bytes. Leave the sibling in place instead.
        let decoded = owned
            .get("RawData")
            .and_then(|raw| raw.get("value"))
            .is_some_and(|payload| payload.get("values").is_none());
        if decoded {
            if let Some(sibling) = take_sibling(&mut owned, "CustomVersionData") {
                let bytes = sibling.get("value").and_then(|v| v.get("values")).cloned();
                if let (Some(bytes), Some(raw)) = (bytes, owned.get_mut("RawData")) {
                    if let Some(payload) = raw.get_mut("value") {
                        payload["custom_version_data"] = bytes;
                    }
                }
            }
        }
        &owned
    } else {
        value
    };

    let object = value.as_object().ok_or_else(|| err(path, "expected a property object"))?;
    let mut properties = Properties::default();
    for (key, child) in object {
        properties.insert(
            PropertyKey::from(key.as_str()),
            property_from_json::<G>(child, &join(path, key))?,
        );
    }
    Ok(properties)
}

fn engine_struct<T: serde::de::DeserializeOwned>(value: &Value, path: &str) -> Result<T> {
    serde_json::from_value(value.clone()).map_err(|e| err(path, e.to_string()))
}

fn byte_array(values: &Value, path: &str) -> Result<Vec<u8>> {
    let items = values.as_array().ok_or_else(|| err(path, "expected a byte array"))?;
    items
        .iter()
        .map(|item| {
            item.as_u64()
                .filter(|byte| *byte <= u8::MAX as u64)
                .map(|byte| byte as u8)
                .ok_or_else(|| err(path, "byte array holds a non-byte"))
        })
        .collect()
}

/// Reads one dialect property object into a [`Property`].
pub fn property_from_json<G: Game>(
    value: &Value,
    path: &str,
) -> Result<Property<SaveGameArchiveType<G>>> {
    let property_type = type_of(value, path)?;
    let inner = || field(value, "value", path);

    Ok(match property_type {
        "IntProperty" => Property::Int({
            let value = inner()?.as_i64().ok_or_else(|| err(path, "expected an integer"))?;
            i32::try_from(value).map_err(|_| err(path, format!("integer {value} does not fit in i32")))?
        }),
        "Int64Property" => Property::Int64(
            inner()?.as_i64().ok_or_else(|| err(path, "expected an integer"))?,
        ),
        "Int8Property" => Property::Int8({
            let value = inner()?.as_i64().ok_or_else(|| err(path, "expected an integer"))?;
            i8::try_from(value).map_err(|_| err(path, format!("integer {value} does not fit in i8")))?
        }),
        "Int16Property" => Property::Int16({
            let value = inner()?.as_i64().ok_or_else(|| err(path, "expected an integer"))?;
            i16::try_from(value)
                .map_err(|_| err(path, format!("integer {value} does not fit in i16")))?
        }),
        "UInt8Property" => Property::UInt8({
            let value = inner()?.as_u64().ok_or_else(|| err(path, "expected an integer"))?;
            u8::try_from(value).map_err(|_| err(path, format!("integer {value} does not fit in u8")))?
        }),
        // The scalar the dialect emits for a `u16`-backed field (e.g. `RankUpExp`); same
        // shape as `IntProperty`/`Int64Property`, one level narrower and unsigned.
        "UInt16Property" => Property::UInt16({
            let value = inner()?.as_u64().ok_or_else(|| err(path, "expected an integer"))?;
            u16::try_from(value)
                .map_err(|_| err(path, format!("integer {value} does not fit in u16")))?
        }),
        "UInt32Property" => Property::UInt32({
            let value = inner()?.as_u64().ok_or_else(|| err(path, "expected an integer"))?;
            u32::try_from(value)
                .map_err(|_| err(path, format!("integer {value} does not fit in u32")))?
        }),
        "UInt64Property" => Property::UInt64(
            inner()?.as_u64().ok_or_else(|| err(path, "expected an integer"))?,
        ),
        "FloatProperty" => Property::Float(crate::Float(
            inner()?.as_f64().ok_or_else(|| err(path, "expected a float"))? as f32,
        )),
        "DoubleProperty" => Property::Double(crate::Double(
            inner()?.as_f64().ok_or_else(|| err(path, "expected a float"))?,
        )),
        "BoolProperty" => Property::Bool(
            inner()?.as_bool().ok_or_else(|| err(path, "expected a bool"))?,
        ),
        "NameProperty" => Property::Name(as_str(inner()?, path)?),
        "StrProperty" => Property::Str(as_str(inner()?, path)?),
        // The dialect writes {"type": "<EnumName>", "value": "<EnumName>::Variant"}; only
        // the variant survives into uesave, whose Enum is a bare String.
        "EnumProperty" => Property::Enum(as_str(field(inner()?, "value", path)?, path)?),
        // A scalar `ByteProperty` carries the same {"type", "value"} shape as an enum
        // property: `"type": "None"` is a plain numeric byte, anything else is an
        // enum-backed byte and `"value"` is the variant name.
        "ByteProperty" => {
            let body = inner()?;
            let enum_type = field(body, "type", path)?
                .as_str()
                .ok_or_else(|| err(path, "byte property has no string type tag"))?;
            if enum_type == "None" {
                let byte = field(body, "value", path)?
                    .as_u64()
                    .filter(|b| *b <= u8::MAX as u64)
                    .ok_or_else(|| err(path, "byte property value is not a byte"))?;
                Property::Byte(Byte::Byte(byte as u8))
            } else {
                Property::Byte(Byte::Label(as_str(field(body, "value", path)?, path)?))
            }
        }
        "StructProperty" => {
            let struct_type = value.get("struct_type").and_then(Value::as_str).unwrap_or("");
            match struct_type {
                "Guid" => Property::Struct(StructValue::Guid(
                    FGuid::parse_str(&as_str(inner()?, path)?)
                        .map_err(|e| err(path, format!("invalid guid: {e}")))?,
                )),
                // Built-in engine math structs: the dialect writes their fields as
                // plain numbers (`{"x": 1.0, ...}`), not as nested property objects,
                // unlike a user struct's fields. Each type below already deserializes
                // from exactly that shape.
                "Vector" => Property::Struct(StructValue::Vector(engine_struct(inner()?, path)?)),
                "Vector2D" => {
                    Property::Struct(StructValue::Vector2D(engine_struct(inner()?, path)?))
                }
                "Rotator" => {
                    Property::Struct(StructValue::Rotator(engine_struct(inner()?, path)?))
                }
                "Quat" => Property::Struct(StructValue::Quat(engine_struct(inner()?, path)?)),
                _ => Property::Struct(StructValue::Struct(properties_from_json::<G>(
                    inner()?, path,
                )?)),
            }
        }
        "ArrayProperty" => {
            let array_type = value.get("array_type").and_then(Value::as_str).unwrap_or("");
            match array_type {
                "ByteProperty" => {
                    let body = inner()?;
                    match body.get("values") {
                        // An opaque blob: the dialect kept the bytes.
                        Some(values) => Property::Array(ValueVec::Byte(ByteArray::Byte(
                            byte_array(values, path)?,
                        ))),
                        // A decoded payload. Ask the game to type it; failing that,
                        // ask it to re-encode the payload back to opaque bytes.
                        None => match G::game_struct_from_gvas_json(path, body)? {
                            Some(game_struct) => Property::Struct(StructValue::Game(game_struct)),
                            None => match G::game_bytes_from_gvas_json(path, body)? {
                                Some(bytes) => {
                                    Property::Array(ValueVec::Byte(ByteArray::Byte(bytes)))
                                }
                                None => {
                                    return Err(err(
                                        path,
                                        "decoded RawData at a path this game does not type",
                                    ))
                                }
                            },
                        },
                    }
                }
                "StructProperty" => {
                    let body = inner()?;
                    let values = field(body, "values", path)?
                        .as_array()
                        .ok_or_else(|| err(path, "struct array has no values"))?;
                    // A `Guid`-typed array (e.g. `OldOwnerPlayerUIds`) carries bare uuid
                    // strings, not nested property objects; anything else does.
                    let type_name = body.get("type_name").and_then(Value::as_str).unwrap_or("");
                    let items = if type_name == "Guid" {
                        values
                            .iter()
                            .map(|item| {
                                FGuid::parse_str(&as_str(item, path)?)
                                    .map(StructValue::Guid)
                                    .map_err(|e| err(path, format!("invalid guid: {e}")))
                            })
                            .collect::<Result<Vec<_>>>()?
                    } else {
                        values
                            .iter()
                            .map(|item| {
                                Ok(StructValue::Struct(properties_from_json::<G>(item, path)?))
                            })
                            .collect::<Result<Vec<_>>>()?
                    };
                    Property::Array(ValueVec::Struct(items))
                }
                "NameProperty" => {
                    let values = field(inner()?, "values", path)?
                        .as_array()
                        .ok_or_else(|| err(path, "name array has no values"))?;
                    let items =
                        values.iter().map(|item| as_str(item, path)).collect::<Result<Vec<_>>>()?;
                    Property::Array(ValueVec::Name(items))
                }
                "EnumProperty" => {
                    let values = field(inner()?, "values", path)?
                        .as_array()
                        .ok_or_else(|| err(path, "enum array has no values"))?;
                    let items = values
                        .iter()
                        .map(|item| as_str(item, path))
                        .collect::<Result<Vec<_>>>()?;
                    Property::Array(ValueVec::Enum(items))
                }
                other => return Err(err(path, format!("unknown array type: {other}"))),
            }
        }
        "MapProperty" => {
            let key_type = value.get("key_type").and_then(Value::as_str).unwrap_or("");
            let entries = inner()?
                .as_array()
                .ok_or_else(|| err(path, "map has no entry array"))?;
            let entries = entries
                .iter()
                .map(|entry| {
                    let raw_key = field(entry, "key", path)?;
                    let key = match key_type {
                        "EnumProperty" => Property::Enum(as_str(raw_key, path)?),
                        "IntProperty" => Property::Int({
                            let key = raw_key
                                .as_i64()
                                .ok_or_else(|| err(path, "map key is not an integer"))?;
                            i32::try_from(key)
                                .map_err(|_| err(path, format!("map key {key} does not fit in i32")))?
                        }),
                        other => {
                            return Err(err(path, format!("unsupported map key type: {other}")))
                        }
                    };
                    let value = Property::Struct(StructValue::Struct(
                        properties_from_json::<G>(field(entry, "value", path)?, path)?,
                    ));
                    Ok(MapEntry { key, value })
                })
                .collect::<Result<Vec<_>>>()?;
            Property::Map(entries)
        }
        other => return Err(err(path, format!("unknown property type: {other}"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_builds_dotted_paths() {
        assert_eq!(join("", "worldSaveData"), "worldSaveData");
        assert_eq!(join("worldSaveData", "MapObjectSaveData"), "worldSaveData.MapObjectSaveData");
    }
}
