use crate::{
    Properties, Property, PropertyKey, PropertySchemas, PropertyTagDataPartial, PropertyValue,
    Root, Save, StructType, StructValue, ValueVec,
};
use serde::{
    de::{DeserializeSeed, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use std::fmt;

struct PropertySeed<'a> {
    tag: &'a PropertyTagDataPartial,
    path: &'a str,
    schemas: &'a PropertySchemas,
}

impl<'de, 'a> DeserializeSeed<'de> for PropertySeed<'a> {
    type Value = Property;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        use crate::PropertyType;
        match &self.tag {
            PropertyTagDataPartial::Other(pt) => match pt {
                PropertyType::BoolProperty => Ok(Property::Bool(bool::deserialize(deserializer)?)),
                PropertyType::Int8Property => Ok(Property::Int8(i8::deserialize(deserializer)?)),
                PropertyType::Int16Property => Ok(Property::Int16(i16::deserialize(deserializer)?)),
                PropertyType::IntProperty => Ok(Property::Int(i32::deserialize(deserializer)?)),
                PropertyType::Int64Property => Ok(Property::Int64(i64::deserialize(deserializer)?)),
                PropertyType::UInt8Property => Ok(Property::UInt8(u8::deserialize(deserializer)?)),
                PropertyType::UInt16Property => {
                    Ok(Property::UInt16(u16::deserialize(deserializer)?))
                }
                PropertyType::UInt32Property => {
                    Ok(Property::UInt32(u32::deserialize(deserializer)?))
                }
                PropertyType::UInt64Property => {
                    Ok(Property::UInt64(u64::deserialize(deserializer)?))
                }
                PropertyType::FloatProperty => {
                    Ok(Property::Float(f32::deserialize(deserializer)?.into()))
                }
                PropertyType::DoubleProperty => {
                    Ok(Property::Double(f64::deserialize(deserializer)?.into()))
                }
                PropertyType::NameProperty => {
                    Ok(Property::Name(String::deserialize(deserializer)?))
                }
                PropertyType::StrProperty => Ok(Property::Str(String::deserialize(deserializer)?)),
                PropertyType::ObjectProperty => {
                    Ok(Property::Object(String::deserialize(deserializer)?))
                }
                PropertyType::TextProperty => {
                    Ok(Property::Text(crate::Text::deserialize(deserializer)?))
                }
                PropertyType::FieldPathProperty => Ok(Property::FieldPath(
                    crate::FieldPath::deserialize(deserializer)?,
                )),
                PropertyType::SoftObjectProperty => Ok(Property::SoftObject(
                    crate::SoftObjectPath::deserialize(deserializer)?,
                )),
                PropertyType::DelegateProperty => Ok(Property::Delegate(
                    crate::Delegate::deserialize(deserializer)?,
                )),
                PropertyType::MulticastDelegateProperty => Ok(Property::MulticastDelegate(
                    crate::MulticastDelegate::deserialize(deserializer)?,
                )),
                PropertyType::MulticastInlineDelegateProperty => {
                    Ok(Property::MulticastInlineDelegate(
                        crate::MulticastInlineDelegate::deserialize(deserializer)?,
                    ))
                }
                PropertyType::MulticastSparseDelegateProperty => {
                    Ok(Property::MulticastSparseDelegate(
                        crate::MulticastSparseDelegate::deserialize(deserializer)?,
                    ))
                }
                // These should never appear in Other - they have dedicated variants
                PropertyType::ByteProperty
                | PropertyType::EnumProperty
                | PropertyType::ArrayProperty
                | PropertyType::SetProperty
                | PropertyType::MapProperty
                | PropertyType::StructProperty => Err(serde::de::Error::custom(format!(
                    "Property type {:?} should not appear in Other variant",
                    pt
                ))),
            },
            PropertyTagDataPartial::Byte(_enum_type) => {
                Ok(Property::Byte(crate::Byte::deserialize(deserializer)?))
            }
            PropertyTagDataPartial::Enum(_, _) => {
                Ok(Property::Enum(String::deserialize(deserializer)?))
            }
            PropertyTagDataPartial::Struct { struct_type, .. } => {
                let sv = StructValueSeed {
                    struct_type,
                    path: self.path,
                    schemas: self.schemas,
                }
                .deserialize(deserializer)?;
                Ok(Property::Struct(sv))
            }
            PropertyTagDataPartial::Array(inner_tag) => {
                let va = ValueVecSeed {
                    tag: inner_tag,
                    path: self.path,
                    schemas: self.schemas,
                }
                .deserialize(deserializer)?;
                Ok(Property::Array(va))
            }
            PropertyTagDataPartial::Set { key_type } => {
                let vs = ValueVecSeed {
                    tag: key_type,
                    path: self.path,
                    schemas: self.schemas,
                }
                .deserialize(deserializer)?;
                Ok(Property::Set(vs))
            }
            PropertyTagDataPartial::Map {
                key_type,
                value_type,
            } => {
                let entries = MapEntriesSeed {
                    key_type,
                    value_type,
                    path: self.path,
                    schemas: self.schemas,
                }
                .deserialize(deserializer)?;
                Ok(Property::Map(entries))
            }
        }
    }
}

struct StructValueSeed<'a> {
    struct_type: &'a StructType,
    path: &'a str,
    schemas: &'a PropertySchemas,
}

impl<'de, 'a> DeserializeSeed<'de> for StructValueSeed<'a> {
    type Value = StructValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        match self.struct_type {
            StructType::Guid => Ok(StructValue::Guid(crate::FGuid::deserialize(deserializer)?)),
            StructType::DateTime => Ok(StructValue::DateTime(u64::deserialize(deserializer)?)),
            StructType::Timespan => Ok(StructValue::Timespan(i64::deserialize(deserializer)?)),
            StructType::Vector2D => Ok(StructValue::Vector2D(crate::Vector2D::deserialize(
                deserializer,
            )?)),
            StructType::Vector => Ok(StructValue::Vector(crate::Vector::deserialize(
                deserializer,
            )?)),
            StructType::IntVector => Ok(StructValue::IntVector(crate::IntVector::deserialize(
                deserializer,
            )?)),
            StructType::Box => Ok(StructValue::Box(crate::Box::deserialize(deserializer)?)),
            StructType::IntPoint => Ok(StructValue::IntPoint(crate::IntPoint::deserialize(
                deserializer,
            )?)),
            StructType::Quat => Ok(StructValue::Quat(crate::Quat::deserialize(deserializer)?)),
            StructType::LinearColor => Ok(StructValue::LinearColor(
                crate::LinearColor::deserialize(deserializer)?,
            )),
            StructType::Color => Ok(StructValue::Color(crate::Color::deserialize(deserializer)?)),
            StructType::Rotator => Ok(StructValue::Rotator(crate::Rotator::deserialize(
                deserializer,
            )?)),
            StructType::SoftObjectPath => Ok(StructValue::SoftObjectPath(
                crate::SoftObjectPath::deserialize(deserializer)?,
            )),
            StructType::GameplayTagContainer => Ok(StructValue::GameplayTagContainer(
                crate::GameplayTagContainer::deserialize(deserializer)?,
            )),
            StructType::UniqueNetIdRepl => Ok(StructValue::UniqueNetIdRepl(
                crate::UniqueNetIdRepl::deserialize(deserializer)?,
            )),
            StructType::Struct(_) => {
                let props = PropertiesSeed {
                    path: self.path,
                    schemas: self.schemas,
                }
                .deserialize(deserializer)?;
                Ok(StructValue::Struct(props))
            }
            StructType::Raw(_) => Ok(StructValue::Raw(Vec::<u8>::deserialize(deserializer)?)),
        }
    }
}

struct ValueVecSeed<'a> {
    tag: &'a PropertyTagDataPartial,
    path: &'a str,
    schemas: &'a PropertySchemas,
}

impl<'de, 'a> DeserializeSeed<'de> for ValueVecSeed<'a> {
    type Value = ValueVec;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        match self.tag {
            PropertyTagDataPartial::Struct { struct_type, .. } => {
                struct StructVecVisitor<'a> {
                    struct_type: &'a StructType,
                    path: &'a str,
                    schemas: &'a PropertySchemas,
                }

                impl<'de, 'a> Visitor<'de> for StructVecVisitor<'a> {
                    type Value = Vec<StructValue>;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("array or set of structs")
                    }

                    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                    where
                        A: SeqAccess<'de>,
                    {
                        let mut vec = Vec::new();
                        while let Some(elem) = seq.next_element_seed(StructValueSeed {
                            struct_type: self.struct_type,
                            path: self.path,
                            schemas: self.schemas,
                        })? {
                            vec.push(elem);
                        }
                        Ok(vec)
                    }
                }

                let structs = deserializer.deserialize_seq(StructVecVisitor {
                    struct_type,
                    path: self.path,
                    schemas: self.schemas,
                })?;
                Ok(ValueVec::Struct(structs))
            }
            _ => {
                // For base types, manually deserialize without the Struct variant
                deserialize_value_vec_base(deserializer)
            }
        }
    }
}

struct MapEntriesSeed<'a> {
    key_type: &'a PropertyTagDataPartial,
    value_type: &'a PropertyTagDataPartial,
    path: &'a str,
    schemas: &'a PropertySchemas,
}

impl<'de, 'a> DeserializeSeed<'de> for MapEntriesSeed<'a> {
    type Value = Vec<crate::MapEntry>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MapEntriesVisitor<'a> {
            key_type: &'a PropertyTagDataPartial,
            value_type: &'a PropertyTagDataPartial,
            path: &'a str,
            schemas: &'a PropertySchemas,
        }

        impl<'de, 'a> Visitor<'de> for MapEntriesVisitor<'a> {
            type Value = Vec<crate::MapEntry>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("array of map entries")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                while let Some(elem) = seq.next_element_seed(MapEntrySeed {
                    key_type: self.key_type,
                    value_type: self.value_type,
                    path: self.path,
                    schemas: self.schemas,
                })? {
                    vec.push(elem);
                }
                Ok(vec)
            }
        }

        deserializer.deserialize_seq(MapEntriesVisitor {
            key_type: self.key_type,
            value_type: self.value_type,
            path: self.path,
            schemas: self.schemas,
        })
    }
}

struct MapEntrySeed<'a> {
    key_type: &'a PropertyTagDataPartial,
    value_type: &'a PropertyTagDataPartial,
    path: &'a str,
    schemas: &'a PropertySchemas,
}

impl<'de, 'a> DeserializeSeed<'de> for MapEntrySeed<'a> {
    type Value = crate::MapEntry;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Key,
            Value,
        }

        struct MapEntryVisitor<'a> {
            key_type: &'a PropertyTagDataPartial,
            value_type: &'a PropertyTagDataPartial,
            path: &'a str,
            schemas: &'a PropertySchemas,
        }

        impl<'de, 'a> Visitor<'de> for MapEntryVisitor<'a> {
            type Value = crate::MapEntry;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("map entry with key and value fields")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut key = None;
                let mut value = None;

                while let Some(field) = map.next_key()? {
                    match field {
                        Field::Key => {
                            key = Some(map.next_value_seed(PropertyValueSeed {
                                tag: self.key_type,
                                path: self.path,
                                schemas: self.schemas,
                            })?);
                        }
                        Field::Value => {
                            value = Some(map.next_value_seed(PropertyValueSeed {
                                tag: self.value_type,
                                path: self.path,
                                schemas: self.schemas,
                            })?);
                        }
                    }
                }

                let key = key.ok_or_else(|| serde::de::Error::missing_field("key"))?;
                let value = value.ok_or_else(|| serde::de::Error::missing_field("value"))?;

                Ok(crate::MapEntry { key, value })
            }
        }

        deserializer.deserialize_struct(
            "MapEntry",
            &["key", "value"],
            MapEntryVisitor {
                key_type: self.key_type,
                value_type: self.value_type,
                path: self.path,
                schemas: self.schemas,
            },
        )
    }
}

struct PropertyValueSeed<'a> {
    tag: &'a PropertyTagDataPartial,
    path: &'a str,
    schemas: &'a PropertySchemas,
}

impl<'de, 'a> DeserializeSeed<'de> for PropertyValueSeed<'a> {
    type Value = PropertyValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        use crate::PropertyType;
        match &self.tag {
            PropertyTagDataPartial::Other(pt) => match pt {
                PropertyType::IntProperty => {
                    Ok(PropertyValue::Int(i32::deserialize(deserializer)?))
                }
                PropertyType::Int8Property => {
                    Ok(PropertyValue::Int8(i8::deserialize(deserializer)?))
                }
                PropertyType::Int16Property => {
                    Ok(PropertyValue::Int16(i16::deserialize(deserializer)?))
                }
                PropertyType::Int64Property => {
                    Ok(PropertyValue::Int64(i64::deserialize(deserializer)?))
                }
                PropertyType::UInt16Property => {
                    Ok(PropertyValue::UInt16(u16::deserialize(deserializer)?))
                }
                PropertyType::UInt32Property => {
                    Ok(PropertyValue::UInt32(u32::deserialize(deserializer)?))
                }
                PropertyType::FloatProperty => {
                    Ok(PropertyValue::Float(f32::deserialize(deserializer)?.into()))
                }
                PropertyType::DoubleProperty => Ok(PropertyValue::Double(
                    f64::deserialize(deserializer)?.into(),
                )),
                PropertyType::BoolProperty => {
                    Ok(PropertyValue::Bool(bool::deserialize(deserializer)?))
                }
                PropertyType::NameProperty => {
                    Ok(PropertyValue::Name(String::deserialize(deserializer)?))
                }
                PropertyType::StrProperty => {
                    Ok(PropertyValue::Str(String::deserialize(deserializer)?))
                }
                PropertyType::ObjectProperty => {
                    Ok(PropertyValue::Object(String::deserialize(deserializer)?))
                }
                PropertyType::SoftObjectProperty => Ok(PropertyValue::SoftObject(
                    crate::SoftObjectPath::deserialize(deserializer)?,
                )),
                // PropertyValue doesn't support these types - they should use dedicated variants or shouldn't appear here
                PropertyType::UInt8Property
                | PropertyType::UInt64Property
                | PropertyType::ByteProperty
                | PropertyType::EnumProperty
                | PropertyType::TextProperty
                | PropertyType::FieldPathProperty
                | PropertyType::DelegateProperty
                | PropertyType::MulticastDelegateProperty
                | PropertyType::MulticastInlineDelegateProperty
                | PropertyType::MulticastSparseDelegateProperty
                | PropertyType::ArrayProperty
                | PropertyType::SetProperty
                | PropertyType::MapProperty
                | PropertyType::StructProperty => Err(serde::de::Error::custom(format!(
                    "Property type {:?} is not supported in PropertyValue",
                    pt
                ))),
            },
            PropertyTagDataPartial::Byte(_enum_type) => {
                Ok(PropertyValue::Byte(crate::Byte::deserialize(deserializer)?))
            }
            PropertyTagDataPartial::Enum(_, _) => {
                Ok(PropertyValue::Enum(String::deserialize(deserializer)?))
            }
            PropertyTagDataPartial::Struct { struct_type, .. } => {
                let sv = StructValueSeed {
                    struct_type,
                    path: self.path,
                    schemas: self.schemas,
                }
                .deserialize(deserializer)?;
                Ok(PropertyValue::Struct(sv))
            }
            _ => Err(serde::de::Error::custom(
                "Unexpected property tag type for PropertyValue",
            )),
        }
    }
}

struct PropertiesSeed<'a> {
    path: &'a str,
    schemas: &'a PropertySchemas,
}

impl<'de, 'a> DeserializeSeed<'de> for PropertiesSeed<'a> {
    type Value = Properties;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PropertiesVisitor<'a> {
            path: &'a str,
            schemas: &'a PropertySchemas,
        }

        impl<'de, 'a> Visitor<'de> for PropertiesVisitor<'a> {
            type Value = Properties;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("properties map")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut properties = indexmap::IndexMap::new();

                while let Some(key) = map.next_key::<PropertyKey>()? {
                    let prop_path = if self.path.is_empty() {
                        key.1.to_string()
                    } else {
                        format!("{}.{}", self.path, key.1)
                    };

                    let tag = self.schemas.schemas().get(&prop_path).ok_or_else(|| {
                        serde::de::Error::custom(format!("No schema for property: {}", prop_path))
                    })?;

                    let prop = map.next_value_seed(PropertySeed {
                        tag: &tag.data,
                        path: &prop_path,
                        schemas: self.schemas,
                    })?;

                    properties.insert(key, prop);
                }

                Ok(Properties(properties))
            }
        }

        deserializer.deserialize_map(PropertiesVisitor {
            path: self.path,
            schemas: self.schemas,
        })
    }
}

struct RootSeed<'a> {
    schemas: &'a PropertySchemas,
}

impl<'de, 'a> DeserializeSeed<'de> for RootSeed<'a> {
    type Value = Root;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            SaveGameType,
            Properties,
        }

        struct RootVisitorWithSchemas<'a> {
            schemas: &'a PropertySchemas,
        }

        impl<'de, 'a> Visitor<'de> for RootVisitorWithSchemas<'a> {
            type Value = Root;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Root struct")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut save_game_type = None;
                let mut properties = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::SaveGameType => {
                            save_game_type = Some(map.next_value()?);
                        }
                        Field::Properties => {
                            properties = Some(map.next_value_seed(PropertiesSeed {
                                path: "",
                                schemas: self.schemas,
                            })?);
                        }
                    }
                }

                let save_game_type = save_game_type
                    .ok_or_else(|| serde::de::Error::missing_field("save_game_type"))?;
                let properties =
                    properties.ok_or_else(|| serde::de::Error::missing_field("properties"))?;

                Ok(Root {
                    save_game_type,
                    properties,
                })
            }
        }

        deserializer.deserialize_struct(
            "Root",
            &["save_game_type", "properties"],
            RootVisitorWithSchemas {
                schemas: self.schemas,
            },
        )
    }
}

// Deserialize implementation for Save
impl<'de> Deserialize<'de> for Save {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Header,
            Schemas,
            Root,
            Extra,
        }

        struct SaveVisitor;

        impl<'de> Visitor<'de> for SaveVisitor {
            type Value = Save;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Save struct")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut header = None;
                let mut schemas = None;
                let mut root = None;
                let mut extra = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Header => {
                            header = Some(map.next_value()?);
                        }
                        Field::Schemas => {
                            schemas = Some(map.next_value()?);
                        }
                        Field::Root => {
                            // Schemas must have been parsed before root
                            let schemas_ref = schemas.as_ref().ok_or_else(|| {
                                serde::de::Error::custom("schemas must appear before root in JSON")
                            })?;

                            root = Some(map.next_value_seed(RootSeed {
                                schemas: schemas_ref,
                            })?);
                        }
                        Field::Extra => {
                            extra = Some(map.next_value()?);
                        }
                    }
                }

                let header = header.ok_or_else(|| serde::de::Error::missing_field("header"))?;
                let schemas = schemas.ok_or_else(|| serde::de::Error::missing_field("schemas"))?;
                let root = root.ok_or_else(|| serde::de::Error::missing_field("root"))?;
                let extra = extra.ok_or_else(|| serde::de::Error::missing_field("extra"))?;

                Ok(Save {
                    header,
                    schemas,
                    root,
                    extra,
                })
            }
        }

        deserializer.deserialize_struct(
            "Save",
            &["header", "schemas", "root", "extra"],
            SaveVisitor,
        )
    }
}

// Helper function to deserialize ValueVec for non-struct base types
fn deserialize_value_vec_base<'de, D>(deserializer: D) -> Result<ValueVec, D::Error>
where
    D: Deserializer<'de>,
{
    // We need to implement custom deserialization since ValueVec doesn't derive Deserialize
    // For now, use untagged enum deserialization for all non-Struct variants
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ValueVecBase {
        Int8(Vec<i8>),
        Int16(Vec<i16>),
        Int(Vec<i32>),
        Int64(Vec<i64>),
        UInt8(Vec<u8>),
        UInt16(Vec<u16>),
        UInt32(Vec<u32>),
        UInt64(Vec<u64>),
        Float(Vec<crate::Float>),
        Double(Vec<crate::Double>),
        Bool(Vec<bool>),
        Byte(crate::ByteArray),
        Enum(Vec<String>),
        Str(Vec<String>),
        Text(Vec<crate::Text>),
        SoftObject(Vec<(String, String)>),
        Name(Vec<String>),
        Object(Vec<String>),
        Box(Vec<crate::Box>),
    }

    let base = ValueVecBase::deserialize(deserializer)?;
    Ok(match base {
        ValueVecBase::Int8(v) => ValueVec::Int8(v),
        ValueVecBase::Int16(v) => ValueVec::Int16(v),
        ValueVecBase::Int(v) => ValueVec::Int(v),
        ValueVecBase::Int64(v) => ValueVec::Int64(v),
        ValueVecBase::UInt8(v) => ValueVec::UInt8(v),
        ValueVecBase::UInt16(v) => ValueVec::UInt16(v),
        ValueVecBase::UInt32(v) => ValueVec::UInt32(v),
        ValueVecBase::UInt64(v) => ValueVec::UInt64(v),
        ValueVecBase::Float(v) => ValueVec::Float(v),
        ValueVecBase::Double(v) => ValueVec::Double(v),
        ValueVecBase::Bool(v) => ValueVec::Bool(v),
        ValueVecBase::Byte(v) => ValueVec::Byte(v),
        ValueVecBase::Enum(v) => ValueVec::Enum(v),
        ValueVecBase::Str(v) => ValueVec::Str(v),
        ValueVecBase::Text(v) => ValueVec::Text(v),
        ValueVecBase::SoftObject(v) => ValueVec::SoftObject(v),
        ValueVecBase::Name(v) => ValueVec::Name(v),
        ValueVecBase::Object(v) => ValueVec::Object(v),
        ValueVecBase::Box(v) => ValueVec::Box(v),
    })
}
