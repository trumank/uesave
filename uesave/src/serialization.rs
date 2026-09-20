use crate::game::Game;
use crate::{
    ArchiveType, ByteArray, FClothLODDataCommon, FMeshToMeshVertData, FNiagaraVariable,
    FNiagaraVariableBase, FNiagaraVariableWithOffset, Properties, Property, PropertyKey,
    PropertySchemas, PropertyTagDataPartial, Root, Save, SaveGameArchiveType, SoftObjectPath,
    StructType, StructValue, ValueVec,
};
use serde::{
    de::{DeserializeSeed, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;

// ---------------------------------------------------------------------------
// Schema-aware `Properties` deserialization context.
//
// Game structs (routed through [`Game::deserialize_struct`]) may embed nested
// `Properties` fields that are not self-describing: interpreting their untyped
// JSON needs the schema table plus the path prefix under which those nested
// properties were recorded. Rather than thread a bespoke seed through every
// game struct field (the structs are large externally-tagged enums), the
// pipeline installs the (schemas, path) context here before deriving the game
// struct, and `Deserialize for Properties<T>` reads it back.
//
// The context is a stack so nested game structs (a Properties inside a game
// struct that itself contains another RawData game struct) each see their own
// (schemas, path). The `*const PropertySchemas` is only ever read while the
// `deserialize_struct` call that pushed it (and therefore the borrow it came
// from) is still on the stack, so the deref is sound.
// ---------------------------------------------------------------------------
thread_local! {
    static PROPERTIES_CTX: RefCell<Vec<(*const PropertySchemas, String)>> =
        const { RefCell::new(Vec::new()) };
}

/// RAII guard that pops the properties context pushed by [`push_properties_ctx`].
pub(crate) struct PropertiesCtxGuard;

impl Drop for PropertiesCtxGuard {
    fn drop(&mut self) {
        PROPERTIES_CTX.with(|c| {
            c.borrow_mut().pop();
        });
    }
}

/// Install `(schemas, path)` as the active context for the duration of the
/// returned guard. Call this before deriving a game struct that embeds nested
/// [`Properties`]; the properties' tags live in `schemas` at `{path}.{field}`.
pub(crate) fn push_properties_ctx(schemas: &PropertySchemas, path: &str) -> PropertiesCtxGuard {
    PROPERTIES_CTX.with(|c| {
        c.borrow_mut()
            .push((schemas as *const PropertySchemas, path.to_string()));
    });
    PropertiesCtxGuard
}

/// Deserialize a `Properties<SaveGameArchiveType<G>>` using an explicit schema
/// context. Exposed to [`ArchiveType::deserialize_properties`] so the concrete
/// game `G` is threaded into the seed.
pub(crate) fn deserialize_properties_seed<'de, D, G: Game>(
    path: &str,
    schemas: &PropertySchemas,
    deserializer: D,
) -> Result<Properties<SaveGameArchiveType<G>>, D::Error>
where
    D: Deserializer<'de>,
{
    PropertiesSeed::<G> {
        path,
        schemas,
        _game: PhantomData,
    }
    .deserialize(deserializer)
}

/// Native `Deserialize` for `Properties`, valid only inside a game struct
/// deserialization (see the context module docs above). It reads the active
/// `(schemas, path)` and dispatches on `T` so the correct game's schema-aware
/// seed runs. Deserializing `Properties` outside such a context is an error.
impl<'de, T: ArchiveType> Deserialize<'de> for Properties<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ctx = PROPERTIES_CTX.with(|c| c.borrow().last().cloned());
        let (schemas_ptr, path) = ctx.ok_or_else(|| {
            serde::de::Error::custom(
                "Properties can only be deserialized within a game struct context",
            )
        })?;
        // SAFETY: `schemas_ptr` was produced from a `&PropertySchemas` borrow
        // held by the `deserialize_struct` call still executing above us on the
        // stack, so the pointee outlives this read.
        let schemas: &PropertySchemas = unsafe { &*schemas_ptr };
        T::deserialize_properties(&path, schemas, deserializer)
    }
}

/// Generates one or more `DeserializeSeed`s that thread `PropertySchemas` + a
/// path prefix through to any field marked `= "segment"`, which is seeded as a
/// nested `Properties` at `{parent_path}.{segment}`. Unmarked fields use plain
/// `Deserialize`. Each seed is generic over the active game `G`.
macro_rules! properties_seeds {
    (
        $(
            $ctor:ident : $value:ty => $seed:ident {
                $( $field:ident : $ty:ty $( = $segment:literal )? ),+ $(,)?
            }
        )+
    ) => {
        $(
            struct $seed<'a, G: Game> {
                path: &'a str,
                schemas: &'a PropertySchemas,
                _game: PhantomData<G>,
            }

            impl<'de, 'a, G: Game> DeserializeSeed<'de> for $seed<'a, G> {
                type Value = $value;

                fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    deserializer.deserialize_map(self)
                }
            }

            impl<'de, 'a, G: Game> Visitor<'de> for $seed<'a, G> {
                type Value = $value;

                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str(concat!(stringify!($ctor), " map"))
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let __path = self.path;
                    let __schemas = self.schemas;
                    $( let mut $field: Option<$ty> = None; )+

                    while let Some(__key) = map.next_key::<String>()? {
                        match __key.as_str() {
                            $(
                                stringify!($field) => {
                                    $field = Some(properties_seeds!(
                                        @read __path, __schemas, map $(, $segment)?
                                    ));
                                }
                            )+
                            other => {
                                return Err(serde::de::Error::unknown_field(
                                    other,
                                    &[$( stringify!($field) ),+],
                                ))
                            }
                        }
                    }

                    Ok($ctor {
                        $(
                            $field: $field.ok_or_else(|| {
                                serde::de::Error::missing_field(stringify!($field))
                            })?,
                        )+
                    })
                }
            }
        )+
    };

    (@read $path:ident, $schemas:ident, $m:ident, $segment:literal) => {{
        let __sub_path = if $path.is_empty() {
            String::from($segment)
        } else if $segment.is_empty() {
            $path.to_string()
        } else {
            format!("{}.{}", $path, $segment)
        };
        $m.next_value_seed(PropertiesSeed::<G> {
            path: &__sub_path,
            schemas: $schemas,
            _game: PhantomData,
        })?
    }};

    (@read $path:ident, $schemas:ident, $m:ident) => {{
        $m.next_value()?
    }};
}

properties_seeds! {
    Root : Root<SaveGameArchiveType<G>> => RootSeed {
        save_game_type: String,
        properties: Properties<SaveGameArchiveType<G>> = "",
    }

    FClothLODDataCommon : FClothLODDataCommon<SaveGameArchiveType<G>> => ClothLODDataCommonSeed {
        properties: Properties<SaveGameArchiveType<G>> = "properties",
        transition_up_skin_data: Vec<FMeshToMeshVertData>,
        transition_down_skin_data: Vec<FMeshToMeshVertData>,
    }

    FNiagaraVariableBase : FNiagaraVariableBase<SaveGameArchiveType<G>> => NiagaraVariableBaseSeed {
        name: String,
        type_def: Properties<SaveGameArchiveType<G>> = "type_def",
    }

    FNiagaraVariable : FNiagaraVariable<SaveGameArchiveType<G>> => NiagaraVariableSeed {
        name: String,
        type_def: Properties<SaveGameArchiveType<G>> = "type_def",
        var_data: Vec<u8>,
    }

    FNiagaraVariableWithOffset : FNiagaraVariableWithOffset<SaveGameArchiveType<G>> => NiagaraVariableWithOffsetSeed {
        name: String,
        type_def: Properties<SaveGameArchiveType<G>> = "type_def",
        offset: i32,
    }
}

struct PropertySeed<'a, G: Game> {
    tag: &'a PropertyTagDataPartial,
    path: &'a str,
    schemas: &'a PropertySchemas,
    _game: PhantomData<G>,
}

impl<'de, 'a, G: Game> DeserializeSeed<'de> for PropertySeed<'a, G> {
    type Value = Property<SaveGameArchiveType<G>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        use crate::PropertyType;
        if self.tag.has_raw_struct() {
            return Ok(Property::Raw(Vec::<u8>::deserialize(deserializer)?));
        }
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
                PropertyType::ObjectProperty | PropertyType::InterfaceProperty => {
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
                    "Property type {pt:?} should not appear in Other variant"
                ))),
            },
            PropertyTagDataPartial::Byte(_enum_type) => {
                Ok(Property::Byte(crate::Byte::deserialize(deserializer)?))
            }
            PropertyTagDataPartial::Enum(_, _) => {
                Ok(Property::Enum(String::deserialize(deserializer)?))
            }
            PropertyTagDataPartial::Struct { struct_type, .. } => {
                // A game (embedded-RawData) struct may have been left as an
                // empty/unparsed byte array on read (paths shared by many map
                // entries record a single schema, so an entry that stayed raw
                // still serializes as a byte array). Accept either shape: a JSON
                // map is the typed game struct, a JSON sequence is leftover bytes.
                if matches!(struct_type, StructType::Game(_)) {
                    deserializer.deserialize_any(GameStructOrBytesSeed::<G> {
                        struct_type,
                        path: self.path,
                        schemas: self.schemas,
                        _game: PhantomData,
                    })
                } else {
                    let sv = StructValueSeed::<G> {
                        struct_type,
                        path: self.path,
                        schemas: self.schemas,
                        _game: PhantomData,
                    }
                    .deserialize(deserializer)?;
                    Ok(Property::Struct(sv))
                }
            }
            PropertyTagDataPartial::Array(inner_tag) => {
                let va = ValueVecSeed::<G> {
                    tag: inner_tag,
                    path: self.path,
                    schemas: self.schemas,
                    _game: PhantomData,
                }
                .deserialize(deserializer)?;
                Ok(Property::Array(va))
            }
            PropertyTagDataPartial::Set { key_type } => {
                let vs = ValueVecSeed::<G> {
                    tag: key_type,
                    path: self.path,
                    schemas: self.schemas,
                    _game: PhantomData,
                }
                .deserialize(deserializer)?;
                Ok(Property::Set(vs))
            }
            PropertyTagDataPartial::Map {
                key_type,
                value_type,
            } => {
                let entries = MapEntriesSeed::<G> {
                    key_type,
                    value_type,
                    path: self.path,
                    schemas: self.schemas,
                    _game: PhantomData,
                }
                .deserialize(deserializer)?;
                Ok(Property::Map(entries))
            }
        }
    }
}

/// Deserializes a `StructType::Game` property that may be either the typed game
/// struct (a JSON map) or a leftover empty/unparsed byte array (a JSON
/// sequence). Handled at the `Property` level because a `StructValue`-only seed
/// cannot express the byte-array shape.
struct GameStructOrBytesSeed<'a, G: Game> {
    struct_type: &'a StructType,
    path: &'a str,
    schemas: &'a PropertySchemas,
    _game: PhantomData<G>,
}

impl<'de, 'a, G: Game> DeserializeSeed<'de> for GameStructOrBytesSeed<'a, G> {
    type Value = Property<SaveGameArchiveType<G>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de, 'a, G: Game> Visitor<'de> for GameStructOrBytesSeed<'a, G> {
    type Value = Property<SaveGameArchiveType<G>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a game struct map or a raw byte array")
    }

    // A bare JSON array is a raw byte payload (kept for robustness; the crate's
    // own byte arrays serialize as `{"Byte": [...]}`, handled in `visit_map`).
    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let bytes = Vec::<u8>::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))?;
        Ok(Property::Array(ValueVec::Byte(ByteArray::Byte(bytes))))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // `ByteArray` serializes externally tagged (`{"Byte": [...]}` /
        // `{"Label": [...]}`), so a leftover/empty byte payload arrives here as
        // a map too. Peek the first key to tell a byte payload apart from a game
        // struct map; if it is a struct, re-inject the key and parse the struct.
        let Some(first_key) = map.next_key::<String>()? else {
            return Err(serde::de::Error::custom(format!(
                "empty map for {:?} property",
                self.struct_type
            )));
        };
        match first_key.as_str() {
            "Byte" => Ok(Property::Array(ValueVec::Byte(ByteArray::Byte(
                map.next_value()?,
            )))),
            "Label" => Ok(Property::Array(ValueVec::Byte(ByteArray::Label(
                map.next_value()?,
            )))),
            _ => {
                let sv = StructValueSeed::<G> {
                    struct_type: self.struct_type,
                    path: self.path,
                    schemas: self.schemas,
                    _game: PhantomData,
                }
                .deserialize(serde::de::value::MapAccessDeserializer::new(
                    PrependedMapAccess {
                        first_key: Some(first_key),
                        inner: map,
                    },
                ))?;
                Ok(Property::Struct(sv))
            }
        }
    }
}

/// A [`MapAccess`] adapter that re-injects an already-consumed first key, so a
/// map whose first key was peeked can still be handed to a struct deserializer.
struct PrependedMapAccess<M> {
    first_key: Option<String>,
    inner: M,
}

impl<'de, M: MapAccess<'de>> MapAccess<'de> for PrependedMapAccess<M> {
    type Error = M::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        use serde::de::IntoDeserializer;
        if let Some(key) = self.first_key.take() {
            return seed.deserialize(key.into_deserializer()).map(Some);
        }
        self.inner.next_key_seed(seed)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        self.inner.next_value_seed(seed)
    }
}

struct StructValueSeed<'a, G: Game> {
    struct_type: &'a StructType,
    path: &'a str,
    schemas: &'a PropertySchemas,
    _game: PhantomData<G>,
}

impl<'de, 'a, G: Game> DeserializeSeed<'de> for StructValueSeed<'a, G> {
    type Value = StructValue<SaveGameArchiveType<G>>;

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
            StructType::Vector4 => Ok(StructValue::Vector4(crate::Vector4::deserialize(
                deserializer,
            )?)),
            StructType::IntVector => Ok(StructValue::IntVector(crate::IntVector::deserialize(
                deserializer,
            )?)),
            StructType::Box => Ok(StructValue::Box(crate::Box::deserialize(deserializer)?)),
            StructType::Box2D => Ok(StructValue::Box2D(crate::Box2D::deserialize(deserializer)?)),
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
            StructType::SoftClassPath => Ok(StructValue::SoftClassPath(
                crate::SoftObjectPath::deserialize(deserializer)?,
            )),
            StructType::GameplayTagContainer => Ok(StructValue::GameplayTagContainer(
                crate::GameplayTagContainer::deserialize(deserializer)?,
            )),
            StructType::UniqueNetIdRepl => Ok(StructValue::UniqueNetIdRepl(
                crate::UniqueNetIdRepl::deserialize(deserializer)?,
            )),
            StructType::KeyHandleMap => Ok(StructValue::KeyHandleMap(
                crate::FKeyHandleMap::deserialize(deserializer)?,
            )),
            StructType::RichCurveKey => Ok(StructValue::RichCurveKey(
                crate::FRichCurveKey::deserialize(deserializer)?,
            )),
            StructType::SkeletalMeshSamplingLODBuiltData => {
                Ok(StructValue::SkeletalMeshSamplingLODBuiltData(
                    crate::FSkeletalMeshSamplingLODBuiltData::deserialize(deserializer)?,
                ))
            }
            StructType::PerPlatformFloat => Ok(StructValue::PerPlatformFloat(
                crate::FPerPlatformFloat::deserialize(deserializer)?,
            )),
            StructType::MovieSceneFrameRange => Ok(StructValue::MovieSceneFrameRange(
                crate::FMovieSceneFrameRange::deserialize(deserializer)?,
            )),
            StructType::MovieSceneFloatChannel => Ok(StructValue::MovieSceneFloatChannel(
                crate::FMovieSceneFloatChannel::deserialize(deserializer)?,
            )),
            StructType::FrameNumber => Ok(StructValue::FrameNumber(
                crate::FFrameNumber::deserialize(deserializer)?,
            )),
            StructType::ExpressionInput => Ok(StructValue::ExpressionInput(
                crate::FExpressionInput::deserialize(deserializer)?,
            )),
            StructType::MaterialAttributesInput => Ok(StructValue::MaterialAttributesInput(
                crate::FExpressionInput::deserialize(deserializer)?,
            )),
            StructType::ColorMaterialInput => Ok(StructValue::ColorMaterialInput(
                crate::FColorMaterialInput::deserialize(deserializer)?,
            )),
            StructType::ScalarMaterialInput => Ok(StructValue::ScalarMaterialInput(
                crate::FScalarMaterialInput::deserialize(deserializer)?,
            )),
            StructType::ShadingModelMaterialInput => Ok(StructValue::ShadingModelMaterialInput(
                crate::FShadingModelMaterialInput::deserialize(deserializer)?,
            )),
            StructType::VectorMaterialInput => Ok(StructValue::VectorMaterialInput(
                crate::FVectorMaterialInput::deserialize(deserializer)?,
            )),
            StructType::Vector2MaterialInput => Ok(StructValue::Vector2MaterialInput(
                crate::FVector2MaterialInput::deserialize(deserializer)?,
            )),
            StructType::MovieSceneSequenceID => Ok(StructValue::MovieSceneSequenceID(
                crate::FMovieSceneSequenceID::deserialize(deserializer)?,
            )),
            StructType::MovieSceneTrackIdentifier => Ok(StructValue::MovieSceneTrackIdentifier(
                crate::FMovieSceneTrackIdentifier::deserialize(deserializer)?,
            )),
            StructType::MovieSceneEvaluationKey => Ok(StructValue::MovieSceneEvaluationKey(
                crate::FMovieSceneEvaluationKey::deserialize(deserializer)?,
            )),
            StructType::MovieSceneEvaluationFieldEntityTree => {
                Ok(StructValue::MovieSceneEvaluationFieldEntityTree(
                    crate::FMovieSceneEvaluationFieldEntityTree::deserialize(deserializer)?,
                ))
            }
            StructType::NiagaraDataInterfaceGeneratedFunction => {
                Ok(StructValue::NiagaraDataInterfaceGeneratedFunction(
                    crate::FNiagaraDataInterfaceGeneratedFunction::deserialize(deserializer)?,
                ))
            }
            StructType::NiagaraDataInterfaceGPUParamInfo => {
                Ok(StructValue::NiagaraDataInterfaceGPUParamInfo(
                    crate::FNiagaraDataInterfaceGPUParamInfo::deserialize(deserializer)?,
                ))
            }
            StructType::FontData => Ok(StructValue::FontData(crate::FFontData::<
                SaveGameArchiveType<G>,
            >::deserialize(
                deserializer
            )?)),
            StructType::ClothLODDataCommon => Ok(StructValue::ClothLODDataCommon(
                ClothLODDataCommonSeed::<G> {
                    path: self.path,
                    schemas: self.schemas,
                    _game: PhantomData,
                }
                .deserialize(deserializer)?,
            )),
            StructType::NiagaraVariable => Ok(StructValue::NiagaraVariable(
                NiagaraVariableSeed::<G> {
                    path: self.path,
                    schemas: self.schemas,
                    _game: PhantomData,
                }
                .deserialize(deserializer)?,
            )),
            StructType::NiagaraVariableBase => Ok(StructValue::NiagaraVariableBase(
                NiagaraVariableBaseSeed::<G> {
                    path: self.path,
                    schemas: self.schemas,
                    _game: PhantomData,
                }
                .deserialize(deserializer)?,
            )),
            StructType::NiagaraVariableWithOffset => Ok(StructValue::NiagaraVariableWithOffset(
                NiagaraVariableWithOffsetSeed::<G> {
                    path: self.path,
                    schemas: self.schemas,
                    _game: PhantomData,
                }
                .deserialize(deserializer)?,
            )),
            StructType::Game(type_path) => {
                // Route typed game structs through the active game's
                // `deserialize_struct`, threading the property path + schemas so
                // structs embedding nested `Properties` can reconstruct them.
                let name = type_path.rsplit('.').next().unwrap_or(type_path);
                Ok(StructValue::Game(<G as Game>::deserialize_struct::<
                    D,
                    SaveGameArchiveType<G>,
                >(
                    name,
                    self.path,
                    self.schemas,
                    deserializer,
                )?))
            }
            StructType::Struct(_) => {
                let props = PropertiesSeed::<G> {
                    path: self.path,
                    schemas: self.schemas,
                    _game: PhantomData,
                }
                .deserialize(deserializer)?;
                Ok(StructValue::Struct(props))
            }
            StructType::Raw(_) => Ok(StructValue::Raw(Vec::<u8>::deserialize(deserializer)?)),
        }
    }
}

struct StructVecSeed<'a, G: Game> {
    struct_type: &'a StructType,
    path: &'a str,
    schemas: &'a PropertySchemas,
    _game: PhantomData<G>,
}

impl<'de, 'a, G: Game> DeserializeSeed<'de> for StructVecSeed<'a, G> {
    type Value = Vec<StructValue<SaveGameArchiveType<G>>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}

impl<'de, 'a, G: Game> Visitor<'de> for StructVecSeed<'a, G> {
    type Value = Vec<StructValue<SaveGameArchiveType<G>>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("array or set of structs")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut vec = Vec::new();
        while let Some(elem) = seq.next_element_seed(StructValueSeed::<G> {
            struct_type: self.struct_type,
            path: self.path,
            schemas: self.schemas,
            _game: PhantomData,
        })? {
            vec.push(elem);
        }
        Ok(vec)
    }
}

struct ValueVecSeed<'a, G: Game> {
    tag: &'a PropertyTagDataPartial,
    path: &'a str,
    schemas: &'a PropertySchemas,
    _game: PhantomData<G>,
}

impl<'de, 'a, G: Game> DeserializeSeed<'de> for ValueVecSeed<'a, G> {
    type Value = ValueVec<SaveGameArchiveType<G>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        match self.tag {
            PropertyTagDataPartial::Struct { struct_type, .. } => {
                let structs = StructVecSeed::<G> {
                    struct_type,
                    path: self.path,
                    schemas: self.schemas,
                    _game: PhantomData,
                }
                .deserialize(deserializer)?;
                Ok(ValueVec::Struct(structs))
            }
            PropertyTagDataPartial::Other(pt) => {
                use crate::PropertyType;
                match pt {
                    PropertyType::Int8Property => {
                        Ok(ValueVec::Int8(Vec::<i8>::deserialize(deserializer)?))
                    }
                    PropertyType::Int16Property => {
                        Ok(ValueVec::Int16(Vec::<i16>::deserialize(deserializer)?))
                    }
                    PropertyType::IntProperty => {
                        Ok(ValueVec::Int(Vec::<i32>::deserialize(deserializer)?))
                    }
                    PropertyType::Int64Property => {
                        Ok(ValueVec::Int64(Vec::<i64>::deserialize(deserializer)?))
                    }
                    PropertyType::UInt8Property => {
                        Ok(ValueVec::UInt8(Vec::<u8>::deserialize(deserializer)?))
                    }
                    PropertyType::UInt16Property => {
                        Ok(ValueVec::UInt16(Vec::<u16>::deserialize(deserializer)?))
                    }
                    PropertyType::UInt32Property => {
                        Ok(ValueVec::UInt32(Vec::<u32>::deserialize(deserializer)?))
                    }
                    PropertyType::UInt64Property => {
                        Ok(ValueVec::UInt64(Vec::<u64>::deserialize(deserializer)?))
                    }
                    PropertyType::FloatProperty => Ok(ValueVec::Float(
                        Vec::<crate::Float>::deserialize(deserializer)?,
                    )),
                    PropertyType::DoubleProperty => Ok(ValueVec::Double(
                        Vec::<crate::Double>::deserialize(deserializer)?,
                    )),
                    PropertyType::BoolProperty => {
                        Ok(ValueVec::Bool(Vec::<bool>::deserialize(deserializer)?))
                    }
                    PropertyType::StrProperty => {
                        Ok(ValueVec::Str(Vec::<String>::deserialize(deserializer)?))
                    }
                    PropertyType::NameProperty => {
                        Ok(ValueVec::Name(Vec::<String>::deserialize(deserializer)?))
                    }
                    PropertyType::ObjectProperty | PropertyType::InterfaceProperty => {
                        Ok(ValueVec::Object(Vec::<String>::deserialize(deserializer)?))
                    }
                    PropertyType::SoftObjectProperty => {
                        Ok(ValueVec::SoftObject(Vec::<SoftObjectPath>::deserialize(
                            deserializer,
                        )?))
                    }
                    PropertyType::TextProperty => Ok(ValueVec::Text(
                        Vec::<crate::Text>::deserialize(deserializer)?,
                    )),
                    PropertyType::ByteProperty
                    | PropertyType::EnumProperty
                    | PropertyType::ArrayProperty
                    | PropertyType::SetProperty
                    | PropertyType::MapProperty
                    | PropertyType::StructProperty
                    | PropertyType::FieldPathProperty
                    | PropertyType::DelegateProperty
                    | PropertyType::MulticastDelegateProperty
                    | PropertyType::MulticastInlineDelegateProperty
                    | PropertyType::MulticastSparseDelegateProperty => {
                        Err(serde::de::Error::custom(format!(
                            "Unexpected property type {pt:?} in array"
                        )))
                    }
                }
            }
            PropertyTagDataPartial::Byte(_) => {
                Ok(ValueVec::Byte(crate::ByteArray::deserialize(deserializer)?))
            }
            PropertyTagDataPartial::Enum(_, _) => {
                Ok(ValueVec::Enum(Vec::<String>::deserialize(deserializer)?))
            }
            PropertyTagDataPartial::Array(_)
            | PropertyTagDataPartial::Set { .. }
            | PropertyTagDataPartial::Map { .. } => Err(serde::de::Error::custom(
                "Nested array/set/map not supported",
            )),
        }
    }
}

struct MapEntriesSeed<'a, G: Game> {
    key_type: &'a PropertyTagDataPartial,
    value_type: &'a PropertyTagDataPartial,
    path: &'a str,
    schemas: &'a PropertySchemas,
    _game: PhantomData<G>,
}

impl<'de, 'a, G: Game> DeserializeSeed<'de> for MapEntriesSeed<'a, G> {
    type Value = Vec<crate::MapEntry<SaveGameArchiveType<G>>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}

impl<'de, 'a, G: Game> Visitor<'de> for MapEntriesSeed<'a, G> {
    type Value = Vec<crate::MapEntry<SaveGameArchiveType<G>>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("array of map entries")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut vec = Vec::new();
        while let Some(elem) = seq.next_element_seed(MapEntrySeed::<G> {
            key_type: self.key_type,
            value_type: self.value_type,
            path: self.path,
            schemas: self.schemas,
            _game: PhantomData,
        })? {
            vec.push(elem);
        }
        Ok(vec)
    }
}

struct MapEntrySeed<'a, G: Game> {
    key_type: &'a PropertyTagDataPartial,
    value_type: &'a PropertyTagDataPartial,
    path: &'a str,
    schemas: &'a PropertySchemas,
    _game: PhantomData<G>,
}

impl<'de, 'a, G: Game> DeserializeSeed<'de> for MapEntrySeed<'a, G> {
    type Value = crate::MapEntry<SaveGameArchiveType<G>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_struct("MapEntry", &["key", "value"], self)
    }
}

impl<'de, 'a, G: Game> Visitor<'de> for MapEntrySeed<'a, G> {
    type Value = crate::MapEntry<SaveGameArchiveType<G>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("map entry with key and value fields")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Key,
            Value,
        }

        let mut key = None;
        let mut value = None;

        while let Some(field) = map.next_key()? {
            match field {
                Field::Key => {
                    key = Some(map.next_value_seed(PropertySeed::<G> {
                        tag: self.key_type,
                        path: self.path,
                        schemas: self.schemas,
                        _game: PhantomData,
                    })?);
                }
                Field::Value => {
                    value = Some(map.next_value_seed(PropertySeed::<G> {
                        tag: self.value_type,
                        path: self.path,
                        schemas: self.schemas,
                        _game: PhantomData,
                    })?);
                }
            }
        }

        let key = key.ok_or_else(|| serde::de::Error::missing_field("key"))?;
        let value = value.ok_or_else(|| serde::de::Error::missing_field("value"))?;

        Ok(crate::MapEntry { key, value })
    }
}

struct PropertiesSeed<'a, G: Game> {
    path: &'a str,
    schemas: &'a PropertySchemas,
    _game: PhantomData<G>,
}

impl<'de, 'a, G: Game> DeserializeSeed<'de> for PropertiesSeed<'a, G> {
    type Value = Properties<SaveGameArchiveType<G>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de, 'a, G: Game> Visitor<'de> for PropertiesSeed<'a, G> {
    type Value = Properties<SaveGameArchiveType<G>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("properties map")
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
                serde::de::Error::custom(format!("No schema for property: {prop_path}"))
            })?;

            let prop = map.next_value_seed(PropertySeed::<G> {
                tag: &tag.data,
                path: &prop_path,
                schemas: self.schemas,
                _game: PhantomData,
            })?;

            properties.insert(key, prop);
        }

        Ok(Properties(properties))
    }
}

// Deserialize implementation for Save, generic over the active game `G`. All
// property data is routed through the schema-aware seed pipeline above, so
// `StructType::Game` reaches `<G>::deserialize_struct` and typed game structs
// round-trip through JSON.
impl<'de, G: Game> Deserialize<'de> for Save<G> {
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

        struct SaveVisitor<G: Game>(PhantomData<G>);

        impl<'de, G: Game> Visitor<'de> for SaveVisitor<G> {
            type Value = Save<G>;

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

                            root = Some(map.next_value_seed(RootSeed::<G> {
                                path: "",
                                schemas: schemas_ref,
                                _game: PhantomData,
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
            SaveVisitor::<G>(PhantomData),
        )
    }
}
