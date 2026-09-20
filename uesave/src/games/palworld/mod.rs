//! Support for Palworld save files.
//!
//! Palworld embeds much of its data as opaque byte arrays (typically properties
//! named `RawData`) inside regular GVAS property trees. When the paths of these
//! properties are registered in [`Types`] (see [`palworld_types`]), the byte
//! arrays are transparently parsed into typed [`StructValue`]s on read and
//! serialized back to byte arrays on write.

pub mod base_camp;
pub mod build_process;
pub mod character;
pub mod compression;
pub mod connector;
pub mod groups;
pub mod guild;
pub mod items;
pub mod map_concrete_model;
pub mod map_concrete_model_module;
pub mod map_model;
pub mod map_object;
pub mod pal_struct;
pub mod types;
pub mod work;

pub use base_camp::*;
pub use build_process::*;
pub use character::*;
pub use compression::{CompressionFormat, MagicBytes};
pub use connector::*;
pub use groups::*;
pub use guild::*;
pub use items::*;
pub use map_concrete_model::*;
pub use map_concrete_model_module::*;
pub use map_model::*;
pub use pal_struct::PalStruct;
pub use types::*;
pub use work::*;

use crate::game::Game;
use crate::{
    ArchiveReader, ByteArray, Property, PropertyKey, PropertyTagDataPartial, PropertyTagPartial,
    Result, Save, SaveGameArchive, SaveGameArchiveType, StructType, StructValue, Types, ValueVec,
};
use pal_struct::{bare_name, PAL_STRUCT_TYPES};
use std::io::{Cursor, Read, Seek, Write};

/// Path of the map object save data which needs context-dependent parsing of
/// the embedded data of its elements.
const MAP_OBJECT_SAVE_DATA_PATH: &str = "worldSaveData.MapObjectSaveData";

const GROUP_SAVE_DATA_PATH: &str = "worldSaveData.GroupSaveDataMap";

const WORK_SAVE_DATA_PATH: &str = "worldSaveData.WorkSaveData";

/// The Palworld game. Wires Palworld's embedded-struct parsing (see the module
/// docs) into the core archive via the [`Game`] trait.
#[derive(Default, Clone, PartialEq, Debug, serde::Serialize)]
pub struct Palworld;

impl Game for Palworld {
    type Struct<T: crate::ArchiveType> = PalStruct<T>;

    /// Does this type name (bare `PalXxx` or full `/Script/Pal.PalXxx` path)
    /// name a Palworld struct parsed from bytes?
    fn is_game_struct_type(full_path: &str) -> bool {
        let name = bare_name(full_path);
        PAL_STRUCT_TYPES.iter().any(|(bare, _)| *bare == name)
    }

    fn read_struct<A: ArchiveReader>(
        ar: &mut A,
        name: &str,
    ) -> Result<Self::Struct<A::ArchiveType>> {
        Ok(match bare_name(name) {
            "PalCharacterData" => PalStruct::CharacterData(PalCharacterData::read(ar)?),
            "PalItemContainer" => PalStruct::ItemContainer(PalItemContainer::read(ar)?),
            "PalGroupData" => PalStruct::GroupData(PalGroupData::read(ar)?),
            "PalDynamicItem" => PalStruct::DynamicItem(PalDynamicItem::read(ar)?.into()),
            "PalBuildProcess" => PalStruct::BuildProcess(PalBuildProcess::read(ar)?),
            "PalGuildItemStorage" => PalStruct::GuildItemStorage(PalGuildItemStorage::read(ar)?),
            "PalGuildLab" => PalStruct::GuildLab(PalGuildLab::read(ar)?),
            "PalItemContainerSlots" => {
                PalStruct::ItemContainerSlots(PalItemContainerSlot::read(ar)?)
            }
            "PalCharacterContainer" => {
                PalStruct::CharacterContainer(PalCharacterContainer::read(ar)?)
            }
            "PalConnector" => PalStruct::Connector(PalConnector::read(ar)?),
            "PalBaseCamp" => PalStruct::BaseCamp(PalBaseCamp::read(ar)?.into()),
            "PalWork" => PalStruct::Work(PalWork::read(ar)?.into()),
            "PalWorkAssign" => {
                PalStruct::WorkAssign(PalWorkAssign::read_with_work_type(ar, "Unknown")?)
            }
            "PalMapModel" => PalStruct::MapModel(PalMapModel::read(ar)?.into()),
            "PalMapConcreteModel" => {
                PalStruct::MapConcreteModel(PalMapConcreteModel::read(ar)?.into())
            }
            "PalMapConcreteModelModule" => {
                PalStruct::MapConcreteModelModule(PalMapConcreteModelModule::read(ar)?)
            }
            other => {
                return Err(crate::Error::Other(format!(
                    "unknown Palworld struct type {other:?}"
                )))
            }
        })
    }

    /// Deserialize a Palworld struct by name.
    ///
    /// Self-describing variants deserialize directly from `d`. The three
    /// schema-threading variants (`PalCharacterData`, `PalDynamicItem`,
    /// `PalMapConcreteModel`) embed nested [`crate::Properties`] whose tags live
    /// in `schemas` at `{path}.{field}`; deriving their `Deserialize` reaches
    /// those `Properties` fields, which read the (schemas, path) context this
    /// installs so [`crate::ArchiveType::deserialize_properties`] can interpret
    /// them.
    fn deserialize_struct<'de, D, T: crate::ArchiveType>(
        name: &str,
        path: &str,
        schemas: &crate::PropertySchemas,
        d: D,
    ) -> std::result::Result<Self::Struct<T>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::Deserialize;
        Ok(match bare_name(name) {
            "PalCharacterData" => {
                let _ctx = crate::serialization::push_properties_ctx(schemas, path);
                PalStruct::CharacterData(PalCharacterData::<T>::deserialize(d)?)
            }
            "PalDynamicItem" => {
                let _ctx = crate::serialization::push_properties_ctx(schemas, path);
                PalStruct::DynamicItem(std::boxed::Box::new(PalDynamicItem::<T>::deserialize(d)?))
            }
            "PalMapConcreteModel" => {
                let _ctx = crate::serialization::push_properties_ctx(schemas, path);
                PalStruct::MapConcreteModel(std::boxed::Box::new(
                    PalMapConcreteModel::<T>::deserialize(d)?,
                ))
            }
            "PalItemContainer" => PalStruct::ItemContainer(PalItemContainer::deserialize(d)?),
            "PalGroupData" => PalStruct::GroupData(PalGroupData::deserialize(d)?),
            "PalBuildProcess" => PalStruct::BuildProcess(PalBuildProcess::deserialize(d)?),
            "PalGuildItemStorage" => {
                PalStruct::GuildItemStorage(PalGuildItemStorage::deserialize(d)?)
            }
            "PalGuildLab" => PalStruct::GuildLab(PalGuildLab::deserialize(d)?),
            "PalItemContainerSlots" => {
                PalStruct::ItemContainerSlots(PalItemContainerSlot::deserialize(d)?)
            }
            "PalCharacterContainer" => {
                PalStruct::CharacterContainer(PalCharacterContainer::deserialize(d)?)
            }
            "PalConnector" => PalStruct::Connector(PalConnector::deserialize(d)?),
            "PalBaseCamp" => PalStruct::BaseCamp(PalBaseCamp::deserialize(d)?.into()),
            "PalWork" => PalStruct::Work(PalWork::deserialize(d)?.into()),
            "PalWorkAssign" => PalStruct::WorkAssign(PalWorkAssign::deserialize(d)?),
            "PalMapModel" => PalStruct::MapModel(PalMapModel::deserialize(d)?.into()),
            "PalMapConcreteModelModule" => {
                PalStruct::MapConcreteModelModule(PalMapConcreteModelModule::deserialize(d)?)
            }
            other => {
                return Err(serde::de::Error::custom(format!(
                    "unknown Palworld struct type {other:?}"
                )))
            }
        })
    }

    /// Called for every property after it has been read (with the property
    /// name on the scope). Converts Palworld data embedded as byte arrays
    /// into typed struct values when the current path is registered with a
    /// Pal struct type in the [`Types`] specification, updating `tag` (and
    /// thereby the recorded schema) to match.
    fn process_property_for_read<R: Read + Seek>(
        ar: &mut SaveGameArchive<R, Self>,
        tag: &mut PropertyTagPartial,
        value: Property<SaveGameArchiveType<Self>>,
    ) -> Result<Property<SaveGameArchiveType<Self>>> {
        let Some(hint) = ar.get_type().cloned() else {
            return Ok(value);
        };

        // MapObjectSaveData elements need context-dependent parsing (the embedded
        // data formats depend on sibling properties such as MapObjectId)
        if ar.scope.path() == MAP_OBJECT_SAVE_DATA_PATH {
            if let Property::Array(ValueVec::Struct(mut values)) = value {
                for (i, struct_value) in values.iter_mut().enumerate() {
                    match struct_value {
                        StructValue::Struct(properties) => {
                            map_object::parse_map_object_with_context(ar, properties)?;
                        }
                        other => {
                            return Err(crate::Error::Other(format!(
                                "Expected struct value for MapObjectSaveData element {}, got: {:?}",
                                i + 1,
                                other
                            )))
                        }
                    }
                }
                return Ok(Property::Array(ValueVec::Struct(values)));
            }
            return Ok(value);
        }

        // Group RawData depends on the sibling GroupType property: the blob itself
        // carries no marker of which group schema it follows.
        if ar.scope.path() == GROUP_SAVE_DATA_PATH {
            if let Property::Map(mut entries) = value {
                for entry in entries.iter_mut() {
                    let Property::Struct(StructValue::Struct(group_properties)) = &mut entry.value
                    else {
                        continue;
                    };
                    let group_type = match group_properties.0.get(&PropertyKey::from("GroupType")) {
                        Some(Property::Enum(t))
                        | Some(Property::Str(t))
                        | Some(Property::Name(t)) => t.clone(),
                        _ => continue,
                    };
                    if let Some(raw_data_prop) =
                        group_properties.0.get_mut(&PropertyKey::from("RawData"))
                    {
                        map_object::convert_embedded(
                            ar,
                            raw_data_prop,
                            // Map entries do not push Key/Value scope segments, so
                            // value properties live directly under the map path
                            &["RawData"],
                            StructType::Game("PalGroupData".to_owned()),
                            move |nested| {
                                Ok(StructValue::Game(PalStruct::GroupData(
                                    groups::PalGroupData::read_with_group_type(
                                        nested,
                                        &group_type,
                                    )?,
                                )))
                            },
                        )?;
                    }
                }
                return Ok(Property::Map(entries));
            }
            return Ok(value);
        }

        // Work RawData depends on the sibling WorkableType property, which also
        // decides the tail of each of the work's assignment records.
        if ar.scope.path() == WORK_SAVE_DATA_PATH {
            if let Property::Array(ValueVec::Struct(mut values)) = value {
                for struct_value in values.iter_mut() {
                    if let StructValue::Struct(work_properties) = struct_value {
                        work::parse_work_with_context(ar, work_properties)?;
                    }
                }
                return Ok(Property::Array(ValueVec::Struct(values)));
            }
            return Ok(value);
        }

        if !is_pal_struct_type(&hint) {
            return Ok(value);
        }
        let Property::Array(ValueVec::Byte(ByteArray::Byte(bytes))) = &value else {
            return Ok(value);
        };
        // Empty payloads stay as byte arrays and round-trip unchanged
        if bytes.is_empty() {
            return Ok(value);
        }

        let len = bytes.len() as u64;
        let parsed = ar.with_nested(Cursor::new(bytes.clone()), |nested| {
            let struct_value = StructValue::read(nested, &hint)?;
            // Refuse partial parses: unconsumed bytes would be lost on rewrite
            let consumed = nested.stream_position()?;
            if consumed != len {
                return Err(crate::Error::Other(format!(
                    "Palworld struct {hint:?} consumed only {consumed} of {len} bytes"
                )));
            }
            Ok(struct_value)
        });

        match parsed {
            Ok(struct_value) => {
                tag.data = PropertyTagDataPartial::Struct {
                    struct_type: hint,
                    id: Default::default(),
                };
                Ok(Property::Struct(struct_value))
            }
            Err(e) if ar.error_to_raw() => {
                if ar.log() {
                    eprintln!(
                        "Warning: Failed to parse Palworld data at '{}', leaving as raw bytes: {}",
                        ar.scope.path(),
                        e
                    );
                }
                Ok(value)
            }
            Err(e) => Err(e),
        }
    }

    /// Called for every property before it is written (with the property name on
    /// the scope). Serializes typed Palworld struct values back into the byte
    /// arrays they are embedded as, based on the schema recorded when reading.
    fn process_property_for_write<W: Write + Seek>(
        ar: &mut SaveGameArchive<W, Self>,
        _key: &PropertyKey,
        tag: &PropertyTagPartial,
        prop: &Property<SaveGameArchiveType<Self>>,
    ) -> Result<Option<(PropertyTagPartial, Property<SaveGameArchiveType<Self>>)>> {
        let PropertyTagDataPartial::Struct { struct_type, .. } = &tag.data else {
            return Ok(None);
        };
        if !is_pal_struct_type(struct_type) {
            return Ok(None);
        }

        let byte_array_tag = PropertyTagPartial {
            id: tag.id,
            data: PropertyTagDataPartial::Array(Box::new(PropertyTagDataPartial::Byte(None))),
        };

        match prop {
            // Empty/unparsed payloads were left as byte arrays on read
            Property::Array(ValueVec::Byte(ByteArray::Byte(_))) => {
                Ok(Some((byte_array_tag, prop.clone())))
            }
            Property::Struct(struct_value) => {
                let mut buf = Vec::new();
                ar.with_nested(Cursor::new(&mut buf), |nested| struct_value.write(nested))?;
                Ok(Some((
                    byte_array_tag,
                    Property::Array(ValueVec::Byte(ByteArray::Byte(buf))),
                )))
            }
            _ => Ok(None),
        }
    }

    /// Decode a Palworld save's container format (PLM/PLZ/CNK, or plain GVAS).
    fn decompress_save<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
        compression::decompress_save(reader)
    }

    /// Encode plain GVAS bytes into Palworld's canonical container: the
    /// double-zlib PLZ format (matches the reference tooling's default and
    /// `plz_round_trip`). Callers that need Oodle's PLM format explicitly use
    /// [`Save::<Palworld>::write_plm`] instead.
    fn compress_save(data: &[u8]) -> Result<Vec<u8>> {
        compression::compress_save(data, CompressionFormat::Zlib)
    }
}

impl Save<Palworld> {
    /// Write compressed in Palworld's double-zlib PLZ container. Equivalent to
    /// [`Save::write_compressed`] for `Palworld`, spelled out for callers that
    /// want the format explicit regardless of the trait's canonical default.
    pub fn write_plz<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.write_container(CompressionFormat::Zlib, writer)
    }

    /// Write Oodle-compressed in Palworld's PLM container (requires the
    /// `oodle` feature).
    pub fn write_plm<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.write_container(CompressionFormat::Oodle, writer)
    }

    fn write_container<W: Write>(&self, format: CompressionFormat, writer: &mut W) -> Result<()> {
        let mut buffer = Vec::new();
        self.write(&mut buffer)?;
        let output = compression::compress_save(&buffer, format)?;
        writer.write_all(&output)?;
        Ok(())
    }
}

#[cfg(feature = "cli")]
impl crate::games::registry::GameInfo for Palworld {
    const NAME: &'static str = "palworld";

    fn default_types() -> Types {
        palworld_types()
    }

    fn formats() -> &'static [&'static str] {
        #[cfg(feature = "oodle")]
        {
            &["oodle", "zlib"]
        }
        #[cfg(not(feature = "oodle"))]
        {
            &["zlib"]
        }
    }

    fn write_format(save: &Save<Self>, name: Option<&str>, w: &mut dyn Write) -> Result<()> {
        let mut w = w;
        match name {
            None => save.write(&mut w),
            Some("zlib") => save.write_plz(&mut w),
            Some("oodle") => save.write_plm(&mut w),
            Some(other) => Err(crate::Error::Other(format!(
                "unknown format {other:?} for game palworld"
            ))),
        }
    }
}

pub(crate) fn bytes_remaining<A: ArchiveReader>(ar: &mut A) -> Result<u64> {
    let position = ar.stream_position()?;
    let end = ar.seek(std::io::SeekFrom::End(0))?;
    ar.seek(std::io::SeekFrom::Start(position))?;
    Ok(end - position)
}

pub(crate) fn is_pal_struct_type(t: &StructType) -> bool {
    matches!(t, StructType::Game(name) if Palworld::is_game_struct_type(name))
}

/// Build a Palworld [`StructType`] (a `StructType::Game`) for the given bare
/// `PalXxx` name.
pub(crate) fn pal_struct_type(name: &str) -> StructType {
    StructType::Game(name.to_owned())
}

/// Build a [`Types`] specification for parsing Palworld save files (Level.sav,
/// LevelMeta.sav, Players/\*.sav, ...).
pub fn palworld_types() -> Types {
    let mut types = Types::new();

    let struct_hints = [
        "worldSaveData.CharacterContainerSaveData.Key",
        "worldSaveData.CharacterSaveParameterMap.Key",
        "worldSaveData.CharacterSaveParameterMap.Value",
        "worldSaveData.FoliageGridSaveDataMap.Key",
        "worldSaveData.FoliageGridSaveDataMap.Value",
        "worldSaveData.FoliageGridSaveDataMap.ModelMap.Value",
        "worldSaveData.FoliageGridSaveDataMap.ModelMap.InstanceDataMap.Key",
        "worldSaveData.FoliageGridSaveDataMap.ModelMap.InstanceDataMap.Value",
        "worldSaveData.ItemContainerSaveData.Key",
        "worldSaveData.ItemContainerSaveData.Value",
        "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.Value",
        "worldSaveData.MapObjectSaveData.Model.EffectMap.Value",
        "worldSaveData.MapObjectSpawnerInStageSaveData.Key",
        "worldSaveData.MapObjectSpawnerInStageSaveData.Value",
        "worldSaveData.MapObjectSpawnerInStageSaveData.Value.SpawnerDataMapByLevelObjectInstanceId.Value",
        "worldSaveData.MapObjectSpawnerInStageSaveData.Value.SpawnerDataMapByLevelObjectInstanceId.Value.ItemMap.Value",
        "worldSaveData.WorkSaveData.WorkAssignMap.Value",
        "worldSaveData.BaseCampSaveData.Value",
        "worldSaveData.BaseCampSaveData.ModuleMap.Value",
        "worldSaveData.CharacterContainerSaveData.Value",
        "worldSaveData.GroupSaveDataMap.Value",
        "worldSaveData.EnemyCampSaveData.EnemyCampStatusMap.Value",
        "worldSaveData.EnemyCampSaveData.EnemyCampStatusMap.Value.TreasureBoxInfoMapBySpawnerName.Value",
        "worldSaveData.DungeonSaveData.MapObjectSaveData.Model.EffectMap.Value",
        "worldSaveData.DungeonSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.Value",
        "worldSaveData.InvaderSaveData.Value",
        "worldSaveData.OilrigSaveData.OilrigMap.Value",
        "worldSaveData.SupplySaveData.SupplyInfos.Value",
        "worldSaveData.GuildExtraSaveDataMap.Value",
        "SaveData.Local_MaxFriendshipPalIds.Value",
        "SaveData.Local_MaxFriendshipPalIds.Key",
        "worldSaveData.MapObjectSpawnerInStageSaveData.SpawnerDataMapByLevelObjectInstanceId.Value",
        "worldSaveData.MapObjectSpawnerInStageSaveData.SpawnerDataMapByLevelObjectInstanceId.ItemMap.Value",
        "worldSaveData.DungeonSaveData.RewardSaveDataMap.Value",
        "worldSaveData.InLockerCharacterInstanceIDArray",
        "worldSaveData.EnemyCampSaveData.EnemyCampStatusMap.TreasureBoxInfoMapBySpawnerName.Value",
        "worldSaveData.FoliageGridSaveDataMap.ModelMap.RawData",
        "worldSaveData.FoliageGridSaveDataMap.ModelMap.InstanceDataMap.RawData",
        "worldSaveData.BaseCampSaveData.WorkerDirector.RawData",
        "worldSaveData.BaseCampSaveData.WorkCollection.RawData",
        MAP_OBJECT_SAVE_DATA_PATH,
        GROUP_SAVE_DATA_PATH,
        WORK_SAVE_DATA_PATH,
    ];
    for path in struct_hints {
        types.add(path.to_string(), StructType::Struct(None));
    }

    let guid_hints = [
        "worldSaveData.InvaderDeclarationSaveData.ValidatedStartPointIds",
        "worldSaveData.MapObjectSpawnerInStageSaveData.Value.SpawnerDataMapByLevelObjectInstanceId.Key",
        "worldSaveData.BaseCampSaveData.Key",
        "worldSaveData.GroupSaveDataMap.Key",
        "worldSaveData.InvaderSaveData.Key",
        "worldSaveData.SupplySaveData.SupplyInfos.Key",
        "worldSaveData.GuildExtraSaveDataMap.Key",
        "worldSaveData.MapObjectSpawnerInStageSaveData.SpawnerDataMapByLevelObjectInstanceId.Key",
        "worldSaveData.DungeonSaveData.RewardSaveDataMap.Key",
    ];
    for path in guid_hints {
        types.add(path.to_string(), StructType::Guid);
    }

    // Embedded (RawData) properties parsed into typed Palworld structs.
    // GroupSaveDataMap and WorkSaveData are absent on purpose: their RawData
    // needs a sibling property (GroupType / WorkableType) to be parsed, so they
    // go through context-dependent parsing.
    let pal_hints = [
        (
            "worldSaveData.CharacterSaveParameterMap.RawData",
            "PalCharacterData",
        ),
        (
            "worldSaveData.ItemContainerSaveData.RawData",
            "PalItemContainer",
        ),
        (
            "worldSaveData.ItemContainerSaveData.Slots.RawData",
            "PalItemContainerSlots",
        ),
        (
            "worldSaveData.CharacterContainerSaveData.Slots.RawData",
            "PalCharacterContainer",
        ),
        (
            "worldSaveData.DynamicItemSaveData.RawData",
            "PalDynamicItem",
        ),
        ("worldSaveData.BaseCampSaveData.RawData", "PalBaseCamp"),
        (
            "worldSaveData.GuildExtraSaveDataMap.GuildItemStorage.RawData",
            "PalGuildItemStorage",
        ),
        (
            "worldSaveData.GuildExtraSaveDataMap.Lab.RawData",
            "PalGuildLab",
        ),
    ];
    for (path, name) in pal_hints {
        types.add(path.to_string(), pal_struct_type(name));
    }

    types
}

#[cfg(test)]
mod skip_list_parity_tests {
    use super::*;

    /// Paths intentionally treated as opaque byte payloads.
    ///
    /// For ordinary property traversal, this crate does not persist a `.Value`
    /// path segment when descending through a `MapProperty` value. `Value`/`Key`
    /// are only used as transient scope components while resolving the
    /// `MapProperty` key/value `StructType` tag (see `PropertyType::MapProperty`
    /// in `lib.rs`).
    ///
    /// As a result, the canonical internal path for that branch is
    /// `BaseCampSaveData.ModuleMap`.
    const SKIP_LIST: &[&str] = &[
        "worldSaveData.FoliageGridSaveDataMap",
        "worldSaveData.MapObjectSpawnerInStageSaveData",
        "worldSaveData.DungeonSaveData",
        "worldSaveData.EnemyCampSaveData",
        "worldSaveData.InvaderSaveData",
        "worldSaveData.DungeonPointMarkerSaveData",
        "worldSaveData.GameTimeSaveData",
        "worldSaveData.OilrigSaveData",
        "worldSaveData.SupplySaveData",
        "worldSaveData.BaseCampSaveData.ModuleMap",
    ];

    #[test]
    fn test_no_typed_codec_under_skip_list() {
        let types = palworld_types();
        for skip_path in SKIP_LIST {
            for suffix in ["", ".RawData", ".Value.RawData"] {
                let candidate = format!("{skip_path}{suffix}");
                if let Some(struct_type) = types.get(&candidate) {
                    assert!(
                        !is_pal_struct_type(struct_type),
                        "typed Palworld codec registered under skip-listed path: {candidate}"
                    );
                }
            }
        }
    }
}
