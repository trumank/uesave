//! The Palworld game-struct enum reached through [`crate::StructValue::Game`].
//!
//! Each variant is one Palworld struct type, parsed from an opaque byte array
//! embedded in the GVAS tree (see the [module docs](super)).

use serde::Serialize;

use crate::archive::{ArchiveType, ArchiveWriter};
use crate::game::GameStruct;
use crate::{Result, SaveGameArchiveType};

use super::{
    PalBaseCamp, PalBuildProcess, PalCharacterContainer, PalCharacterData, PalConnector,
    PalDynamicItem, PalGroupData, PalGuildItemStorage, PalGuildLab, PalItemContainer,
    PalItemContainerSlot, PalMapConcreteModel, PalMapConcreteModelModule, PalMapModel, PalWork,
    PalWorkAssign, PalWorkCollection, PalWorkerDirector,
};

/// The Palworld struct values, parsed from `RawData` byte arrays. Larger types
/// are boxed to keep the enum size down.
///
/// `#[serde(untagged)]` so each variant serializes as just its payload; the
/// variant name is recovered from the schema on the way back in.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
#[serde(bound(serialize = "T::ObjectRef: Serialize, T::SoftObjectPath: Serialize"))]
pub enum PalStruct<T: ArchiveType = SaveGameArchiveType> {
    CharacterData(PalCharacterData<T>),
    ItemContainer(PalItemContainer),
    GroupData(PalGroupData),
    DynamicItem(std::boxed::Box<PalDynamicItem<T>>),
    BuildProcess(PalBuildProcess),
    GuildItemStorage(PalGuildItemStorage),
    GuildLab(PalGuildLab),
    ItemContainerSlots(PalItemContainerSlot),
    CharacterContainer(PalCharacterContainer),
    Connector(PalConnector),
    BaseCamp(std::boxed::Box<PalBaseCamp>),
    Work(std::boxed::Box<PalWork>),
    WorkAssign(PalWorkAssign),
    MapModel(std::boxed::Box<PalMapModel>),
    MapConcreteModel(std::boxed::Box<PalMapConcreteModel<T>>),
    MapConcreteModelModule(PalMapConcreteModelModule),
    WorkerDirector(std::boxed::Box<PalWorkerDirector>),
    WorkCollection(PalWorkCollection),
}

impl<T: ArchiveType> GameStruct<T> for PalStruct<T> {
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        match self {
            PalStruct::CharacterData(v) => v.write(ar)?,
            PalStruct::ItemContainer(v) => v.write(ar)?,
            PalStruct::GroupData(v) => v.write(ar)?,
            PalStruct::DynamicItem(v) => v.write(ar)?,
            PalStruct::BuildProcess(v) => v.write(ar)?,
            PalStruct::GuildItemStorage(v) => v.write(ar)?,
            PalStruct::GuildLab(v) => v.write(ar)?,
            PalStruct::ItemContainerSlots(v) => v.write(ar)?,
            PalStruct::CharacterContainer(v) => v.write(ar)?,
            PalStruct::Connector(v) => v.write(ar)?,
            PalStruct::BaseCamp(v) => v.write(ar)?,
            PalStruct::Work(v) => v.write(ar)?,
            PalStruct::WorkAssign(v) => v.write(ar)?,
            PalStruct::MapModel(v) => v.write(ar)?,
            PalStruct::MapConcreteModel(v) => v.write(ar)?,
            PalStruct::MapConcreteModelModule(v) => v.write(ar)?,
            PalStruct::WorkerDirector(v) => v.write(ar)?,
            PalStruct::WorkCollection(v) => v.write(ar)?,
        }
        Ok(())
    }
}

/// The bare Palworld struct-type names, mapped to their canonical
/// `/Script/Pal.PalXxx` paths.
pub(crate) const PAL_STRUCT_TYPES: &[(&str, &str)] = &[
    ("PalCharacterData", "/Script/Pal.PalCharacterData"),
    ("PalItemContainer", "/Script/Pal.PalItemContainer"),
    ("PalGroupData", "/Script/Pal.PalGroupData"),
    ("PalDynamicItem", "/Script/Pal.PalDynamicItem"),
    ("PalBuildProcess", "/Script/Pal.PalBuildProcess"),
    ("PalGuildItemStorage", "/Script/Pal.PalGuildItemStorage"),
    ("PalGuildLab", "/Script/Pal.PalGuildLab"),
    ("PalItemContainerSlots", "/Script/Pal.PalItemContainerSlots"),
    ("PalCharacterContainer", "/Script/Pal.PalCharacterContainer"),
    ("PalConnector", "/Script/Pal.PalConnector"),
    ("PalBaseCamp", "/Script/Pal.PalBaseCamp"),
    ("PalWork", "/Script/Pal.PalWork"),
    ("PalWorkAssign", "/Script/Pal.PalWorkAssign"),
    ("PalMapModel", "/Script/Pal.PalMapModel"),
    ("PalMapConcreteModel", "/Script/Pal.PalMapConcreteModel"),
    (
        "PalMapConcreteModelModule",
        "/Script/Pal.PalMapConcreteModelModule",
    ),
    ("PalWorkerDirector", "/Script/Pal.PalWorkerDirector"),
    ("PalWorkCollection", "/Script/Pal.PalWorkCollection"),
];

/// Returns the bare (last-segment) struct-type name, whether given a bare name
/// or a full `/Script/Pal.PalXxx` path.
pub(crate) fn bare_name(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}
