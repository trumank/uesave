use super::types::PalItemAndNum;
use crate::{ArchiveReader, ArchiveWriter, Result, SaveGameArchive, Vector};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// Module types that carry no payload.
const NO_OP_TYPES: &[&str] = &[
    "EPalBaseCampModuleType::Energy",
    "EPalBaseCampModuleType::Medical",
    "EPalBaseCampModuleType::ResourceCollector",
    "EPalBaseCampModuleType::ItemStorages",
    "EPalBaseCampModuleType::FacilityReservation",
    "EPalBaseCampModuleType::ObjectMaintenance",
    "EPalBaseCampModuleType::ItemStackInfo",
];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalTransportItemCharacterInfo {
    pub item_infos: Vec<PalItemAndNum>,
    pub character_location: Vector,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalBaseCampPassiveEffect {
    pub effect_type: u8,
    /// Present only for `WorkHard` (type 2).
    pub work_hard_type: Option<u8>,
    /// Present only for `WorkHard` (type 2); 12 bytes.
    pub unknown_trailer: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PalBaseCampModuleData {
    TransportItemDirector {
        character_infos: Vec<PalTransportItemCharacterInfo>,
        trailing_bytes: [u8; 4],
    },
    PassiveEffect {
        passive_effects: Vec<PalBaseCampPassiveEffect>,
    },
    NoOp,
    Unknown {
        data: Vec<u8>,
    },
}

/// `BaseCampSaveData.ModuleMap`'s `RawData`, keyed on the sibling module type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalBaseCampModule {
    pub module_type: String,
    pub data: PalBaseCampModuleData,
}

fn read_count<A: ArchiveReader>(ar: &mut A) -> Result<usize> {
    let count = ar.read_i32::<LE>()?;
    usize::try_from(count)
        .map_err(|_| crate::Error::Other(format!("negative array count: {count}")))
}

impl PalBaseCampModule {
    pub fn read_with_module_type<A: ArchiveReader>(ar: &mut A, module_type: &str) -> Result<Self> {
        let data = if NO_OP_TYPES.contains(&module_type) {
            PalBaseCampModuleData::NoOp
        } else {
            match module_type {
                "EPalBaseCampModuleType::TransportItemDirector" => {
                    let count = read_count(ar)?;
                    let mut character_infos = Vec::with_capacity(crate::bounded_prealloc(count));
                    for _ in 0..count {
                        let item_count = read_count(ar)?;
                        let mut item_infos =
                            Vec::with_capacity(crate::bounded_prealloc(item_count));
                        for _ in 0..item_count {
                            item_infos.push(PalItemAndNum::read(ar)?);
                        }
                        character_infos.push(PalTransportItemCharacterInfo {
                            item_infos,
                            character_location: Vector::read(ar)?,
                        });
                    }
                    let mut trailing_bytes = [0u8; 4];
                    ar.read_exact(&mut trailing_bytes)?;
                    PalBaseCampModuleData::TransportItemDirector {
                        character_infos,
                        trailing_bytes,
                    }
                }
                "EPalBaseCampModuleType::PassiveEffect" => {
                    let count = read_count(ar)?;
                    let mut passive_effects = Vec::with_capacity(crate::bounded_prealloc(count));
                    for _ in 0..count {
                        let effect_type = ar.read_u8()?;
                        let (work_hard_type, unknown_trailer) = if effect_type == 2 {
                            let work_hard_type = ar.read_u8()?;
                            let mut trailer = vec![0u8; 12];
                            ar.read_exact(&mut trailer)?;
                            (Some(work_hard_type), Some(trailer))
                        } else {
                            (None, None)
                        };
                        passive_effects.push(PalBaseCampPassiveEffect {
                            effect_type,
                            work_hard_type,
                            unknown_trailer,
                        });
                    }
                    PalBaseCampModuleData::PassiveEffect { passive_effects }
                }
                _ => {
                    let mut data = Vec::new();
                    ar.read_to_end(&mut data)?;
                    PalBaseCampModuleData::Unknown { data }
                }
            }
        };
        Ok(PalBaseCampModule {
            module_type: module_type.to_string(),
            data,
        })
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match &self.data {
            PalBaseCampModuleData::TransportItemDirector {
                character_infos,
                trailing_bytes,
            } => {
                ar.write_i32::<LE>(character_infos.len() as i32)?;
                for info in character_infos {
                    ar.write_i32::<LE>(info.item_infos.len() as i32)?;
                    for item in &info.item_infos {
                        item.write(ar)?;
                    }
                    info.character_location.write(ar)?;
                }
                ar.write_all(trailing_bytes)?;
            }
            PalBaseCampModuleData::PassiveEffect { passive_effects } => {
                ar.write_i32::<LE>(passive_effects.len() as i32)?;
                for effect in passive_effects {
                    ar.write_u8(effect.effect_type)?;
                    if effect.effect_type == 2 {
                        ar.write_u8(effect.work_hard_type.unwrap_or(0))?;
                        let trailer = effect
                            .unknown_trailer
                            .clone()
                            .unwrap_or_else(|| vec![0; 12]);
                        ar.write_all(&trailer)?;
                    }
                }
            }
            PalBaseCampModuleData::NoOp => {}
            PalBaseCampModuleData::Unknown { data } => ar.write_all(data)?,
        }
        Ok(())
    }

    /// Decode a module payload without a surrounding property tree. `bytes` is
    /// the module's raw `RawData` contents and `module_type` its sibling
    /// `EPalBaseCampModuleType::*` string; `engine_version_major` must match
    /// the source save's engine version, since it decides whether the
    /// `character_location` inside `TransportItemDirector` reads as f32 or f64.
    pub fn from_bytes(bytes: &[u8], module_type: &str, engine_version_major: u16) -> Result<Self> {
        let mut ar: SaveGameArchive<Cursor<&[u8]>> = SaveGameArchive::new(Cursor::new(bytes));
        ar.set_version(super::standalone_header(engine_version_major));
        Self::read_with_module_type(&mut ar, module_type)
    }

    /// Inverse of [`Self::from_bytes`]: re-encode this module back into the
    /// bytes its `RawData` property carries. `engine_version_major` must match
    /// what the module was decoded with.
    pub fn to_bytes(&self, engine_version_major: u16) -> Result<Vec<u8>> {
        let mut ar: SaveGameArchive<Cursor<Vec<u8>>> =
            SaveGameArchive::new(Cursor::new(Vec::new()));
        ar.set_version(super::standalone_header(engine_version_major));
        self.write(&mut ar)?;
        Ok(ar.stream.into_inner())
    }
}
