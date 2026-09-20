use crate::games::palworld::types::PalDynamicId;
use crate::games::palworld::{bytes_remaining, PalItemId};
use crate::{ArchiveReader, ArchiveType, ArchiveWriter, Properties, Result, SaveGameArchiveType};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalItemContainer {
    pub permission: PalItemContainerPermission,
    pub trailing_unparsed_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalItemContainerPermission {
    pub type_a: Vec<u8>,
    pub type_b: Vec<u8>,
    pub item_static_ids: Vec<String>,
}

impl PalItemContainer {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let type_a_count = ar.read_u32::<LE>()?;
        let mut type_a = Vec::with_capacity(crate::bounded_prealloc(type_a_count as usize));
        for _ in 0..type_a_count {
            type_a.push(ar.read_u8()?);
        }

        let type_b_count = ar.read_u32::<LE>()?;
        let mut type_b = Vec::with_capacity(crate::bounded_prealloc(type_b_count as usize));
        for _ in 0..type_b_count {
            type_b.push(ar.read_u8()?);
        }

        let item_static_ids_count = ar.read_u32::<LE>()?;
        let mut item_static_ids =
            Vec::with_capacity(crate::bounded_prealloc(item_static_ids_count as usize));
        for _ in 0..item_static_ids_count {
            item_static_ids.push(ar.read_string()?);
        }

        let mut trailing_unparsed_data = Vec::new();
        if ar.read_to_end(&mut trailing_unparsed_data)? > 0 {
            Ok(PalItemContainer {
                permission: PalItemContainerPermission {
                    type_a,
                    type_b,
                    item_static_ids,
                },
                trailing_unparsed_data: Some(trailing_unparsed_data),
            })
        } else {
            Ok(PalItemContainer {
                permission: PalItemContainerPermission {
                    type_a,
                    type_b,
                    item_static_ids,
                },
                trailing_unparsed_data: None,
            })
        }
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.permission.type_a.len() as u32)?;
        for byte in &self.permission.type_a {
            ar.write_u8(*byte)?;
        }

        ar.write_u32::<LE>(self.permission.type_b.len() as u32)?;
        for byte in &self.permission.type_b {
            ar.write_u8(*byte)?;
        }

        ar.write_u32::<LE>(self.permission.item_static_ids.len() as u32)?;
        for id in &self.permission.item_static_ids {
            ar.write_string(id)?;
        }

        if let Some(trailing_data) = &self.trailing_unparsed_data {
            ar.write_all(trailing_data)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalItemContainerSlot {
    pub slot_index: i32,
    pub count: i32,
    pub item: PalItemId,
    pub trailing_bytes: Vec<u8>,
}

impl PalItemContainerSlot {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let slot_index = ar.read_i32::<LE>()?;
        let count = ar.read_i32::<LE>()?;
        let item = PalItemId::read(ar)?;

        let mut trailing_bytes = Vec::new();
        ar.read_to_end(&mut trailing_bytes)?;

        Ok(PalItemContainerSlot {
            slot_index,
            count,
            item,
            trailing_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.slot_index)?;
        ar.write_i32::<LE>(self.count)?;
        self.item.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize, T::SoftObjectPath: Serialize",
    deserialize = ""
))]
pub struct PalDynamicItem<T: ArchiveType = SaveGameArchiveType> {
    pub id: PalDynamicId,
    pub static_id: String,
    pub item_type: PalDynamicItemType<T>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize, T::SoftObjectPath: Serialize",
    deserialize = ""
))]
pub enum PalDynamicItemType<T: ArchiveType = SaveGameArchiveType> {
    Unknown {
        trailer: Vec<u8>,
    },
    Egg {
        leading_bytes: [u8; 4],
        character_id: String,
        object: Properties<T>,
        trailing_bytes: [u8; 28],
    },
    Armor {
        leading_bytes: [u8; 4],
        durability: f32,
        trailing_bytes: [u8; 4],
    },
    Weapon {
        leading_bytes: [u8; 4],
        durability: f32,
        remaining_bullets: i32,
        passive_skill_list: Vec<String>,
        unknown_str: Option<String>,
        trailing_bytes: [u8; 4],
    },
}

impl<T: ArchiveType> PalDynamicItem<T> {
    pub fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        let id = PalDynamicId::read(ar)?;
        let static_id = ar.read_string()?;

        let remaining_start = ar.stream_position()?;

        if let Ok(egg_data) = try_parse_egg(ar) {
            return Ok(PalDynamicItem {
                id,
                static_id,
                item_type: egg_data,
            });
        }

        ar.seek(std::io::SeekFrom::Start(remaining_start))?;
        if let Ok(weapon_data) = try_parse_weapon(ar) {
            return Ok(PalDynamicItem {
                id,
                static_id,
                item_type: weapon_data,
            });
        }

        ar.seek(std::io::SeekFrom::Start(remaining_start))?;
        if let Ok(armor_data) = try_parse_armor(ar) {
            return Ok(PalDynamicItem {
                id,
                static_id,
                item_type: armor_data,
            });
        }

        ar.seek(std::io::SeekFrom::Start(remaining_start))?;
        let mut trailer = Vec::new();
        ar.read_to_end(&mut trailer)?;

        Ok(PalDynamicItem {
            id,
            static_id,
            item_type: PalDynamicItemType::Unknown { trailer },
        })
    }
    pub fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        self.id.write(ar)?;
        ar.write_string(&self.static_id)?;

        match &self.item_type {
            PalDynamicItemType::Unknown { trailer } => {
                ar.write_all(trailer)?;
            }
            PalDynamicItemType::Egg {
                leading_bytes,
                character_id,
                object,
                trailing_bytes,
            } => {
                ar.write_all(leading_bytes)?;
                ar.write_string(character_id)?;
                crate::write_properties_none_terminated(ar, object)?;
                ar.write_all(trailing_bytes)?;
            }
            PalDynamicItemType::Armor {
                leading_bytes,
                durability,
                trailing_bytes,
            } => {
                ar.write_all(leading_bytes)?;
                ar.write_f32::<LE>(*durability)?;
                ar.write_all(trailing_bytes)?;
            }
            PalDynamicItemType::Weapon {
                leading_bytes,
                durability,
                remaining_bullets,
                passive_skill_list,
                unknown_str,
                trailing_bytes,
            } => {
                ar.write_all(leading_bytes)?;
                ar.write_f32::<LE>(*durability)?;
                ar.write_i32::<LE>(*remaining_bullets)?;

                ar.write_u32::<LE>(passive_skill_list.len() as u32)?;
                for skill in passive_skill_list {
                    ar.write_string(skill)?;
                }

                if let Some(unknown_str) = unknown_str {
                    ar.write_string(unknown_str)?;
                }

                ar.write_all(trailing_bytes)?;
            }
        }

        Ok(())
    }
}

fn try_parse_egg<T: ArchiveType, A: ArchiveReader<ArchiveType = T>>(
    ar: &mut A,
) -> Result<PalDynamicItemType<T>> {
    let start_pos = ar.stream_position()?;

    let mut leading_bytes = [0u8; 4];
    ar.read_exact(&mut leading_bytes)?;

    let character_id = ar.read_string()?;
    let object = crate::read_properties_until_none(ar)?;

    let mut trailing_bytes = [0u8; 28];
    ar.read_exact(&mut trailing_bytes)?;

    let mut test_byte = [0u8; 1];
    if ar.read(&mut test_byte)? != 0 {
        ar.seek(std::io::SeekFrom::Start(start_pos))?;
        return Err(crate::Error::Other("Not an egg".to_string()));
    }

    Ok(PalDynamicItemType::Egg {
        leading_bytes,
        character_id,
        object,
        trailing_bytes,
    })
}

fn try_parse_weapon<T: ArchiveType, A: ArchiveReader<ArchiveType = T>>(
    ar: &mut A,
) -> Result<PalDynamicItemType<T>> {
    let start_pos = ar.stream_position()?;

    let mut leading_bytes = [0u8; 4];
    ar.read_exact(&mut leading_bytes)?;

    let durability = ar.read_f32::<LE>()?;
    let remaining_bullets = ar.read_i32::<LE>()?;

    let skill_count = ar.read_u32::<LE>()?;
    let mut passive_skill_list = Vec::with_capacity(crate::bounded_prealloc(skill_count as usize));
    for _ in 0..skill_count {
        passive_skill_list.push(ar.read_string()?);
    }

    let unknown_str = if bytes_remaining(ar)? > 4 {
        Some(ar.read_string()?)
    } else {
        None
    };

    let mut trailing_bytes = [0u8; 4];
    ar.read_exact(&mut trailing_bytes)?;

    let mut test_byte = [0u8; 1];
    if ar.read(&mut test_byte)? != 0 {
        ar.seek(std::io::SeekFrom::Start(start_pos))?;
        return Err(crate::Error::Other("Not a weapon".to_string()));
    }

    Ok(PalDynamicItemType::Weapon {
        leading_bytes,
        durability,
        remaining_bullets,
        passive_skill_list,
        unknown_str,
        trailing_bytes,
    })
}

fn try_parse_armor<T: ArchiveType, A: ArchiveReader<ArchiveType = T>>(
    ar: &mut A,
) -> Result<PalDynamicItemType<T>> {
    let start_pos = ar.stream_position()?;

    let mut leading_bytes = [0u8; 4];
    ar.read_exact(&mut leading_bytes)?;

    let durability = ar.read_f32::<LE>()?;

    let mut trailing_bytes = [0u8; 4];
    ar.read_exact(&mut trailing_bytes)?;

    let mut test_byte = [0u8; 1];
    if ar.read(&mut test_byte)? != 0 {
        ar.seek(std::io::SeekFrom::Start(start_pos))?;
        return Err(crate::Error::Other("Not armor".to_string()));
    }

    Ok(PalDynamicItemType::Armor {
        leading_bytes,
        durability,
        trailing_bytes,
    })
}
