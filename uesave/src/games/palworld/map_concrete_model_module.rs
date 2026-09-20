use crate::{ArchiveReader, ArchiveWriter, FGuid, Result};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalModuleSlotIndexes {
    pub attribute: u8,
    pub indexes: Vec<i32>,
}

impl PalModuleSlotIndexes {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let attribute = ar.read_u8()?;
        let indexes_count = ar.read_u32::<LE>()?;
        let mut indexes = Vec::with_capacity(crate::bounded_prealloc(indexes_count as usize));
        for _ in 0..indexes_count {
            indexes.push(ar.read_i32::<LE>()?);
        }
        Ok(PalModuleSlotIndexes { attribute, indexes })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.attribute)?;
        ar.write_u32::<LE>(self.indexes.len() as u32)?;
        for index in &self.indexes {
            ar.write_i32::<LE>(*index)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalPlayerLockInfo {
    pub player_uid: FGuid,
    pub try_failed_count: i32,
    pub try_success_cache: u32,
}

impl PalPlayerLockInfo {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalPlayerLockInfo {
            player_uid: FGuid::read(ar)?,
            try_failed_count: ar.read_i32::<LE>()?,
            try_success_cache: ar.read_u32::<LE>()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.player_uid.write(ar)?;
        ar.write_i32::<LE>(self.try_failed_count)?;
        ar.write_u32::<LE>(self.try_success_cache)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PalMapConcreteModelModuleData {
    ItemContainer {
        target_container_id: FGuid,
        slot_attribute_indexes: Vec<PalModuleSlotIndexes>,
        all_slot_attribute: Vec<u8>,
        drop_item_at_disposed: u32,
        usage_type: u8,
        trailing_bytes: [u8; 4],
    },
    CharacterContainer {
        target_container_id: FGuid,
        trailing_bytes: [u8; 4],
    },
    Workee {
        target_work_id: FGuid,
        trailing_bytes: [u8; 4],
    },
    Energy,
    StatusObserver,
    ItemStack,
    Switch {
        switch_state: u8,
        trailing_bytes: [u8; 4],
    },
    PlayerRecord,
    BaseCampPassiveEffect,
    PasswordLock {
        lock_state: u8,
        password: String,
        player_infos: Vec<PalPlayerLockInfo>,
        trailing_bytes: [u8; 4],
    },
    RequireElementalAction {
        unlock_item: String,
        trailing_bytes: [u8; 12],
    },
    GuildSecurity {
        /// `EPalGuildRole` values.
        allowed_roles: Vec<u8>,
        trailing_bytes: [u8; 4],
    },
    ColorSetting {
        color_entries: Vec<PalColorSettingEntry>,
        trailing_bytes: [u8; 4],
    },
    OperationalLoad {
        current_load: f32,
        trailing_bytes: [u8; 8],
    },
    Unknown {
        module_type: String,
        raw_bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalColorSettingEntry {
    pub key: String,
    pub color: PalLinearColor,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalLinearColor {
    pub r: f32,
    pub g: f32,
    pub b: f32,
    pub a: f32,
}

impl PalColorSettingEntry {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalColorSettingEntry {
            key: ar.read_string()?,
            color: PalLinearColor {
                r: ar.read_f32::<LE>()?,
                g: ar.read_f32::<LE>()?,
                b: ar.read_f32::<LE>()?,
                a: ar.read_f32::<LE>()?,
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.key)?;
        ar.write_f32::<LE>(self.color.r)?;
        ar.write_f32::<LE>(self.color.g)?;
        ar.write_f32::<LE>(self.color.b)?;
        ar.write_f32::<LE>(self.color.a)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapConcreteModelModule {
    pub module_type: String,
    pub data: PalMapConcreteModelModuleData,
    pub custom_version_data: Vec<u8>,
}

impl PalMapConcreteModelModule {
    pub(crate) fn read_with_module_type<A: ArchiveReader>(
        ar: &mut A,
        module_type: &str,
        bytes: Vec<u8>,
        custom_version_data: Vec<u8>,
    ) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(PalMapConcreteModelModule {
                module_type: module_type.to_string(),
                data: match module_type {
                    "EPalMapObjectConcreteModelModuleType::Energy" => {
                        PalMapConcreteModelModuleData::Energy
                    }
                    "EPalMapObjectConcreteModelModuleType::StatusObserver" => {
                        PalMapConcreteModelModuleData::StatusObserver
                    }
                    "EPalMapObjectConcreteModelModuleType::ItemStack" => {
                        PalMapConcreteModelModuleData::ItemStack
                    }
                    "EPalMapObjectConcreteModelModuleType::PlayerRecord" => {
                        PalMapConcreteModelModuleData::PlayerRecord
                    }
                    "EPalMapObjectConcreteModelModuleType::BaseCampPassiveEffect" => {
                        PalMapConcreteModelModuleData::BaseCampPassiveEffect
                    }
                    _ => PalMapConcreteModelModuleData::Unknown {
                        module_type: module_type.to_string(),
                        raw_bytes: bytes,
                    },
                },
                custom_version_data,
            });
        }

        // `ar` is expected to be positioned at the start of the module payload
        // (the caller parses the RawData bytes through a nested archive), so the
        // module data can be read from it directly.
        let data: PalMapConcreteModelModuleData =
            (|byte_reader: &mut A| -> Result<PalMapConcreteModelModuleData> {
                match module_type {
                    "EPalMapObjectConcreteModelModuleType::ItemContainer" => {
                        let target_container_id = FGuid::read(byte_reader)?;
                        let slot_count = byte_reader.read_u32::<LE>()?;
                        let mut slot_attribute_indexes =
                            Vec::with_capacity(crate::bounded_prealloc(slot_count as usize));
                        for _ in 0..slot_count {
                            slot_attribute_indexes.push(PalModuleSlotIndexes::read(byte_reader)?);
                        }
                        let all_slot_count = byte_reader.read_u32::<LE>()?;
                        let mut all_slot_attribute =
                            Vec::with_capacity(crate::bounded_prealloc(all_slot_count as usize));
                        for _ in 0..all_slot_count {
                            all_slot_attribute.push(byte_reader.read_u8()?);
                        }
                        let drop_item_at_disposed = byte_reader.read_u32::<LE>()?;
                        let usage_type = byte_reader.read_u8()?;
                        let mut trailing_bytes = [0u8; 4];
                        byte_reader.read_exact(&mut trailing_bytes)?;

                        Ok(PalMapConcreteModelModuleData::ItemContainer {
                            target_container_id,
                            slot_attribute_indexes,
                            all_slot_attribute,
                            drop_item_at_disposed,
                            usage_type,
                            trailing_bytes,
                        })
                    }
                    "EPalMapObjectConcreteModelModuleType::CharacterContainer" => {
                        let target_container_id = FGuid::read(byte_reader)?;
                        let mut trailing_bytes = [0u8; 4];
                        byte_reader.read_exact(&mut trailing_bytes)?;
                        Ok(PalMapConcreteModelModuleData::CharacterContainer {
                            target_container_id,
                            trailing_bytes,
                        })
                    }
                    "EPalMapObjectConcreteModelModuleType::Workee" => {
                        let target_work_id = FGuid::read(byte_reader)?;
                        let mut trailing_bytes = [0u8; 4];
                        byte_reader.read_exact(&mut trailing_bytes)?;
                        Ok(PalMapConcreteModelModuleData::Workee {
                            target_work_id,
                            trailing_bytes,
                        })
                    }
                    "EPalMapObjectConcreteModelModuleType::Switch" => {
                        let switch_state = byte_reader.read_u8()?;
                        let mut trailing_bytes = [0u8; 4];
                        byte_reader.read_exact(&mut trailing_bytes)?;
                        Ok(PalMapConcreteModelModuleData::Switch {
                            switch_state,
                            trailing_bytes,
                        })
                    }
                    "EPalMapObjectConcreteModelModuleType::PasswordLock" => {
                        let lock_state = byte_reader.read_u8()?;
                        let password = byte_reader.read_string()?;
                        let player_count = byte_reader.read_u32::<LE>()?;
                        let mut player_infos =
                            Vec::with_capacity(crate::bounded_prealloc(player_count as usize));
                        for _ in 0..player_count {
                            player_infos.push(PalPlayerLockInfo::read(byte_reader)?);
                        }
                        let mut trailing_bytes = [0u8; 4];
                        byte_reader.read_exact(&mut trailing_bytes)?;
                        Ok(PalMapConcreteModelModuleData::PasswordLock {
                            lock_state,
                            password,
                            player_infos,
                            trailing_bytes,
                        })
                    }
                    "EPalMapObjectConcreteModelModuleType::RequireElementalAction" => {
                        let unlock_item = byte_reader.read_string()?;
                        let mut trailing_bytes = [0u8; 12];
                        byte_reader.read_exact(&mut trailing_bytes)?;
                        Ok(PalMapConcreteModelModuleData::RequireElementalAction {
                            unlock_item,
                            trailing_bytes,
                        })
                    }
                    "EPalMapObjectConcreteModelModuleType::GuildSecurity" => {
                        let role_count = byte_reader.read_u32::<LE>()?;
                        let mut allowed_roles =
                            Vec::with_capacity(crate::bounded_prealloc(role_count as usize));
                        for _ in 0..role_count {
                            allowed_roles.push(byte_reader.read_u8()?);
                        }
                        let mut trailing_bytes = [0u8; 4];
                        byte_reader.read_exact(&mut trailing_bytes)?;
                        Ok(PalMapConcreteModelModuleData::GuildSecurity {
                            allowed_roles,
                            trailing_bytes,
                        })
                    }
                    "EPalMapObjectConcreteModelModuleType::ColorSetting" => {
                        let entry_count = byte_reader.read_u32::<LE>()?;
                        let color_entries = crate::read_array(
                            entry_count,
                            byte_reader,
                            PalColorSettingEntry::read,
                        )?;
                        let mut trailing_bytes = [0u8; 4];
                        byte_reader.read_exact(&mut trailing_bytes)?;
                        Ok(PalMapConcreteModelModuleData::ColorSetting {
                            color_entries,
                            trailing_bytes,
                        })
                    }
                    "EPalMapObjectConcreteModelModuleType::OperationalLoad" => {
                        let current_load = byte_reader.read_f32::<LE>()?;
                        let mut trailing_bytes = [0u8; 8];
                        byte_reader.read_exact(&mut trailing_bytes)?;
                        Ok(PalMapConcreteModelModuleData::OperationalLoad {
                            current_load,
                            trailing_bytes,
                        })
                    }
                    "EPalMapObjectConcreteModelModuleType::Energy" => {
                        Ok(PalMapConcreteModelModuleData::Energy)
                    }
                    "EPalMapObjectConcreteModelModuleType::StatusObserver" => {
                        Ok(PalMapConcreteModelModuleData::StatusObserver)
                    }
                    "EPalMapObjectConcreteModelModuleType::ItemStack" => {
                        Ok(PalMapConcreteModelModuleData::ItemStack)
                    }
                    "EPalMapObjectConcreteModelModuleType::PlayerRecord" => {
                        Ok(PalMapConcreteModelModuleData::PlayerRecord)
                    }
                    "EPalMapObjectConcreteModelModuleType::BaseCampPassiveEffect" => {
                        Ok(PalMapConcreteModelModuleData::BaseCampPassiveEffect)
                    }
                    _ => {
                        let mut raw_bytes = Vec::new();
                        byte_reader.read_to_end(&mut raw_bytes)?;
                        Ok(PalMapConcreteModelModuleData::Unknown {
                            module_type: module_type.to_string(),
                            raw_bytes,
                        })
                    }
                }
            })(ar)?;

        Ok(PalMapConcreteModelModule {
            module_type: module_type.to_string(),
            data,
            custom_version_data,
        })
    }

    pub fn read<A: ArchiveReader>(_ar: &mut A) -> Result<Self> {
        Err(crate::Error::Other(
            "PalMapConcreteModelModule::read called without module_type context. Use read_with_module_type instead."
                .to_string(),
        ))
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match &self.data {
            PalMapConcreteModelModuleData::ItemContainer {
                target_container_id,
                slot_attribute_indexes,
                all_slot_attribute,
                drop_item_at_disposed,
                usage_type,
                trailing_bytes,
            } => {
                target_container_id.write(ar)?;
                ar.write_u32::<LE>(slot_attribute_indexes.len() as u32)?;
                for slot in slot_attribute_indexes {
                    slot.write(ar)?;
                }
                ar.write_u32::<LE>(all_slot_attribute.len() as u32)?;
                for attr in all_slot_attribute {
                    ar.write_u8(*attr)?;
                }
                ar.write_u32::<LE>(*drop_item_at_disposed)?;
                ar.write_u8(*usage_type)?;
                ar.write_all(trailing_bytes)?;
            }
            PalMapConcreteModelModuleData::CharacterContainer {
                target_container_id,
                trailing_bytes,
            } => {
                target_container_id.write(ar)?;
                ar.write_all(trailing_bytes)?;
            }
            PalMapConcreteModelModuleData::Workee {
                target_work_id,
                trailing_bytes,
            } => {
                target_work_id.write(ar)?;
                ar.write_all(trailing_bytes)?;
            }
            PalMapConcreteModelModuleData::Switch {
                switch_state,
                trailing_bytes,
            } => {
                ar.write_u8(*switch_state)?;
                ar.write_all(trailing_bytes)?;
            }
            PalMapConcreteModelModuleData::PasswordLock {
                lock_state,
                password,
                player_infos,
                trailing_bytes,
            } => {
                ar.write_u8(*lock_state)?;
                ar.write_string(password)?;
                ar.write_u32::<LE>(player_infos.len() as u32)?;
                for player_info in player_infos {
                    player_info.write(ar)?;
                }
                ar.write_all(trailing_bytes)?;
            }
            PalMapConcreteModelModuleData::RequireElementalAction {
                unlock_item,
                trailing_bytes,
            } => {
                ar.write_string(unlock_item)?;
                ar.write_all(trailing_bytes)?;
            }
            PalMapConcreteModelModuleData::GuildSecurity {
                allowed_roles,
                trailing_bytes,
            } => {
                ar.write_u32::<LE>(allowed_roles.len() as u32)?;
                for role in allowed_roles {
                    ar.write_u8(*role)?;
                }
                ar.write_all(trailing_bytes)?;
            }
            PalMapConcreteModelModuleData::ColorSetting {
                color_entries,
                trailing_bytes,
            } => {
                ar.write_u32::<LE>(color_entries.len() as u32)?;
                for entry in color_entries {
                    entry.write(ar)?;
                }
                ar.write_all(trailing_bytes)?;
            }
            PalMapConcreteModelModuleData::OperationalLoad {
                current_load,
                trailing_bytes,
            } => {
                ar.write_f32::<LE>(*current_load)?;
                ar.write_all(trailing_bytes)?;
            }
            PalMapConcreteModelModuleData::Energy
            | PalMapConcreteModelModuleData::StatusObserver
            | PalMapConcreteModelModuleData::ItemStack
            | PalMapConcreteModelModuleData::PlayerRecord
            | PalMapConcreteModelModuleData::BaseCampPassiveEffect => {}
            PalMapConcreteModelModuleData::Unknown { raw_bytes, .. } => {
                ar.write_all(raw_bytes)?;
            }
        }
        Ok(())
    }
}
