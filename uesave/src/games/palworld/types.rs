use crate::{ArchiveReader, ArchiveWriter, FGuid, Quat, Result, Vector};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalItemId {
    pub static_id: String,
    pub dynamic_id: PalDynamicId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalDynamicId {
    pub created_world_id: FGuid,
    pub local_id_in_created_world: FGuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalItemAndNum {
    pub item_id: PalItemId,
    pub num: u32,
}

impl PalItemId {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalItemId {
            static_id: ar.read_string()?,
            dynamic_id: PalDynamicId::read(ar)?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.static_id)?;
        self.dynamic_id.write(ar)?;
        Ok(())
    }
}

impl PalDynamicId {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalDynamicId {
            created_world_id: FGuid::read(ar)?,
            local_id_in_created_world: FGuid::read(ar)?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.created_world_id.write(ar)?;
        self.local_id_in_created_world.write(ar)?;
        Ok(())
    }
}

impl PalItemAndNum {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalItemAndNum {
            item_id: PalItemId::read(ar)?,
            num: ar.read_u32::<LE>()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.item_id.write(ar)?;
        ar.write_u32::<LE>(self.num)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalInstanceId {
    pub guid: FGuid,
    pub instance_id: FGuid,
}

impl PalInstanceId {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalInstanceId {
            guid: FGuid::read(ar)?,
            instance_id: FGuid::read(ar)?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.guid.write(ar)?;
        self.instance_id.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalPlayerInfo {
    pub player_uid: FGuid,
    pub player_info: PalPlayerInfoDetails,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalPlayerInfoDetails {
    pub last_online_real_time: i64,
    pub player_name: String,
}

impl PalPlayerInfo {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalPlayerInfo {
            player_uid: FGuid::read(ar)?,
            player_info: PalPlayerInfoDetails {
                last_online_real_time: ar.read_i64::<LE>()?,
                player_name: ar.read_string()?,
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.player_uid.write(ar)?;
        ar.write_i64::<LE>(self.player_info.last_online_real_time)?;
        ar.write_string(&self.player_info.player_name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalTransform {
    pub rotation: Quat,
    pub translation: Vector,
    pub scale: Vector,
}

impl PalTransform {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalTransform {
            rotation: Quat::read(ar)?,
            translation: Vector::read(ar)?,
            scale: Vector::read(ar)?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.rotation.write(ar)?;
        self.translation.write(ar)?;
        self.scale.write(ar)?;
        Ok(())
    }
}
