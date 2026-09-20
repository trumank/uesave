use super::types::PalTransform;
use crate::{ArchiveReader, ArchiveWriter, FGuid, Result};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectHp {
    pub current: i32,
    pub max: i32,
}

impl PalMapObjectHp {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectHp {
            current: ar.read_i32::<LE>()?,
            max: ar.read_i32::<LE>()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.current)?;
        ar.write_i32::<LE>(self.max)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalStageInstanceId {
    pub id: FGuid,
    pub valid: u32,
}

impl PalStageInstanceId {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let id = FGuid::read(ar)?;
        let valid = ar.read_u32::<LE>()?;
        Ok(PalStageInstanceId { id, valid })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.id.write(ar)?;
        ar.write_u32::<LE>(self.valid)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapModel {
    pub instance_id: FGuid,
    pub concrete_model_instance_id: FGuid,
    pub base_camp_id_belong_to: FGuid,
    pub group_id_belong_to: FGuid,
    pub hp: PalMapObjectHp,
    pub initial_transform_cache: PalTransform,
    pub repair_work_id: FGuid,
    pub owner_spawner_level_object_instance_id: FGuid,
    pub owner_instance_id: FGuid,
    pub build_player_uid: FGuid,
    pub interact_restrict_type: u8,
    pub deterioration_damage: f32,
    pub stage_instance_id_belong_to: PalStageInstanceId,
    pub unknown_bytes: Vec<u8>,
}

impl PalMapModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapModel {
            instance_id: FGuid::read(ar)?,
            concrete_model_instance_id: FGuid::read(ar)?,
            base_camp_id_belong_to: FGuid::read(ar)?,
            group_id_belong_to: FGuid::read(ar)?,
            hp: PalMapObjectHp::read(ar)?,
            initial_transform_cache: PalTransform::read(ar)?,
            repair_work_id: FGuid::read(ar)?,
            owner_spawner_level_object_instance_id: FGuid::read(ar)?,
            owner_instance_id: FGuid::read(ar)?,
            build_player_uid: FGuid::read(ar)?,
            interact_restrict_type: ar.read_u8()?,
            deterioration_damage: ar.read_f32::<LE>()?,
            stage_instance_id_belong_to: PalStageInstanceId::read(ar)?,
            unknown_bytes: {
                let mut bytes = Vec::new();
                ar.read_to_end(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.instance_id.write(ar)?;
        self.concrete_model_instance_id.write(ar)?;
        self.base_camp_id_belong_to.write(ar)?;
        self.group_id_belong_to.write(ar)?;
        self.hp.write(ar)?;
        self.initial_transform_cache.write(ar)?;
        self.repair_work_id.write(ar)?;
        self.owner_spawner_level_object_instance_id.write(ar)?;
        self.owner_instance_id.write(ar)?;
        self.build_player_uid.write(ar)?;
        ar.write_u8(self.interact_restrict_type)?;
        ar.write_f32::<LE>(self.deterioration_damage)?;
        self.stage_instance_id_belong_to.write(ar)?;
        ar.write_all(&self.unknown_bytes)?;
        Ok(())
    }
}
