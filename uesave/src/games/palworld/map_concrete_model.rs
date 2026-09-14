use crate::{
    games::palworld::{bytes_remaining, PalInstanceId, PalItemAndNum, PalItemId},
    ArchiveReader, ArchiveType, ArchiveWriter, FGuid, Properties, Result, SaveGameArchiveType,
};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectCharacterTeamMissionModel {
    pub leading_bytes: [u8; 4],
    pub mission_id: String,
    pub assigned_individuals: Vec<PalInstanceId>,
    pub state: u8,
    pub start_time: i64,
    pub trailing_bytes: [u8; 4],
}

impl PalMapObjectCharacterTeamMissionModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectCharacterTeamMissionModel {
            leading_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            mission_id: ar.read_string()?,
            assigned_individuals: {
                let count = ar.read_u32::<LE>()?;
                crate::read_array(count, ar, PalInstanceId::read)?
            },
            state: ar.read_u8()?,
            start_time: ar.read_i64::<LE>()?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        ar.write_string(&self.mission_id)?;
        ar.write_u32::<LE>(self.assigned_individuals.len() as u32)?;
        for individual in &self.assigned_individuals {
            individual.write(ar)?;
        }
        ar.write_u8(self.state)?;
        ar.write_i64::<LE>(self.start_time)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectFarmSkillFruitsModel {
    pub leading_bytes: [u8; 4],
    pub skill_fruits_id: String,
    pub current_state: u8,
    pub progress_rate: f32,
    pub trailing_bytes: [u8; 20],
}

impl PalMapObjectFarmSkillFruitsModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectFarmSkillFruitsModel {
            leading_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            skill_fruits_id: ar.read_string()?,
            current_state: ar.read_u8()?,
            progress_rate: ar.read_f32::<LE>()?,
            trailing_bytes: {
                let mut bytes = [0u8; 20];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        ar.write_string(&self.skill_fruits_id)?;
        ar.write_u8(self.current_state)?;
        ar.write_f32::<LE>(self.progress_rate)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectSupplyStorageModel {
    pub created_at_real_time: i64,
    pub trailing_bytes: [u8; 8],
}

impl PalMapObjectSupplyStorageModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectSupplyStorageModel {
            created_at_real_time: ar.read_i64::<LE>()?,
            trailing_bytes: {
                let mut bytes = [0u8; 8];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i64::<LE>(self.created_at_real_time)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectItemBoothTradeInfo {
    pub product: PalItemAndNum,
    pub cost: PalItemAndNum,
    pub seller_player_uid: FGuid,
}

impl PalMapObjectItemBoothTradeInfo {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectItemBoothTradeInfo {
            product: PalItemAndNum::read(ar)?,
            cost: PalItemAndNum::read(ar)?,
            seller_player_uid: FGuid::read(ar)?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.product.write(ar)?;
        self.cost.write(ar)?;
        self.seller_player_uid.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectItemBoothModel {
    pub leading_bytes: [u8; 4],
    pub private_lock_player_uid: FGuid,
    pub trade_infos: Vec<PalMapObjectItemBoothTradeInfo>,
    pub trailing_bytes: [u8; 20],
}

impl PalMapObjectItemBoothModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let leading_bytes = {
            let mut bytes = [0u8; 4];
            ar.read_exact(&mut bytes)?;
            bytes
        };
        let private_lock_player_uid = FGuid::read(ar)?;
        let trade_infos_count = ar.read_u32::<LE>()?;
        let trade_infos =
            crate::read_array(trade_infos_count, ar, PalMapObjectItemBoothTradeInfo::read)?;
        let trailing_bytes = {
            let mut bytes = [0u8; 20];
            ar.read_exact(&mut bytes)?;
            bytes
        };
        Ok(PalMapObjectItemBoothModel {
            leading_bytes,
            private_lock_player_uid,
            trade_infos,
            trailing_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        self.private_lock_player_uid.write(ar)?;
        ar.write_u32::<LE>(self.trade_infos.len() as u32)?;
        for trade_info in &self.trade_infos {
            trade_info.write(ar)?;
        }
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectEnergyStorageModel {
    pub stored_energy_amount: f32,
    pub trailing_bytes: [u8; 8],
}

impl PalMapObjectEnergyStorageModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectEnergyStorageModel {
            stored_energy_amount: ar.read_f32::<LE>()?,
            trailing_bytes: {
                let mut bytes = [0u8; 8];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_f32::<LE>(self.stored_energy_amount)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectDeathDroppedCharacterModel {
    pub stored_parameter_id: FGuid,
    pub owner_player_uid: FGuid,
    pub trailing_bytes: Vec<u8>,
}

impl PalMapObjectDeathDroppedCharacterModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectDeathDroppedCharacterModel {
            stored_parameter_id: FGuid::read(ar)?,
            owner_player_uid: FGuid::read(ar)?,
            trailing_bytes: {
                let mut bytes = Vec::new();
                ar.read_to_end(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.stored_parameter_id.write(ar)?;
        self.owner_player_uid.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectConvertItemModel {
    pub leading_bytes: [u8; 4],
    pub current_recipe_id: String,
    pub requested_product_num: i32,
    pub remain_product_num: i32,
    pub work_speed_additional_rate: f32,
    pub trailing_bytes: [u8; 8],
}

impl PalMapObjectConvertItemModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectConvertItemModel {
            leading_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            current_recipe_id: ar.read_string()?,
            requested_product_num: ar.read_i32::<LE>()?,
            remain_product_num: ar.read_i32::<LE>()?,
            work_speed_additional_rate: ar.read_f32::<LE>()?,
            trailing_bytes: {
                let mut bytes = [0u8; 8];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        ar.write_string(&self.current_recipe_id)?;
        ar.write_i32::<LE>(self.requested_product_num)?;
        ar.write_i32::<LE>(self.remain_product_num)?;
        ar.write_f32::<LE>(self.work_speed_additional_rate)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalPickupGuard {
    pub pickupable_player_uid: FGuid,
    pub remove_pickup_guard_timer_handle: i64,
}

impl PalPickupGuard {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalPickupGuard {
            pickupable_player_uid: FGuid::read(ar)?,
            remove_pickup_guard_timer_handle: ar.read_i64::<LE>()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.pickupable_player_uid.write(ar)?;
        ar.write_i64::<LE>(self.remove_pickup_guard_timer_handle)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectPickupItemOnLevelModel {
    pub auto_picked_up: u32,
    /// Absent in saves written before the pickup guard joined the serialized base.
    pub pickup_guard: Option<PalPickupGuard>,
}

impl PalMapObjectPickupItemOnLevelModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let auto_picked_up = ar.read_u32::<LE>()?;
        let pickup_guard = if bytes_remaining(ar)? > 0 {
            Some(PalPickupGuard::read(ar)?)
        } else {
            None
        };
        Ok(PalMapObjectPickupItemOnLevelModel {
            auto_picked_up,
            pickup_guard,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.auto_picked_up)?;
        if let Some(pickup_guard) = &self.pickup_guard {
            pickup_guard.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectDropItemModel {
    pub auto_picked_up: u32,
    pub pickupable_player_uid: FGuid,
    pub remove_pickup_guard_timer_handle: i64,
    pub item_id: PalItemId,
    pub trailing_bytes: [u8; 4],
}

impl PalMapObjectDropItemModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectDropItemModel {
            auto_picked_up: ar.read_u32::<LE>()?,
            pickupable_player_uid: FGuid::read(ar)?,
            remove_pickup_guard_timer_handle: ar.read_i64::<LE>()?,
            item_id: PalItemId::read(ar)?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.auto_picked_up)?;
        self.pickupable_player_uid.write(ar)?;
        ar.write_i64::<LE>(self.remove_pickup_guard_timer_handle)?;
        self.item_id.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectItemDropOnDamagModel {
    pub drop_item_infos: Vec<PalItemAndNum>,
    pub trailing_bytes: Vec<u8>,
}

impl PalMapObjectItemDropOnDamagModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let drop_item_count = ar.read_u32::<LE>()?;
        let drop_item_infos = crate::read_array(drop_item_count, ar, PalItemAndNum::read)?;
        let mut trailing_bytes = Vec::new();
        ar.read_to_end(&mut trailing_bytes)?;
        Ok(PalMapObjectItemDropOnDamagModel {
            drop_item_infos,
            trailing_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.drop_item_infos.len() as u32)?;
        for item in &self.drop_item_infos {
            item.write(ar)?;
        }
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectDeathPenaltyStorageModel {
    pub auto_destroy_if_empty: u32,
    pub owner_player_uid: FGuid,
    pub created_at: i64,
    pub trailing_bytes: Vec<u8>,
}

impl PalMapObjectDeathPenaltyStorageModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let auto_destroy_if_empty = ar.read_u32::<LE>()?;
        let owner_player_uid = FGuid::read(ar)?;
        let created_at = ar.read_i64::<LE>()?;
        let mut trailing_bytes = Vec::new();
        ar.read_to_end(&mut trailing_bytes)?;
        Ok(PalMapObjectDeathPenaltyStorageModel {
            auto_destroy_if_empty,
            owner_player_uid,
            created_at,
            trailing_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.auto_destroy_if_empty)?;
        self.owner_player_uid.write(ar)?;
        ar.write_i64::<LE>(self.created_at)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectDefenseBulletLauncherModel {
    pub leading_bytes: [u8; 4],
    pub remaining_bullets: i32,
    pub magazine_size: i32,
    pub bullet_item_name: String,
    pub trailing_bytes: [u8; 4],
}

impl PalMapObjectDefenseBulletLauncherModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectDefenseBulletLauncherModel {
            leading_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            remaining_bullets: ar.read_i32::<LE>()?,
            magazine_size: ar.read_i32::<LE>()?,
            bullet_item_name: ar.read_string()?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        ar.write_i32::<LE>(self.remaining_bullets)?;
        ar.write_i32::<LE>(self.magazine_size)?;
        ar.write_string(&self.bullet_item_name)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectGenerateEnergyModel {
    pub generate_energy_rate_by_worker: f32,
    pub stored_energy_amount: f32,
    pub consume_energy_amount: f32,
}

impl PalMapObjectGenerateEnergyModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectGenerateEnergyModel {
            generate_energy_rate_by_worker: ar.read_f32::<LE>()?,
            stored_energy_amount: ar.read_f32::<LE>()?,
            consume_energy_amount: ar.read_f32::<LE>()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_f32::<LE>(self.generate_energy_rate_by_worker)?;
        ar.write_f32::<LE>(self.stored_energy_amount)?;
        ar.write_f32::<LE>(self.consume_energy_amount)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectFarmBlockV2ModelStateMachine {
    pub growup_required_time: f32,
    pub growup_progress_time: f32,
}

impl PalMapObjectFarmBlockV2ModelStateMachine {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectFarmBlockV2ModelStateMachine {
            growup_required_time: ar.read_f32::<LE>()?,
            growup_progress_time: ar.read_f32::<LE>()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_f32::<LE>(self.growup_required_time)?;
        ar.write_f32::<LE>(self.growup_progress_time)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectFarmBlockV2Model {
    pub crop_progress_rate: f32,
    pub crop_data_id: String,
    pub current_state: u8,
    pub crop_progress_rate_value: f32,
    pub water_stack_rate_value: f32,
    pub state_machine: PalMapObjectFarmBlockV2ModelStateMachine,
    pub trailing_bytes: [u8; 8],
}

impl PalMapObjectFarmBlockV2Model {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectFarmBlockV2Model {
            crop_progress_rate: ar.read_f32::<LE>()?,
            crop_data_id: ar.read_string()?,
            current_state: ar.read_u8()?,
            crop_progress_rate_value: ar.read_f32::<LE>()?,
            water_stack_rate_value: ar.read_f32::<LE>()?,
            state_machine: PalMapObjectFarmBlockV2ModelStateMachine::read(ar)?,
            trailing_bytes: {
                let mut bytes = [0u8; 8];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_f32::<LE>(self.crop_progress_rate)?;
        ar.write_string(&self.crop_data_id)?;
        ar.write_u8(self.current_state)?;
        ar.write_f32::<LE>(self.crop_progress_rate_value)?;
        ar.write_f32::<LE>(self.water_stack_rate_value)?;
        self.state_machine.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectFastTravelPointModel {
    pub location_instance_id: FGuid,
    pub trailing_bytes: Vec<u8>,
}

impl PalMapObjectFastTravelPointModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let location_instance_id = FGuid::read(ar)?;
        let mut trailing_bytes = Vec::new();
        if ar.read_to_end(&mut trailing_bytes)? > 0 {
            Ok(PalMapObjectFastTravelPointModel {
                location_instance_id,
                trailing_bytes,
            })
        } else {
            Ok(PalMapObjectFastTravelPointModel {
                location_instance_id,
                trailing_bytes: Vec::new(),
            })
        }
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.location_instance_id.write(ar)?;
        if !self.trailing_bytes.is_empty() {
            ar.write_all(&self.trailing_bytes)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectShippingItemModel {
    pub shipping_hours: Vec<i32>,
}

fn read_int_array<A: ArchiveReader>(count: u32, ar: &mut A) -> Result<Vec<i32>> {
    let mut vec = Vec::with_capacity(crate::bounded_prealloc(count as usize));
    for _ in 0..count {
        vec.push(ar.read_i32::<LE>()?);
    }
    Ok(vec)
}

impl PalMapObjectShippingItemModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let count = ar.read_u32::<LE>()?;
        Ok(PalMapObjectShippingItemModel {
            shipping_hours: read_int_array(count, ar)?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.shipping_hours.len() as u32)?;
        for hour in &self.shipping_hours {
            ar.write_i32::<LE>(*hour)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectProductItemModel {
    pub leading_bytes: [u8; 4],
    pub work_speed_additional_rate: f32,
    pub product_item_id: String,
    pub trailing_bytes: [u8; 4],
}

impl PalMapObjectProductItemModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectProductItemModel {
            leading_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            work_speed_additional_rate: ar.read_f32::<LE>()?,
            product_item_id: ar.read_string()?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        ar.write_f32::<LE>(self.work_speed_additional_rate)?;
        ar.write_string(&self.product_item_id)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectRecoverOtomoModel {
    pub recover_amount_by_sec: f32,
}

impl PalMapObjectRecoverOtomoModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectRecoverOtomoModel {
            recover_amount_by_sec: ar.read_f32::<LE>()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_f32::<LE>(self.recover_amount_by_sec)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize, T::SoftObjectPath: Serialize",
    deserialize = ""
))]
pub struct PalMapObjectHatchingEggModel<T: ArchiveType = SaveGameArchiveType> {
    pub leading_bytes: [u8; 4],
    pub hatched_character_save_parameter: Properties<T>,
    pub current_pal_egg_temp_diff: i32,
    pub hatched_character_guid: FGuid,
    pub trailing_bytes: [u8; 4],
}

impl<T: ArchiveType> PalMapObjectHatchingEggModel<T> {
    pub fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectHatchingEggModel {
            leading_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            hatched_character_save_parameter: crate::read_properties_until_none(ar)?,
            current_pal_egg_temp_diff: ar.read_i32::<LE>()?,
            hatched_character_guid: FGuid::read(ar)?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        crate::write_properties_none_terminated(ar, &self.hatched_character_save_parameter)?;
        ar.write_i32::<LE>(self.current_pal_egg_temp_diff)?;
        self.hatched_character_guid.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectTreasureBoxModel {
    pub treasure_grade_type: u8,
    pub treasure_special_type: u8,
    pub opened: u8,
    pub long_hold_interaction_duration: f32,
    pub interact_player_action_type: u8,
    pub is_lock_riding: u8,
}

impl PalMapObjectTreasureBoxModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectTreasureBoxModel {
            treasure_grade_type: ar.read_u8()?,
            treasure_special_type: ar.read_u8()?,
            opened: ar.read_u8()?,
            long_hold_interaction_duration: ar.read_f32::<LE>()?,
            interact_player_action_type: ar.read_u8()?,
            is_lock_riding: ar.read_u8()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.treasure_grade_type)?;
        ar.write_u8(self.treasure_special_type)?;
        ar.write_u8(self.opened)?;
        ar.write_f32::<LE>(self.long_hold_interaction_duration)?;
        ar.write_u8(self.interact_player_action_type)?;
        ar.write_u8(self.is_lock_riding)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalBreedFarmBreeding {
    pub last_proceed_worker_individual_ids: Vec<PalInstanceId>,
    pub target_breed_item_ids: Vec<String>,
}

impl PalBreedFarmBreeding {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let worker_count = ar.read_u32::<LE>()?;
        let last_proceed_worker_individual_ids =
            crate::read_array(worker_count, ar, PalInstanceId::read)?;
        let item_count = ar.read_u32::<LE>()?;
        let target_breed_item_ids = crate::read_array(item_count, ar, |r| r.read_string())?;
        Ok(PalBreedFarmBreeding {
            last_proceed_worker_individual_ids,
            target_breed_item_ids,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.last_proceed_worker_individual_ids.len() as u32)?;
        for worker in &self.last_proceed_worker_individual_ids {
            worker.write(ar)?;
        }
        ar.write_u32::<LE>(self.target_breed_item_ids.len() as u32)?;
        for item_id in &self.target_breed_item_ids {
            ar.write_string(item_id)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectBreedFarmModel {
    pub leading_bytes: [u8; 4],
    pub spawned_egg_instance_ids: Vec<FGuid>,
    pub trailing_bytes: [u8; 4],
    pub breeding: Option<PalBreedFarmBreeding>,
}

impl PalMapObjectBreedFarmModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let leading_bytes = {
            let mut bytes = [0u8; 4];
            ar.read_exact(&mut bytes)?;
            bytes
        };
        let egg_count = ar.read_u32::<LE>()?;
        let spawned_egg_instance_ids = crate::read_array(egg_count, ar, FGuid::read)?;
        let trailing_bytes = {
            let mut bytes = [0u8; 4];
            ar.read_exact(&mut bytes)?;
            bytes
        };
        let breeding = if bytes_remaining(ar)? > 0 {
            Some(PalBreedFarmBreeding::read(ar)?)
        } else {
            None
        };
        Ok(PalMapObjectBreedFarmModel {
            leading_bytes,
            spawned_egg_instance_ids,
            trailing_bytes,
            breeding,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        ar.write_u32::<LE>(self.spawned_egg_instance_ids.len() as u32)?;
        for egg_id in &self.spawned_egg_instance_ids {
            egg_id.write(ar)?;
        }
        ar.write_all(&self.trailing_bytes)?;
        if let Some(breeding) = &self.breeding {
            breeding.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectSignboardModel {
    pub leading_bytes: [u8; 4],
    pub signboard_text: String,
    pub last_modified_player_uid: FGuid,
    pub trailing_bytes: [u8; 4],
}

impl PalMapObjectSignboardModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectSignboardModel {
            leading_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            signboard_text: ar.read_string()?,
            last_modified_player_uid: FGuid::read(ar)?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        ar.write_string(&self.signboard_text)?;
        self.last_modified_player_uid.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectTorchModel {
    pub ignition_minutes: i32,
    pub extinction_date_time: i64,
    pub trailing_bytes: [u8; 4],
}

impl PalMapObjectTorchModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectTorchModel {
            ignition_minutes: ar.read_i32::<LE>()?,
            extinction_date_time: ar.read_i64::<LE>()?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.ignition_minutes)?;
        ar.write_i64::<LE>(self.extinction_date_time)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectPalEggModel {
    pub auto_picked_up: u32,
    pub pickupdable_player_uid: FGuid,
    pub remove_pickup_guard_timer_handle: i64,
}

impl PalMapObjectPalEggModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectPalEggModel {
            auto_picked_up: ar.read_u32::<LE>()?,
            pickupdable_player_uid: FGuid::read(ar)?,
            remove_pickup_guard_timer_handle: ar.read_i64::<LE>()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.auto_picked_up)?;
        self.pickupdable_player_uid.write(ar)?;
        ar.write_i64::<LE>(self.remove_pickup_guard_timer_handle)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectBaseCampPoint {
    pub leading_bytes: [u8; 4],
    pub base_camp_id: FGuid,
    pub trailing_bytes: [u8; 4],
}

impl PalMapObjectBaseCampPoint {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectBaseCampPoint {
            leading_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            base_camp_id: FGuid::read(ar)?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        self.base_camp_id.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectItemChestModel {
    pub leading_bytes: [u8; 4],
    pub private_lock_player_uid: FGuid,
    pub trailing_bytes: [u8; 4],
}

impl PalMapObjectItemChestModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectItemChestModel {
            leading_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            private_lock_player_uid: FGuid::read(ar)?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        self.private_lock_player_uid.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectItemChestAffectCorruption {
    pub leading_bytes: [u8; 4],
    pub private_lock_player_uid: FGuid,
    pub trailing_bytes: [u8; 4],
}

impl PalMapObjectItemChestAffectCorruption {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalMapObjectItemChestAffectCorruption {
            leading_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            private_lock_player_uid: FGuid::read(ar)?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.leading_bytes)?;
        self.private_lock_player_uid.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalLampLight {
    pub is_manually_turned_off: u32,
    pub unknown_bytes: [u8; 4],
}

impl PalLampLight {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalLampLight {
            is_manually_turned_off: ar.read_u32::<LE>()?,
            unknown_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.is_manually_turned_off)?;
        ar.write_all(&self.unknown_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalMapObjectLampModel {
    pub trailing_bytes: [u8; 4],
    /// Absent in saves written before the 2026-07 update.
    pub light: Option<PalLampLight>,
}

impl PalMapObjectLampModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let trailing_bytes = {
            let mut bytes = [0u8; 4];
            ar.read_exact(&mut bytes)?;
            bytes
        };
        let light = if bytes_remaining(ar)? > 0 {
            Some(PalLampLight::read(ar)?)
        } else {
            None
        };
        Ok(PalMapObjectLampModel {
            trailing_bytes,
            light,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_all(&self.trailing_bytes)?;
        if let Some(light) = &self.light {
            light.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseModel {
    pub trailing_bytes: Vec<u8>,
}

impl BaseModel {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let mut trailing_bytes = Vec::new();
        ar.read_to_end(&mut trailing_bytes)?;
        Ok(BaseModel { trailing_bytes })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        if !self.trailing_bytes.is_empty() {
            ar.write_all(&self.trailing_bytes)?;
        }
        Ok(())
    }
}

static DEFAULT_UNKNOWN_PAL_MAP_OBJECT_CONCRETE_MODEL_BASE: LazyLock<
    HashMap<&'static str, &'static str>,
> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert(
        "defenseminigun",
        "DEFAULT_UNKNOWN_PalMapObjectConcreteModelBase",
    );
    m.insert(
        "trap_leghold",
        "DEFAULT_UNKNOWN_PalMapObjectConcreteModelBase",
    );
    m.insert(
        "trap_leghold_big",
        "DEFAULT_UNKNOWN_PalMapObjectConcreteModelBase",
    );
    m.insert(
        "trap_mineattack",
        "DEFAULT_UNKNOWN_PalMapObjectConcreteModelBase",
    );
    m.insert(
        "trap_mineelecshock",
        "DEFAULT_UNKNOWN_PalMapObjectConcreteModelBase",
    );
    m.insert(
        "trap_minefreeze",
        "DEFAULT_UNKNOWN_PalMapObjectConcreteModelBase",
    );
    m.insert(
        "trap_movingpanel",
        "DEFAULT_UNKNOWN_PalMapObjectConcreteModelBase",
    );
    m.insert(
        "trap_noose",
        "DEFAULT_UNKNOWN_PalMapObjectConcreteModelBase",
    );
    m
});

static PAL_BUILD_OBJECT: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("andon", "PalBuildObject");
    m.insert("banyan_big", "PalBuildObject");
    m.insert("barrel01_iron", "PalBuildObject");
    m.insert("barrel02_iron", "PalBuildObject");
    m.insert("barrel03_iron", "PalBuildObject");
    m.insert("bathtub_stone", "PalBuildObject");
    m.insert("believer_banner", "PalBuildObject");
    m.insert("believer_flag", "PalBuildObject");
    m.insert("bonsai", "PalBuildObject");
    m.insert("box01_stone", "PalBuildObject");
    m.insert("byobu", "PalBuildObject");
    m.insert("cablecoil01_iron", "PalBuildObject");
    m.insert("clock01_stone", "PalBuildObject");
    m.insert("clock01_wall_iron", "PalBuildObject");
    m.insert("conservationgroupbannera", "PalBuildObject");
    m.insert("conservationgroupbannerb", "PalBuildObject");
    m.insert("counter_wood", "PalBuildObject");
    m.insert("curtain01_wall_stone", "PalBuildObject");
    m.insert("decal_palsticker_pinkcat", "PalBuildObject");
    m.insert("defensewall", "PalBuildObject");
    m.insert("defensewall_metal", "PalBuildObject");
    m.insert("defensewall_wood", "PalBuildObject");
    m.insert("desk01_iron", "PalBuildObject");
    m.insert("desk01_stone", "PalBuildObject");
    m.insert("enemycamp_andon", "PalBuildObject");
    m.insert("enemycamp_banyan_big", "PalBuildObject");
    m.insert("enemycamp_barrel01_iron", "PalBuildObject");
    m.insert("enemycamp_barrel02_iron", "PalBuildObject");
    m.insert("enemycamp_barrel03_iron", "PalBuildObject");
    m.insert("enemycamp_barrel_wood", "PalBuildObject");
    m.insert("enemycamp_basecampitemdispenser", "PalBuildObject");
    m.insert("enemycamp_basecampworkhard", "PalBuildObject");
    m.insert("enemycamp_bathtub_stone", "PalBuildObject");
    m.insert("enemycamp_believer_banner", "PalBuildObject");
    m.insert("enemycamp_believer_flag", "PalBuildObject");
    m.insert("enemycamp_bench_wood", "PalBuildObject");
    m.insert("enemycamp_blastfurnace", "PalBuildObject");
    m.insert("enemycamp_blastfurnace2", "PalBuildObject");
    m.insert("enemycamp_blastfurnace3", "PalBuildObject");
    m.insert("enemycamp_blastfurnace4", "PalBuildObject");
    m.insert("enemycamp_bonsai", "PalBuildObject");
    m.insert("enemycamp_box01_iron", "PalBuildObject");
    m.insert("enemycamp_box01_stone", "PalBuildObject");
    m.insert("enemycamp_box02_iron", "PalBuildObject");
    m.insert("enemycamp_box_wood", "PalBuildObject");
    m.insert("enemycamp_buildablegoddessstatue", "PalBuildObject");
    m.insert("enemycamp_byobu", "PalBuildObject");
    m.insert("enemycamp_cablecoil01_iron", "PalBuildObject");
    m.insert("enemycamp_campfire", "PalBuildObject");
    m.insert("enemycamp_cauldron", "PalBuildObject");
    m.insert("enemycamp_ceilinglamp", "PalBuildObject");
    m.insert("enemycamp_chair01_iron", "PalBuildObject");
    m.insert("enemycamp_chair01_pal", "PalBuildObject");
    m.insert("enemycamp_chair01_stone", "PalBuildObject");
    m.insert("enemycamp_chair01_wood", "PalBuildObject");
    m.insert("enemycamp_chair02_iron", "PalBuildObject");
    m.insert("enemycamp_chair02_stone", "PalBuildObject");
    m.insert("enemycamp_characterrankup", "PalBuildObject");
    m.insert("enemycamp_clock01_stone", "PalBuildObject");
    m.insert("enemycamp_clock01_wall_iron", "PalBuildObject");
    m.insert("enemycamp_compositedesk", "PalBuildObject");
    m.insert("enemycamp_conservationgroupbannera", "PalBuildObject");
    m.insert("enemycamp_conservationgroupbannerb", "PalBuildObject");
    m.insert("enemycamp_container01_iron", "PalBuildObject");
    m.insert("enemycamp_cookingstove", "PalBuildObject");
    m.insert("enemycamp_cooler", "PalBuildObject");
    m.insert("enemycamp_coolerbox", "PalBuildObject");
    m.insert("enemycamp_coolerpalfoodbox", "PalBuildObject");
    m.insert("enemycamp_copperpit", "PalBuildObject");
    m.insert("enemycamp_copperpit_2", "PalBuildObject");
    m.insert("enemycamp_counter_wood", "PalBuildObject");
    m.insert("enemycamp_crusher", "PalBuildObject");
    m.insert("enemycamp_curtain01_wall_stone", "PalBuildObject");
    m.insert("enemycamp_damagedscarecrow", "PalBuildObject");
    m.insert("enemycamp_defensebowgun", "PalBuildObject");
    m.insert("enemycamp_defensemachinegun", "PalBuildObject");
    m.insert("enemycamp_defensemissile", "PalBuildObject");
    m.insert("enemycamp_defensewait", "PalBuildObject");
    m.insert("enemycamp_defensewall", "PalBuildObject");
    m.insert("enemycamp_defensewall_metal", "PalBuildObject");
    m.insert("enemycamp_defensewall_wood", "PalBuildObject");
    m.insert("enemycamp_desk01_iron", "PalBuildObject");
    m.insert("enemycamp_desk01_stone", "PalBuildObject");
    m.insert("enemycamp_dimensionpalstorage", "PalBuildObject");
    m.insert("enemycamp_dismantlingconveyor", "PalBuildObject");
    m.insert("enemycamp_displaycharacter", "PalBuildObject");
    m.insert("enemycamp_electriccooler", "PalBuildObject");
    m.insert("enemycamp_electricgenerator", "PalBuildObject");
    m.insert("enemycamp_electricgenerator_large", "PalBuildObject");
    m.insert("enemycamp_electrichatchingpalegg", "PalBuildObject");
    m.insert("enemycamp_electricheater", "PalBuildObject");
    m.insert("enemycamp_electrickitchen", "PalBuildObject");
    m.insert("enemycamp_energystorage_electric", "PalBuildObject");
    m.insert("enemycamp_factory_hard_01", "PalBuildObject");
    m.insert("enemycamp_factory_hard_02", "PalBuildObject");
    m.insert("enemycamp_factory_hard_03", "PalBuildObject");
    m.insert("enemycamp_factory_money", "PalBuildObject");
    m.insert("enemycamp_farmblockv2_berries", "PalBuildObject");
    m.insert("enemycamp_farmblockv2_carrot", "PalBuildObject");
    m.insert("enemycamp_farmblockv2_lettuce", "PalBuildObject");
    m.insert("enemycamp_farmblockv2_onion", "PalBuildObject");
    m.insert("enemycamp_farmblockv2_potato", "PalBuildObject");
    m.insert("enemycamp_farmblockv2_tomato", "PalBuildObject");
    m.insert("enemycamp_farmblockv2_wheet", "PalBuildObject");
    m.insert("enemycamp_firecult_banner", "PalBuildObject");
    m.insert("enemycamp_firecult_flag", "PalBuildObject");
    m.insert("enemycamp_flourmill", "PalBuildObject");
    m.insert("enemycamp_flowerbed", "PalBuildObject");
    m.insert("enemycamp_fountain", "PalBuildObject");
    m.insert("enemycamp_fudukue", "PalBuildObject");
    m.insert("enemycamp_garbagebag_iron", "PalBuildObject");
    m.insert("enemycamp_glass_fence", "PalBuildObject");
    m.insert("enemycamp_glass_foundation", "PalBuildObject");
    m.insert("enemycamp_glass_pillars", "PalBuildObject");
    m.insert("enemycamp_glass_roof", "PalBuildObject");
    m.insert("enemycamp_glass_slantedroof", "PalBuildObject");
    m.insert("enemycamp_glass_stair", "PalBuildObject");
    m.insert("enemycamp_glass_trianglewall", "PalBuildObject");
    m.insert("enemycamp_glass_wall", "PalBuildObject");
    m.insert("enemycamp_glass_wall_destructable", "PalBuildObject");
    m.insert("enemycamp_glass_windowwall", "PalBuildObject");
    m.insert("enemycamp_globalpalstorage", "PalBuildObject");
    m.insert("enemycamp_globe01_stone", "PalBuildObject");
    m.insert("enemycamp_goalsoccer_iron", "PalBuildObject");
    m.insert("enemycamp_guardiandogstatue", "PalBuildObject");
    m.insert("enemycamp_guildchest", "PalBuildObject");
    m.insert("enemycamp_hatchingpalegg", "PalBuildObject");
    m.insert("enemycamp_headstone", "PalBuildObject");
    m.insert("enemycamp_heater", "PalBuildObject");
    m.insert("enemycamp_hugekitchen", "PalBuildObject");
    m.insert("enemycamp_hunter_banner", "PalBuildObject");
    m.insert("enemycamp_hunter_flag", "PalBuildObject");
    m.insert("enemycamp_hunter_gangflag", "PalBuildObject");
    m.insert("enemycamp_icecrusher", "PalBuildObject");
    m.insert("enemycamp_iron_fence", "PalBuildObject");
    m.insert("enemycamp_irori", "PalBuildObject");
    m.insert("enemycamp_itembooth", "PalBuildObject");
    m.insert("enemycamp_itemchest", "PalBuildObject");
    m.insert("enemycamp_itemchest_02", "PalBuildObject");
    m.insert("enemycamp_itemchest_03", "PalBuildObject");
    m.insert("enemycamp_itemchest_04", "PalBuildObject");
    m.insert("enemycamp_ivy01", "PalBuildObject");
    m.insert("enemycamp_ivy02", "PalBuildObject");
    m.insert("enemycamp_ivy03", "PalBuildObject");
    m.insert("enemycamp_japanesestyle_fence", "PalBuildObject");
    m.insert("enemycamp_japanesestyle_foundation", "PalBuildObject");
    m.insert("enemycamp_japanesestyle_pillar", "PalBuildObject");
    m.insert("enemycamp_japanesestyle_roof_01", "PalBuildObject");
    m.insert("enemycamp_japanesestyle_roof_02", "PalBuildObject");
    m.insert("enemycamp_japanesestyle_slantedroof", "PalBuildObject");
    m.insert("enemycamp_japanesestyle_stair", "PalBuildObject");
    m.insert("enemycamp_japanesestyle_trianglewall", "PalBuildObject");
    m.insert("enemycamp_japanesestyle_wall_01", "PalBuildObject");
    m.insert(
        "enemycamp_japanesestyle_wall_01_destructable",
        "PalBuildObject",
    );
    m.insert("enemycamp_japanesestyle_windowwall", "PalBuildObject");
    m.insert("enemycamp_kakejiku", "PalBuildObject");
    m.insert("enemycamp_koro", "PalBuildObject");
    m.insert("enemycamp_lab", "PalBuildObject");
    m.insert("enemycamp_lamp", "PalBuildObject");
    m.insert("enemycamp_largeceilinglamp", "PalBuildObject");
    m.insert("enemycamp_largelamp", "PalBuildObject");
    m.insert("enemycamp_light_candlesticks_top", "PalBuildObject");
    m.insert("enemycamp_light_candlesticks_wall", "PalBuildObject");
    m.insert("enemycamp_light_fireplace01", "PalBuildObject");
    m.insert("enemycamp_light_fireplace02", "PalBuildObject");
    m.insert("enemycamp_light_floorlamp01", "PalBuildObject");
    m.insert("enemycamp_light_floorlamp02", "PalBuildObject");
    m.insert("enemycamp_light_lightpole01", "PalBuildObject");
    m.insert("enemycamp_light_lightpole02", "PalBuildObject");
    m.insert("enemycamp_light_lightpole03", "PalBuildObject");
    m.insert("enemycamp_light_lightpole04", "PalBuildObject");
    m.insert("enemycamp_lilyqueenstatue", "PalBuildObject");
    m.insert("enemycamp_machinegame01_iron", "PalBuildObject");
    m.insert("enemycamp_machinevending01_iron", "PalBuildObject");
    m.insert("enemycamp_manualelectricgenerator", "PalBuildObject");
    m.insert("enemycamp_medicalpalbed_02", "PalBuildObject");
    m.insert("enemycamp_medicalpalbed_03", "PalBuildObject");
    m.insert("enemycamp_medicalpalbed_04", "PalBuildObject");
    m.insert("enemycamp_medicalpalbed_05", "PalBuildObject");
    m.insert("enemycamp_medicinefacility_01", "PalBuildObject");
    m.insert("enemycamp_medicinefacility_02", "PalBuildObject");
    m.insert("enemycamp_medicinefacility_03", "PalBuildObject");
    m.insert("enemycamp_metal_foundation", "PalBuildObject");
    m.insert("enemycamp_metal_pillars", "PalBuildObject");
    m.insert("enemycamp_metal_roof", "PalBuildObject");
    m.insert("enemycamp_metal_slantedroof", "PalBuildObject");
    m.insert("enemycamp_metal_stair", "PalBuildObject");
    m.insert("enemycamp_metal_trianglewall", "PalBuildObject");
    m.insert("enemycamp_metal_wall", "PalBuildObject");
    m.insert("enemycamp_metal_wall_destructable", "PalBuildObject");
    m.insert("enemycamp_metal_windowwall", "PalBuildObject");
    m.insert("enemycamp_miningtool", "PalBuildObject");
    m.insert("enemycamp_mirror01_stone", "PalBuildObject");
    m.insert("enemycamp_mirror01_wall_stone", "PalBuildObject");
    m.insert("enemycamp_mirror02_stone", "PalBuildObject");
    m.insert("enemycamp_multielectrichatchingpalegg", "PalBuildObject");
    m.insert("enemycamp_ninja_banner", "PalBuildObject");
    m.insert("enemycamp_ninja_flag", "PalBuildObject");
    m.insert("enemycamp_oilpump", "PalBuildObject");
    m.insert("enemycamp_olympiccauldron", "PalBuildObject");
    m.insert("enemycamp_operatingtable", "PalBuildObject");
    m.insert("enemycamp_palbooth", "PalBuildObject");
    m.insert("enemycamp_palcage", "PalBuildObject");
    m.insert("enemycamp_palfoodbox", "PalBuildObject");
    m.insert("enemycamp_palmedicinebox", "PalBuildObject");
    m.insert("enemycamp_partition_stone", "PalBuildObject");
    m.insert("enemycamp_piano01_stone", "PalBuildObject");
    m.insert("enemycamp_piano02_stone", "PalBuildObject");
    m.insert("enemycamp_pipeclay01_iron", "PalBuildObject");
    m.insert("enemycamp_plant01_plant", "PalBuildObject");
    m.insert("enemycamp_plant02_plant", "PalBuildObject");
    m.insert("enemycamp_plant03_plant", "PalBuildObject");
    m.insert("enemycamp_plant04_plant", "PalBuildObject");
    m.insert("enemycamp_playerbed_02", "PalBuildObject");
    m.insert("enemycamp_playerbed_03", "PalBuildObject");
    m.insert("enemycamp_police_banner", "PalBuildObject");
    m.insert("enemycamp_police_flag", "PalBuildObject");
    m.insert("enemycamp_refrigerator", "PalBuildObject");
    m.insert("enemycamp_repairbench", "PalBuildObject");
    m.insert("enemycamp_rug01_stone", "PalBuildObject");
    m.insert("enemycamp_rug02_stone", "PalBuildObject");
    m.insert("enemycamp_rug03_stone", "PalBuildObject");
    m.insert("enemycamp_rug04_stone", "PalBuildObject");
    m.insert("enemycamp_sanitydecrease1", "PalBuildObject");
    m.insert("enemycamp_scientist_banner", "PalBuildObject");
    m.insert("enemycamp_scientist_flag", "PalBuildObject");
    m.insert("enemycamp_seika", "PalBuildObject");
    m.insert("enemycamp_sf_fence", "PalBuildObject");
    m.insert("enemycamp_sf_foundation", "PalBuildObject");
    m.insert("enemycamp_sf_pillars", "PalBuildObject");
    m.insert("enemycamp_sf_roof", "PalBuildObject");
    m.insert("enemycamp_sf_slantedroof", "PalBuildObject");
    m.insert("enemycamp_sf_stair", "PalBuildObject");
    m.insert("enemycamp_sf_trianglewall", "PalBuildObject");
    m.insert("enemycamp_sf_wall", "PalBuildObject");
    m.insert("enemycamp_sf_wall_destructable", "PalBuildObject");
    m.insert("enemycamp_sf_windowwall", "PalBuildObject");
    m.insert("enemycamp_shelf01_iron", "PalBuildObject");
    m.insert("enemycamp_shelf01_stone", "PalBuildObject");
    m.insert("enemycamp_shelf01_wall_iron", "PalBuildObject");
    m.insert("enemycamp_shelf01_wall_stone", "PalBuildObject");
    m.insert("enemycamp_shelf02_iron", "PalBuildObject");
    m.insert("enemycamp_shelf02_stone", "PalBuildObject");
    m.insert("enemycamp_shelf03_iron", "PalBuildObject");
    m.insert("enemycamp_shelf03_stone", "PalBuildObject");
    m.insert("enemycamp_shelf04_iron", "PalBuildObject");
    m.insert("enemycamp_shelf04_stone", "PalBuildObject");
    m.insert("enemycamp_shelf05_stone", "PalBuildObject");
    m.insert("enemycamp_shelf06_stone", "PalBuildObject");
    m.insert("enemycamp_shelf07_stone", "PalBuildObject");
    m.insert("enemycamp_shelf_cask_wood", "PalBuildObject");
    m.insert("enemycamp_shelf_hang01_wood", "PalBuildObject");
    m.insert("enemycamp_shelf_hang02_wood", "PalBuildObject");
    m.insert("enemycamp_shelf_wood", "PalBuildObject");
    m.insert("enemycamp_shishiodoshi", "PalBuildObject");
    m.insert("enemycamp_signexit_ceiling_iron", "PalBuildObject");
    m.insert("enemycamp_signexit_wall_iron", "PalBuildObject");
    m.insert("enemycamp_silo", "PalBuildObject");
    m.insert("enemycamp_skinchange", "PalBuildObject");
    m.insert("enemycamp_snowman", "PalBuildObject");
    m.insert("enemycamp_sofa01_iron", "PalBuildObject");
    m.insert("enemycamp_sofa01_stone", "PalBuildObject");
    m.insert("enemycamp_sofa02_iron", "PalBuildObject");
    m.insert("enemycamp_sofa02_stone", "PalBuildObject");
    m.insert("enemycamp_sofa03_stone", "PalBuildObject");
    m.insert("enemycamp_spa", "PalBuildObject");
    m.insert("enemycamp_spa2", "PalBuildObject");
    m.insert("enemycamp_spherefactory_black_01", "PalBuildObject");
    m.insert("enemycamp_spherefactory_black_02", "PalBuildObject");
    m.insert("enemycamp_spherefactory_black_03", "PalBuildObject");
    m.insert("enemycamp_spherefactory_black_04", "PalBuildObject");
    m.insert("enemycamp_stationdeforest2", "PalBuildObject");
    m.insert("enemycamp_stone_fence", "PalBuildObject");
    m.insert("enemycamp_stone_foundation", "PalBuildObject");
    m.insert("enemycamp_stone_pillar", "PalBuildObject");
    m.insert("enemycamp_stone_roof", "PalBuildObject");
    m.insert("enemycamp_stone_slantedroof", "PalBuildObject");
    m.insert("enemycamp_stone_stair", "PalBuildObject");
    m.insert("enemycamp_stone_trianglewall", "PalBuildObject");
    m.insert("enemycamp_stone_wall", "PalBuildObject");
    m.insert("enemycamp_stone_wall_destructable", "PalBuildObject");
    m.insert("enemycamp_stone_windowwall", "PalBuildObject");
    m.insert("enemycamp_stonepit", "PalBuildObject");
    m.insert("enemycamp_stool01_iron", "PalBuildObject");
    m.insert("enemycamp_stool01_stone", "PalBuildObject");
    m.insert("enemycamp_stool_high_wood", "PalBuildObject");
    m.insert("enemycamp_stool_wood", "PalBuildObject");
    m.insert("enemycamp_stove01_stone", "PalBuildObject");
    m.insert("enemycamp_stump", "PalBuildObject");
    m.insert("enemycamp_tablecircular01_iron", "PalBuildObject");
    m.insert("enemycamp_tablecircular01_stone", "PalBuildObject");
    m.insert("enemycamp_tablecircular_wood", "PalBuildObject");
    m.insert("enemycamp_tabledresser01_stone", "PalBuildObject");
    m.insert("enemycamp_tableside01_iron", "PalBuildObject");
    m.insert("enemycamp_tablesink01_stone", "PalBuildObject");
    m.insert("enemycamp_tablesquare01_iron", "PalBuildObject");
    m.insert("enemycamp_tablesquare02_iron", "PalBuildObject");
    m.insert("enemycamp_tablesquare_wood", "PalBuildObject");
    m.insert("enemycamp_tansu", "PalBuildObject");
    m.insert("enemycamp_television01_iron", "PalBuildObject");
    m.insert("enemycamp_tire01_iron", "PalBuildObject");
    m.insert("enemycamp_toilet01_stone", "PalBuildObject");
    m.insert("enemycamp_toiletholder01_stone", "PalBuildObject");
    m.insert("enemycamp_toolboxv1", "PalBuildObject");
    m.insert("enemycamp_torch", "PalBuildObject");
    m.insert("enemycamp_toro", "PalBuildObject");
    m.insert("enemycamp_towlrack01_stone", "PalBuildObject");
    m.insert("enemycamp_trafficbarricade01_iron", "PalBuildObject");
    m.insert("enemycamp_trafficbarricade02_iron", "PalBuildObject");
    m.insert("enemycamp_trafficbarricade03_iron", "PalBuildObject");
    m.insert("enemycamp_trafficbarricade04_iron", "PalBuildObject");
    m.insert("enemycamp_trafficbarricade05_iron", "PalBuildObject");
    m.insert("enemycamp_trafficcone01_iron", "PalBuildObject");
    m.insert("enemycamp_trafficcone02_iron", "PalBuildObject");
    m.insert("enemycamp_trafficcone03_iron", "PalBuildObject");
    m.insert("enemycamp_trafficsign01_iron", "PalBuildObject");
    m.insert("enemycamp_trafficsign02_iron", "PalBuildObject");
    m.insert("enemycamp_trafficsign03_iron", "PalBuildObject");
    m.insert("enemycamp_trafficsign04_iron", "PalBuildObject");
    m.insert("enemycamp_transmissiontower", "PalBuildObject");
    m.insert("enemycamp_trap_noose", "PalBuildObject");
    m.insert("enemycamp_wallsignboard_no101", "PalBuildObject");
    m.insert("enemycamp_wallsignboard_no102", "PalBuildObject");
    m.insert("enemycamp_wallsignboard_no103", "PalBuildObject");
    m.insert("enemycamp_wallsignboard_no104", "PalBuildObject");
    m.insert("enemycamp_wallsignboard_no105", "PalBuildObject");
    m.insert("enemycamp_wallsignboard_no106", "PalBuildObject");
    m.insert("enemycamp_wallsignboard_no107", "PalBuildObject");
    m.insert("enemycamp_wallsignboard_no108", "PalBuildObject");
    m.insert("enemycamp_wallsignboard_no109", "PalBuildObject");
    m.insert("enemycamp_wallsignboard_no110", "PalBuildObject");
    m.insert("enemycamp_walltorch", "PalBuildObject");
    m.insert("enemycamp_weaponfactory_dirty_01", "PalBuildObject");
    m.insert("enemycamp_weaponfactory_dirty_02", "PalBuildObject");
    m.insert("enemycamp_weaponfactory_dirty_03", "PalBuildObject");
    m.insert("enemycamp_wire_fence", "PalBuildObject");
    m.insert("enemycamp_wood_fence", "PalBuildObject");
    m.insert("enemycamp_wood_slantedroof", "PalBuildObject");
    m.insert("enemycamp_wood_trianglewall", "PalBuildObject");
    m.insert("enemycamp_wood_windowwall", "PalBuildObject");
    m.insert("enemycamp_wooden_foundation", "PalBuildObject");
    m.insert("enemycamp_wooden_ladder", "PalBuildObject");
    m.insert("enemycamp_wooden_pillar", "PalBuildObject");
    m.insert("enemycamp_wooden_roof", "PalBuildObject");
    m.insert("enemycamp_wooden_stair", "PalBuildObject");
    m.insert("enemycamp_wooden_wall", "PalBuildObject");
    m.insert("enemycamp_wooden_wall_destructable", "PalBuildObject");
    m.insert("enemycamp_woodenbarricade", "PalBuildObject");
    m.insert("enemycamp_workbench", "PalBuildObject");
    m.insert("enemycamp_workbench_skillunlock", "PalBuildObject");
    m.insert("enemycamp_workspeedincrease1", "PalBuildObject");
    m.insert("enemycamp_zabuton", "PalBuildObject");
    m.insert("enemycamp_zaisu", "PalBuildObject");
    m.insert("firecult_banner", "PalBuildObject");
    m.insert("firecult_flag", "PalBuildObject");
    m.insert("fudukue", "PalBuildObject");
    m.insert("garbagebag_iron", "PalBuildObject");
    m.insert("glass_fence", "PalBuildObject");
    m.insert("glass_foundation", "PalBuildObject");
    m.insert("glass_pillars", "PalBuildObject");
    m.insert("glass_roof", "PalBuildObject");
    m.insert("glass_slantedroof", "PalBuildObject");
    m.insert("glass_stair", "PalBuildObject");
    m.insert("glass_trianglewall", "PalBuildObject");
    m.insert("glass_wall", "PalBuildObject");
    m.insert("glass_windowwall", "PalBuildObject");
    m.insert("globe01_stone", "PalBuildObject");
    m.insert("goalsoccer_iron", "PalBuildObject");
    m.insert("guardiandogstatue", "PalBuildObject");
    m.insert("hunter_banner", "PalBuildObject");
    m.insert("hunter_flag", "PalBuildObject");
    m.insert("hunter_gangflag", "PalBuildObject");
    m.insert("iron_fence", "PalBuildObject");
    m.insert("irori", "PalBuildObject");
    m.insert("ivy01", "PalBuildObject");
    m.insert("ivy02", "PalBuildObject");
    m.insert("ivy03", "PalBuildObject");
    m.insert("japanesestyle_fence", "PalBuildObject");
    m.insert("japanesestyle_foundation", "PalBuildObject");
    m.insert("japanesestyle_pillar", "PalBuildObject");
    m.insert("japanesestyle_roof_01", "PalBuildObject");
    m.insert("japanesestyle_roof_02", "PalBuildObject");
    m.insert("japanesestyle_slantedroof", "PalBuildObject");
    m.insert("japanesestyle_stair", "PalBuildObject");
    m.insert("japanesestyle_trianglewall", "PalBuildObject");
    m.insert("japanesestyle_wall_01", "PalBuildObject");
    m.insert("japanesestyle_windowwall", "PalBuildObject");
    m.insert("kakejiku", "PalBuildObject");
    m.insert("koro", "PalBuildObject");
    m.insert("lilyqueenstatue", "PalBuildObject");
    m.insert("machinegame01_iron", "PalBuildObject");
    m.insert("machinevending01_iron", "PalBuildObject");
    m.insert("metal_foundation", "PalBuildObject");
    m.insert("metal_pillars", "PalBuildObject");
    m.insert("metal_roof", "PalBuildObject");
    m.insert("metal_slantedroof", "PalBuildObject");
    m.insert("metal_stair", "PalBuildObject");
    m.insert("metal_trianglewall", "PalBuildObject");
    m.insert("metal_wall", "PalBuildObject");
    m.insert("metal_windowwall", "PalBuildObject");
    m.insert("mirror01_stone", "PalBuildObject");
    m.insert("mirror01_wall_stone", "PalBuildObject");
    m.insert("mirror02_stone", "PalBuildObject");
    m.insert("ninja_banner", "PalBuildObject");
    m.insert("ninja_flag", "PalBuildObject");
    m.insert("palcage", "PalBuildObject");
    m.insert("partition_stone", "PalBuildObject");
    m.insert("piano01_stone", "PalBuildObject");
    m.insert("piano02_stone", "PalBuildObject");
    m.insert("pipeclay01_iron", "PalBuildObject");
    m.insert("plant01_plant", "PalBuildObject");
    m.insert("plant02_plant", "PalBuildObject");
    m.insert("plant03_plant", "PalBuildObject");
    m.insert("plant04_plant", "PalBuildObject");
    m.insert("police_banner", "PalBuildObject");
    m.insert("police_flag", "PalBuildObject");
    m.insert("rug01_stone", "PalBuildObject");
    m.insert("rug02_stone", "PalBuildObject");
    m.insert("rug03_stone", "PalBuildObject");
    m.insert("rug04_stone", "PalBuildObject");
    m.insert("rug_wood", "PalBuildObject");
    m.insert("scientist_banner", "PalBuildObject");
    m.insert("scientist_flag", "PalBuildObject");
    m.insert("seika", "PalBuildObject");
    m.insert("sf_desk", "PalBuildObject");
    m.insert("sf_fence", "PalBuildObject");
    m.insert("sf_foundation", "PalBuildObject");
    m.insert("sf_pillars", "PalBuildObject");
    m.insert("sf_roof", "PalBuildObject");
    m.insert("sf_slantedroof", "PalBuildObject");
    m.insert("sf_stair", "PalBuildObject");
    m.insert("sf_trianglewall", "PalBuildObject");
    m.insert("sf_wall", "PalBuildObject");
    m.insert("sf_windowwall", "PalBuildObject");
    m.insert("shelf_hang02_wood", "PalBuildObject");
    m.insert("shishiodoshi", "PalBuildObject");
    m.insert("signexit_ceiling_iron", "PalBuildObject");
    m.insert("signexit_wall_iron", "PalBuildObject");
    m.insert("sofa03_stone", "PalBuildObject");
    m.insert("stone_fence", "PalBuildObject");
    m.insert("stone_foundation", "PalBuildObject");
    m.insert("stone_pillar", "PalBuildObject");
    m.insert("stone_roof", "PalBuildObject");
    m.insert("stone_slantedroof", "PalBuildObject");
    m.insert("stone_stair", "PalBuildObject");
    m.insert("stone_trianglewall", "PalBuildObject");
    m.insert("stone_wall", "PalBuildObject");
    m.insert("stone_windowwall", "PalBuildObject");
    m.insert("stonehouse1", "PalBuildObject");
    m.insert("stove01_stone", "PalBuildObject");
    m.insert("strawhouse1", "PalBuildObject");
    m.insert("table1", "PalBuildObject");
    m.insert("tablecircular01_iron", "PalBuildObject");
    m.insert("tablecircular01_stone", "PalBuildObject");
    m.insert("tablecircular_wood", "PalBuildObject");
    m.insert("tableside01_iron", "PalBuildObject");
    m.insert("tablesink01_stone", "PalBuildObject");
    m.insert("tablesquare01_iron", "PalBuildObject");
    m.insert("tablesquare02_iron", "PalBuildObject");
    m.insert("tablesquare_wood", "PalBuildObject");
    m.insert("television01_iron", "PalBuildObject");
    m.insert("tire01_iron", "PalBuildObject");
    m.insert("toiletholder01_stone", "PalBuildObject");
    m.insert("toro", "PalBuildObject");
    m.insert("towlrack01_stone", "PalBuildObject");
    m.insert("trafficbarricade01_iron", "PalBuildObject");
    m.insert("trafficbarricade02_iron", "PalBuildObject");
    m.insert("trafficbarricade03_iron", "PalBuildObject");
    m.insert("trafficbarricade04_iron", "PalBuildObject");
    m.insert("trafficbarricade05_iron", "PalBuildObject");
    m.insert("trafficcone01_iron", "PalBuildObject");
    m.insert("trafficcone02_iron", "PalBuildObject");
    m.insert("trafficcone03_iron", "PalBuildObject");
    m.insert("trafficlight01_iron", "PalBuildObject");
    m.insert("trafficsign01_iron", "PalBuildObject");
    m.insert("trafficsign02_iron", "PalBuildObject");
    m.insert("trafficsign03_iron", "PalBuildObject");
    m.insert("trafficsign04_iron", "PalBuildObject");
    m.insert("wire_fence", "PalBuildObject");
    m.insert("wood_fence", "PalBuildObject");
    m.insert("wood_slantedroof", "PalBuildObject");
    m.insert("wood_trianglewall", "PalBuildObject");
    m.insert("wood_windowwall", "PalBuildObject");
    m.insert("wooden_foundation", "PalBuildObject");
    m.insert("wooden_ladder", "PalBuildObject");
    m.insert("wooden_pillar", "PalBuildObject");
    m.insert("wooden_roof", "PalBuildObject");
    m.insert("wooden_stair", "PalBuildObject");
    m.insert("wooden_wall", "PalBuildObject");
    m.insert("woodenbarricade", "PalBuildObject");
    m.insert("woodhouse1", "PalBuildObject");
    m
});

static PAL_BUILD_OBJECT_BREED_FARM: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("enemycamp_breedfarm", "PalBuildObjectBreedFarm");
        m
    });

static PAL_BUILD_OBJECT_CONVERT_CHARACTER_TO_ITEM: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert(
            "dismantlingconveyor",
            "PalBuildObjectConvertCharacterToItem",
        );
        m
    });

static PAL_BUILD_OBJECT_MONSTER_FARM: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("enemycamp_monsterfarm", "PalBuildObjectMonsterFarm");
        m
    });

static PAL_BUILD_OBJECT_RAID_BOSS_SUMMON: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("altar", "PalBuildObjectRaidBossSummon");
        m.insert("enemycamp_altar", "PalBuildObjectRaidBossSummon");
        m
    });

static PAL_MAP_OBJECT_AMUSEMENT_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("spa", "PalMapObjectAmusementModel");
        m.insert("spa2", "PalMapObjectAmusementModel");
        m.insert("spa3", "PalMapObjectAmusementModel");
        m
    });

static PAL_MAP_OBJECT_BASE_CAMP_ITEM_DISPENSER_MODEL: LazyLock<
    HashMap<&'static str, &'static str>,
> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert(
        "basecampitemdispenser",
        "PalMapObjectBaseCampItemDispenserModel",
    );
    m
});

static PAL_MAP_OBJECT_BASE_CAMP_PASSIVE_EFFECT_MODEL: LazyLock<
    HashMap<&'static str, &'static str>,
> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("cauldron", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert("flowerbed", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert("fountain", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert("miningtool", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert("olympiccauldron", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert("sanitydecrease1", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert("silo", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert("snowman", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert("stump", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert("toolboxv1", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert("toolboxv2", "PalMapObjectBaseCampPassiveEffectModel");
    m.insert(
        "transmissiontower",
        "PalMapObjectBaseCampPassiveEffectModel",
    );
    m.insert(
        "workspeedincrease1",
        "PalMapObjectBaseCampPassiveEffectModel",
    );
    m
});

static PAL_MAP_OBJECT_BASE_CAMP_PASSIVE_WORK_HARD_MODEL: LazyLock<
    HashMap<&'static str, &'static str>,
> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert(
        "basecampworkhard",
        "PalMapObjectBaseCampPassiveWorkHardModel",
    );
    m
});

static PAL_MAP_OBJECT_BASE_CAMP_POINT: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("palboxv2", "PalMapObjectBaseCampPoint");
        m
    });

static PAL_MAP_OBJECT_BASE_CAMP_WORKER_DIRECTOR_MODEL: LazyLock<
    HashMap<&'static str, &'static str>,
> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert(
        "basecampbattledirector",
        "PalMapObjectBaseCampWorkerDirectorModel",
    );
    m.insert(
        "enemycamp_basecampbattledirector",
        "PalMapObjectBaseCampWorkerDirectorModel",
    );
    m
});

static PAL_MAP_OBJECT_BASE_CAMP_WORKER_EXTRA_STATION_MODEL: LazyLock<
    HashMap<&'static str, &'static str>,
> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert(
        "basecampworkerextrastation",
        "PalMapObjectBaseCampWorkerExtraStationModel",
    );
    m
});

static PAL_MAP_OBJECT_BREED_FARM_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("breedfarm", "PalMapObjectBreedFarmModel");
        m
    });

static PAL_MAP_OBJECT_CHARACTER_MAKE_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("tabledresser01_stone", "PalMapObjectCharacterMakeModel");
        m
    });

static PAL_MAP_OBJECT_CHARACTER_STATUS_OPERATOR_MODEL: LazyLock<
    HashMap<&'static str, &'static str>,
> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert(
        "buildablegoddessstatue",
        "PalMapObjectCharacterStatusOperatorModel",
    );
    m
});

static PAL_MAP_OBJECT_CHARACTER_TEAM_MISSION_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("expedition", "PalMapObjectCharacterTeamMissionModel");
        m
    });

static PAL_MAP_OBJECT_CONVERT_ITEM_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("blastfurnace", "PalMapObjectConvertItemModel");
        m.insert("blastfurnace2", "PalMapObjectConvertItemModel");
        m.insert("blastfurnace3", "PalMapObjectConvertItemModel");
        m.insert("blastfurnace4", "PalMapObjectConvertItemModel");
        m.insert("blastfurnace5", "PalMapObjectConvertItemModel");
        m.insert("campfire", "PalMapObjectConvertItemModel");
        m.insert("compositedesk", "PalMapObjectConvertItemModel");
        m.insert("cookingstove", "PalMapObjectConvertItemModel");
        m.insert("crusher", "PalMapObjectConvertItemModel");
        m.insert("electrickitchen", "PalMapObjectConvertItemModel");
        m.insert("factory_comfortable_01", "PalMapObjectConvertItemModel");
        m.insert("factory_comfortable_02", "PalMapObjectConvertItemModel");
        m.insert("factory_hard_01", "PalMapObjectConvertItemModel");
        m.insert("factory_hard_02", "PalMapObjectConvertItemModel");
        m.insert("factory_hard_03", "PalMapObjectConvertItemModel");
        m.insert("factory_hard_04", "PalMapObjectConvertItemModel");
        m.insert("factory_money", "PalMapObjectConvertItemModel");
        m.insert("flourmill", "PalMapObjectConvertItemModel");
        m.insert("hightechkitchen", "PalMapObjectConvertItemModel");
        m.insert("hugekitchen", "PalMapObjectConvertItemModel");
        m.insert("icecrusher", "PalMapObjectConvertItemModel");
        m.insert("medicinefacility_01", "PalMapObjectConvertItemModel");
        m.insert("medicinefacility_02", "PalMapObjectConvertItemModel");
        m.insert("medicinefacility_03", "PalMapObjectConvertItemModel");
        m.insert("spherefactory_black_01", "PalMapObjectConvertItemModel");
        m.insert("spherefactory_black_02", "PalMapObjectConvertItemModel");
        m.insert("spherefactory_black_03", "PalMapObjectConvertItemModel");
        m.insert("spherefactory_black_04", "PalMapObjectConvertItemModel");
        m.insert("spherefactory_white_01", "PalMapObjectConvertItemModel");
        m.insert("spherefactory_white_02", "PalMapObjectConvertItemModel");
        m.insert("spherefactory_white_03", "PalMapObjectConvertItemModel");
        m.insert("weaponfactory_clean_01", "PalMapObjectConvertItemModel");
        m.insert("weaponfactory_clean_02", "PalMapObjectConvertItemModel");
        m.insert("weaponfactory_clean_03", "PalMapObjectConvertItemModel");
        m.insert("weaponfactory_dirty_01", "PalMapObjectConvertItemModel");
        m.insert("weaponfactory_dirty_02", "PalMapObjectConvertItemModel");
        m.insert("weaponfactory_dirty_03", "PalMapObjectConvertItemModel");
        m.insert("weaponfactory_dirty_04", "PalMapObjectConvertItemModel");
        m.insert("woodcrusher", "PalMapObjectConvertItemModel");
        m.insert("workbench", "PalMapObjectConvertItemModel");
        m.insert("workbench_skillcard", "PalMapObjectConvertItemModel");
        m.insert("workbench_skillunlock", "PalMapObjectConvertItemModel");
        m
    });

static PAL_MAP_OBJECT_DAMAGED_SCARECROW_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("damagedscarecrow", "PalMapObjectDamagedScarecrowModel");
        m
    });

static PAL_MAP_OBJECT_DEATH_DROPPED_CHARACTER_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("droppedcharacter", "PalMapObjectDeathDroppedCharacterModel");
        m
    });

static PAL_MAP_OBJECT_DEATH_PENALTY_STORAGE_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("deathpenaltychest", "PalMapObjectDeathPenaltyStorageModel");
        m
    });

static PAL_MAP_OBJECT_DEFENSE_BULLET_LAUNCHER_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("defensebowgun", "PalMapObjectDefenseBulletLauncherModel");
        m.insert(
            "defensegatlinggun",
            "PalMapObjectDefenseBulletLauncherModel",
        );
        m.insert(
            "defensemachinegun",
            "PalMapObjectDefenseBulletLauncherModel",
        );
        m.insert("defensemissile", "PalMapObjectDefenseBulletLauncherModel");
        m
    });

static PAL_MAP_OBJECT_DEFENSE_WAIT_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("defensewait", "PalMapObjectDefenseWaitModel");
        m
    });

static PAL_MAP_OBJECT_DIMENSION_PAL_STORAGE_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert(
            "dimensionpalstorage",
            "PalMapObjectDimensionPalStorageModel",
        );
        m
    });

static PAL_MAP_OBJECT_DISPLAY_CHARACTER_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("displaycharacter", "PalMapObjectDisplayCharacterModel");
        m
    });

static PAL_MAP_OBJECT_DOOR_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("enemycamp_glass_doorwall", "PalMapObjectDoorModel");
        m.insert(
            "enemycamp_japanesestyle_doorwall_01",
            "PalMapObjectDoorModel",
        );
        m.insert(
            "enemycamp_japanesestyle_doorwall_02",
            "PalMapObjectDoorModel",
        );
        m.insert(
            "enemycamp_japanesestyle_doorwall_03",
            "PalMapObjectDoorModel",
        );
        m.insert("enemycamp_metal_doorwall", "PalMapObjectDoorModel");
        m.insert("enemycamp_metal_gate", "PalMapObjectDoorModel");
        m.insert("enemycamp_sf_doorwall", "PalMapObjectDoorModel");
        m.insert("enemycamp_stone_doorwall", "PalMapObjectDoorModel");
        m.insert("enemycamp_stone_gate", "PalMapObjectDoorModel");
        m.insert("enemycamp_wood_gate", "PalMapObjectDoorModel");
        m.insert("enemycamp_wooden_doorwall", "PalMapObjectDoorModel");
        m.insert("glass_doorwall", "PalMapObjectDoorModel");
        m.insert("japanesestyle_doorwall_01", "PalMapObjectDoorModel");
        m.insert("japanesestyle_doorwall_02", "PalMapObjectDoorModel");
        m.insert("japanesestyle_doorwall_03", "PalMapObjectDoorModel");
        m.insert("metal_doorwall", "PalMapObjectDoorModel");
        m.insert("metal_gate", "PalMapObjectDoorModel");
        m.insert("sf_doorwall", "PalMapObjectDoorModel");
        m.insert("stone_doorwall", "PalMapObjectDoorModel");
        m.insert("stone_gate", "PalMapObjectDoorModel");
        m.insert("wood_gate", "PalMapObjectDoorModel");
        m.insert("wooden_doorwall", "PalMapObjectDoorModel");
        m
    });

static PAL_MAP_OBJECT_DROP_ITEM_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("commondropitem3d", "PalMapObjectDropItemModel");
        m.insert("commondropitem3d_sk", "PalMapObjectDropItemModel");
        m
    });

static PAL_MAP_OBJECT_ENERGY_STORAGE_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("energystorage_electric", "PalMapObjectEnergyStorageModel");
        m
    });

static PAL_MAP_OBJECT_FARM_BLOCK_V2_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("farmblockv2_berries", "PalMapObjectFarmBlockV2Model");
        m.insert("farmblockv2_carrot", "PalMapObjectFarmBlockV2Model");
        m.insert("farmblockv2_grade01", "PalMapObjectFarmBlockV2Model");
        m.insert("farmblockv2_grade02", "PalMapObjectFarmBlockV2Model");
        m.insert("farmblockv2_grade03", "PalMapObjectFarmBlockV2Model");
        m.insert("farmblockv2_lettuce", "PalMapObjectFarmBlockV2Model");
        m.insert("farmblockv2_onion", "PalMapObjectFarmBlockV2Model");
        m.insert("farmblockv2_potato", "PalMapObjectFarmBlockV2Model");
        m.insert("farmblockv2_tomato", "PalMapObjectFarmBlockV2Model");
        m.insert("farmblockv2_wheet", "PalMapObjectFarmBlockV2Model");
        m
    });

static PAL_MAP_OBJECT_FARM_SKILL_FRUITS_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("farm_skillfruits", "PalMapObjectFarmSkillFruitsModel");
        m
    });

static PAL_MAP_OBJECT_FAST_TRAVEL_POINT_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("fasttravelpoint", "PalMapObjectFastTravelPointModel");
        m
    });

static PAL_MAP_OBJECT_FISH_POND_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("fishingpond1", "PalMapObjectFishPondModel");
        m.insert("fishingpond2", "PalMapObjectFishPondModel");
        m
    });

static PAL_MAP_OBJECT_GENERATE_ENERGY_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("electricgenerator", "PalMapObjectGenerateEnergyModel");
        m.insert("electricgenerator2", "PalMapObjectGenerateEnergyModel");
        m.insert("electricgenerator3", "PalMapObjectGenerateEnergyModel");
        m.insert("electricgenerator_large", "PalMapObjectGenerateEnergyModel");
        m.insert("electricgenerator_slave", "PalMapObjectGenerateEnergyModel");
        m.insert("manualelectricgenerator", "PalMapObjectGenerateEnergyModel");
        m
    });

static PAL_MAP_OBJECT_GLOBAL_PAL_STORAGE_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("globalpalstorage", "PalMapObjectGlobalPalStorageModel");
        m
    });

static PAL_MAP_OBJECT_GUILD_CHEST_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("guildchest", "PalMapObjectGuildChestModel");
        m
    });

static PAL_MAP_OBJECT_HATCHING_EGG_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("electrichatchingpalegg", "PalMapObjectHatchingEggModel");
        m.insert("hatchingpalegg", "PalMapObjectHatchingEggModel");
        m
    });

static PAL_MAP_OBJECT_HEAT_SOURCE_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("cooler", "PalMapObjectHeatSourceModel");
        m.insert("electriccooler", "PalMapObjectHeatSourceModel");
        m.insert("electricheater", "PalMapObjectHeatSourceModel");
        m.insert("heater", "PalMapObjectHeatSourceModel");
        m
    });

static PAL_MAP_OBJECT_INSTANT_EFFECT_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("yakushima_healheart", "PalMapObjectInstantEffectModel");
        m
    });

static PAL_MAP_OBJECT_ITEM_BOOTH_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("itembooth", "PalMapObjectItemBoothModel");
        m
    });

static PAL_MAP_OBJECT_ITEM_CHEST_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("barrel_wood", "PalMapObjectItemChestModel");
        m.insert("box01_iron", "PalMapObjectItemChestModel");
        m.insert("box02_iron", "PalMapObjectItemChestModel");
        m.insert("box_wood", "PalMapObjectItemChestModel");
        m.insert("container01_iron", "PalMapObjectItemChestModel");
        m.insert("dev_itemchest", "PalMapObjectItemChestModel");
        m.insert("itemchest", "PalMapObjectItemChestModel");
        m.insert("itemchest_02", "PalMapObjectItemChestModel");
        m.insert("itemchest_03", "PalMapObjectItemChestModel");
        m.insert("itemchest_04", "PalMapObjectItemChestModel");
        m.insert("shelf01_iron", "PalMapObjectItemChestModel");
        m.insert("shelf01_stone", "PalMapObjectItemChestModel");
        m.insert("shelf01_wall_iron", "PalMapObjectItemChestModel");
        m.insert("shelf01_wall_stone", "PalMapObjectItemChestModel");
        m.insert("shelf02_iron", "PalMapObjectItemChestModel");
        m.insert("shelf02_stone", "PalMapObjectItemChestModel");
        m.insert("shelf03_iron", "PalMapObjectItemChestModel");
        m.insert("shelf03_stone", "PalMapObjectItemChestModel");
        m.insert("shelf04_iron", "PalMapObjectItemChestModel");
        m.insert("shelf04_stone", "PalMapObjectItemChestModel");
        m.insert("shelf05_stone", "PalMapObjectItemChestModel");
        m.insert("shelf06_stone", "PalMapObjectItemChestModel");
        m.insert("shelf07_stone", "PalMapObjectItemChestModel");
        m.insert("shelf_cask_wood", "PalMapObjectItemChestModel");
        m.insert("shelf_hang01_wood", "PalMapObjectItemChestModel");
        m.insert("shelf_wood", "PalMapObjectItemChestModel");
        m.insert("tansu", "PalMapObjectItemChestModel");
        m
    });

static PAL_MAP_OBJECT_ITEM_CHEST_AFFECT_CORRUPTION: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("coolerbox", "PalMapObjectItemChest_AffectCorruption");
        m.insert("refrigerator", "PalMapObjectItemChest_AffectCorruption");
        m
    });

static PAL_MAP_OBJECT_ITEM_DROP_ON_DAMAG_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("damagablerock0001", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0002", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0003", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0004", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0005", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0006", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0007", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0008", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0009", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0010", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0011", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0012", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0013", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0014", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0015", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0016", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0017", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0018", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock0019", "PalMapObjectItemDropOnDamagModel");
        m.insert("damagablerock_pv", "PalMapObjectItemDropOnDamagModel");
        m.insert(
            "damagabletree_yakushima001",
            "PalMapObjectItemDropOnDamagModel",
        );
        m.insert(
            "damagabletree_yakushima002",
            "PalMapObjectItemDropOnDamagModel",
        );
        m.insert(
            "damagabletree_yakushima003",
            "PalMapObjectItemDropOnDamagModel",
        );
        m.insert("destroyablewall_rock01", "PalMapObjectItemDropOnDamagModel");
        m.insert("destroyablewall_rock02", "PalMapObjectItemDropOnDamagModel");
        m.insert("meteordrop_damagable", "PalMapObjectItemDropOnDamagModel");
        m.insert("yakushima_crystal", "PalMapObjectItemDropOnDamagModel");
        m.insert("yakushima_pot", "PalMapObjectItemDropOnDamagModel");
        m
    });

static PAL_MAP_OBJECT_LAB_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("lab", "PalMapObjectLabModel");
        m
    });

static PAL_MAP_OBJECT_LAMP_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("ceilinglamp", "PalMapObjectLampModel");
        m.insert("enemycamp_lanterntop", "PalMapObjectLampModel");
        m.insert("enemycamp_shrine_lantern", "PalMapObjectLampModel");
        m.insert("lamp", "PalMapObjectLampModel");
        m.insert("lanterntop", "PalMapObjectLampModel");
        m.insert("largeceilinglamp", "PalMapObjectLampModel");
        m.insert("largelamp", "PalMapObjectLampModel");
        m.insert("light_candlesticks_top", "PalMapObjectLampModel");
        m.insert("light_candlesticks_wall", "PalMapObjectLampModel");
        m.insert("light_floorlamp01", "PalMapObjectLampModel");
        m.insert("light_floorlamp02", "PalMapObjectLampModel");
        m.insert("light_lightpole01", "PalMapObjectLampModel");
        m.insert("light_lightpole02", "PalMapObjectLampModel");
        m.insert("light_lightpole03", "PalMapObjectLampModel");
        m.insert("light_lightpole04", "PalMapObjectLampModel");
        m.insert("shrine_lantern", "PalMapObjectLampModel");
        m
    });

static PAL_MAP_OBJECT_MEDICAL_PAL_BED_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("medicalpalbed", "PalMapObjectMedicalPalBedModel");
        m.insert("medicalpalbed_02", "PalMapObjectMedicalPalBedModel");
        m.insert("medicalpalbed_03", "PalMapObjectMedicalPalBedModel");
        m.insert("medicalpalbed_04", "PalMapObjectMedicalPalBedModel");
        m.insert("medicalpalbed_05", "PalMapObjectMedicalPalBedModel");
        m
    });

static PAL_MAP_OBJECT_MONSTER_FARM_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("monsterfarm", "PalMapObjectMonsterFarmModel");
        m
    });

static PAL_MAP_OBJECT_MULTI_HATCHING_EGG_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert(
            "multielectrichatchingpalegg",
            "PalMapObjectMultiHatchingEggModel",
        );
        m.insert("multihatchingpalegg", "PalMapObjectMultiHatchingEggModel");
        m
    });

static PAL_MAP_OBJECT_OPERATING_TABLE_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("operatingtable", "PalMapObjectOperatingTableModel");
        m
    });

static PAL_MAP_OBJECT_PAL_BOOTH_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("palbooth", "PalMapObjectPalBoothModel");
        m
    });

static PAL_MAP_OBJECT_PAL_EGG_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("palegg", "PalMapObjectPalEggModel");
        m.insert("palegg_dark", "PalMapObjectPalEggModel");
        m.insert("palegg_dragon", "PalMapObjectPalEggModel");
        m.insert("palegg_earth", "PalMapObjectPalEggModel");
        m.insert("palegg_electricity", "PalMapObjectPalEggModel");
        m.insert("palegg_fire", "PalMapObjectPalEggModel");
        m.insert("palegg_ice", "PalMapObjectPalEggModel");
        m.insert("palegg_leaf", "PalMapObjectPalEggModel");
        m.insert("palegg_water", "PalMapObjectPalEggModel");
        m
    });

static PAL_MAP_OBJECT_PAL_FOOD_BOX_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("coolerpalfoodbox", "PalMapObjectPalFoodBoxModel");
        m.insert("palfoodbox", "PalMapObjectPalFoodBoxModel");
        m
    });

static PAL_MAP_OBJECT_PAL_MEDICINE_BOX_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("palmedicinebox", "PalMapObjectPalMedicineBoxModel");
        m
    });

static PAL_MAP_OBJECT_PICKUP_ITEM_ON_LEVEL_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("meteordrop_pickup", "PalMapObjectPickupItemOnLevelModel");
        m.insert(
            "pickupitem_affectionfruit",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_cavemushroom",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert("pickupitem_dogcoin", "PalMapObjectPickupItemOnLevelModel");
        m.insert("pickupitem_flint", "PalMapObjectPickupItemOnLevelModel");
        m.insert("pickupitem_log", "PalMapObjectPickupItemOnLevelModel");
        m.insert(
            "pickupitem_lotus_attack_01",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_lotus_attack_02",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_lotus_hp_01",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_lotus_hp_02",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_lotus_stamina_01",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_lotus_stamina_02",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_lotus_weight_01",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_lotus_weight_02",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_lotus_workspeed_01",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_lotus_workspeed_02",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert("pickupitem_mushroom", "PalMapObjectPickupItemOnLevelModel");
        m.insert(
            "pickupitem_nightstone",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert("pickupitem_poppy", "PalMapObjectPickupItemOnLevelModel");
        m.insert("pickupitem_potato", "PalMapObjectPickupItemOnLevelModel");
        m.insert("pickupitem_redberry", "PalMapObjectPickupItemOnLevelModel");
        m.insert("pickupitem_stone", "PalMapObjectPickupItemOnLevelModel");
        m.insert(
            "pickupitem_yakushimamushroom_01",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_yakushimamushroom_02",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "pickupitem_yakushimamushroom_03",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert("skillfruit_test", "PalMapObjectPickupItemOnLevelModel");
        m.insert(
            "treasurebox_visiblecontent",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m.insert(
            "treasurebox_visiblecontent_skillfruits",
            "PalMapObjectPickupItemOnLevelModel",
        );
        m
    });

static PAL_MAP_OBJECT_PLAYER_BED_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("playerbed", "PalMapObjectPlayerBedModel");
        m.insert("playerbed_02", "PalMapObjectPlayerBedModel");
        m.insert("playerbed_03", "PalMapObjectPlayerBedModel");
        m
    });

static PAL_MAP_OBJECT_PLAYER_SIT_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("bench_wood", "PalMapObjectPlayerSitModel");
        m.insert("chair01_iron", "PalMapObjectPlayerSitModel");
        m.insert("chair01_pal", "PalMapObjectPlayerSitModel");
        m.insert("chair01_stone", "PalMapObjectPlayerSitModel");
        m.insert("chair01_wood", "PalMapObjectPlayerSitModel");
        m.insert("chair02_iron", "PalMapObjectPlayerSitModel");
        m.insert("chair02_stone", "PalMapObjectPlayerSitModel");
        m.insert("sf_chair", "PalMapObjectPlayerSitModel");
        m.insert("sofa01_iron", "PalMapObjectPlayerSitModel");
        m.insert("sofa01_stone", "PalMapObjectPlayerSitModel");
        m.insert("sofa02_iron", "PalMapObjectPlayerSitModel");
        m.insert("sofa02_stone", "PalMapObjectPlayerSitModel");
        m.insert("stool01_iron", "PalMapObjectPlayerSitModel");
        m.insert("stool01_stone", "PalMapObjectPlayerSitModel");
        m.insert("stool_high_wood", "PalMapObjectPlayerSitModel");
        m.insert("stool_wood", "PalMapObjectPlayerSitModel");
        m.insert("toilet01_stone", "PalMapObjectPlayerSitModel");
        m.insert("zabuton", "PalMapObjectPlayerSitModel");
        m.insert("zaisu", "PalMapObjectPlayerSitModel");
        m
    });

static PAL_MAP_OBJECT_PRODUCT_ITEM_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("coalpit", "PalMapObjectProductItemModel");
        m.insert("copperpit", "PalMapObjectProductItemModel");
        m.insert("copperpit_2", "PalMapObjectProductItemModel");
        m.insert("crystalpit", "PalMapObjectProductItemModel");
        m.insert("oilpump", "PalMapObjectProductItemModel");
        m.insert("quartzpit", "PalMapObjectProductItemModel");
        m.insert("stationdeforest2", "PalMapObjectProductItemModel");
        m.insert("stonepit", "PalMapObjectProductItemModel");
        m.insert("sulfurpit", "PalMapObjectProductItemModel");
        m.insert("well", "PalMapObjectProductItemModel");
        m.insert("woodcreator", "PalMapObjectProductItemModel");
        m
    });

static PAL_MAP_OBJECT_RANK_UP_CHARACTER_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("characterrankup", "PalMapObjectRankUpCharacterModel");
        m
    });

static PAL_MAP_OBJECT_RECOVER_OTOMO_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("recoverotomo", "PalMapObjectRecoverOtomoModel");
        m
    });

static PAL_MAP_OBJECT_REPAIR_ITEM_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("repairbench", "PalMapObjectRepairItemModel");
        m
    });

static PAL_MAP_OBJECT_SHIPPING_ITEM_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("shippingitembox", "PalMapObjectShippingItemModel");
        m
    });

static PAL_MAP_OBJECT_SIGNBOARD_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("headstone", "PalMapObjectSignboardModel");
        m.insert("signboard", "PalMapObjectSignboardModel");
        m.insert("wallsignboard", "PalMapObjectSignboardModel");
        m
    });

static PAL_MAP_OBJECT_SKIN_CHANGE_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("skinchange", "PalMapObjectSkinChangeModel");
        m
    });

static PAL_MAP_OBJECT_SUPPLY_STORAGE_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("supplydrop", "PalMapObjectSupplyStorageModel");
        m
    });

static PAL_MAP_OBJECT_TORCH_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("candlestand", "PalMapObjectTorchModel");
        m.insert("enemycamp_candlestand", "PalMapObjectTorchModel");
        m.insert("enemycamp_firestand", "PalMapObjectTorchModel");
        m.insert("enemycamp_walltorch02", "PalMapObjectTorchModel");
        m.insert("firestand", "PalMapObjectTorchModel");
        m.insert("light_fireplace01", "PalMapObjectTorchModel");
        m.insert("light_fireplace02", "PalMapObjectTorchModel");
        m.insert("torch", "PalMapObjectTorchModel");
        m.insert("walltorch", "PalMapObjectTorchModel");
        m.insert("walltorch02", "PalMapObjectTorchModel");
        m
    });

static PAL_MAP_OBJECT_TREASURE_BOX_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("treasurebox", "PalMapObjectTreasureBoxModel");
        m.insert("treasurebox_electric", "PalMapObjectTreasureBoxModel");
        m.insert("treasurebox_enemycamp", "PalMapObjectTreasureBoxModel");
        m.insert("treasurebox_enemycampgoal", "PalMapObjectTreasureBoxModel");
        m.insert("treasurebox_fire", "PalMapObjectTreasureBoxModel");
        m.insert(
            "treasurebox_fishingjunk_requiredlonghold",
            "PalMapObjectTreasureBoxModel",
        );
        m.insert(
            "treasurebox_fishingjunk_requiredlonghold2",
            "PalMapObjectTreasureBoxModel",
        );
        m.insert("treasurebox_oilrig", "PalMapObjectTreasureBoxModel");
        m.insert(
            "treasurebox_requiredlonghold",
            "PalMapObjectTreasureBoxModel",
        );
        m.insert("treasurebox_water", "PalMapObjectTreasureBoxModel");
        m.insert("treasurebox_yakushima", "PalMapObjectTreasureBoxModel");
        m
    });

static MAP_OBJECT_TO_CONCRETE_MODEL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.extend(
            DEFAULT_UNKNOWN_PAL_MAP_OBJECT_CONCRETE_MODEL_BASE
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(PAL_BUILD_OBJECT.iter().map(|(k, v)| (*k, *v)));
        m.extend(PAL_BUILD_OBJECT_BREED_FARM.iter().map(|(k, v)| (*k, *v)));
        m.extend(
            PAL_BUILD_OBJECT_CONVERT_CHARACTER_TO_ITEM
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(PAL_BUILD_OBJECT_MONSTER_FARM.iter().map(|(k, v)| (*k, *v)));
        m.extend(
            PAL_BUILD_OBJECT_RAID_BOSS_SUMMON
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(PAL_MAP_OBJECT_AMUSEMENT_MODEL.iter().map(|(k, v)| (*k, *v)));
        m.extend(
            PAL_MAP_OBJECT_BASE_CAMP_ITEM_DISPENSER_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_BASE_CAMP_PASSIVE_EFFECT_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_BASE_CAMP_PASSIVE_WORK_HARD_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(PAL_MAP_OBJECT_BASE_CAMP_POINT.iter().map(|(k, v)| (*k, *v)));
        m.extend(
            PAL_MAP_OBJECT_BASE_CAMP_WORKER_DIRECTOR_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_BASE_CAMP_WORKER_EXTRA_STATION_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_BREED_FARM_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_CHARACTER_MAKE_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_CHARACTER_STATUS_OPERATOR_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_CHARACTER_TEAM_MISSION_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_CONVERT_ITEM_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_DAMAGED_SCARECROW_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_DEATH_DROPPED_CHARACTER_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_DEATH_PENALTY_STORAGE_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_DEFENSE_BULLET_LAUNCHER_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_DEFENSE_WAIT_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_DIMENSION_PAL_STORAGE_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_DISPLAY_CHARACTER_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(PAL_MAP_OBJECT_DOOR_MODEL.iter().map(|(k, v)| (*k, *v)));
        m.extend(PAL_MAP_OBJECT_DROP_ITEM_MODEL.iter().map(|(k, v)| (*k, *v)));
        m.extend(
            PAL_MAP_OBJECT_ENERGY_STORAGE_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_FARM_BLOCK_V2_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_FARM_SKILL_FRUITS_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_FAST_TRAVEL_POINT_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(PAL_MAP_OBJECT_FISH_POND_MODEL.iter().map(|(k, v)| (*k, *v)));
        m.extend(
            PAL_MAP_OBJECT_GENERATE_ENERGY_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_GLOBAL_PAL_STORAGE_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_GUILD_CHEST_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_HATCHING_EGG_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_HEAT_SOURCE_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_INSTANT_EFFECT_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_ITEM_BOOTH_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_ITEM_CHEST_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_ITEM_CHEST_AFFECT_CORRUPTION
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_ITEM_DROP_ON_DAMAG_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(PAL_MAP_OBJECT_LAB_MODEL.iter().map(|(k, v)| (*k, *v)));
        m.extend(PAL_MAP_OBJECT_LAMP_MODEL.iter().map(|(k, v)| (*k, *v)));
        m.extend(
            PAL_MAP_OBJECT_MEDICAL_PAL_BED_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_MONSTER_FARM_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_MULTI_HATCHING_EGG_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_OPERATING_TABLE_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(PAL_MAP_OBJECT_PAL_BOOTH_MODEL.iter().map(|(k, v)| (*k, *v)));
        m.extend(PAL_MAP_OBJECT_PAL_EGG_MODEL.iter().map(|(k, v)| (*k, *v)));
        m.extend(
            PAL_MAP_OBJECT_PAL_FOOD_BOX_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_PAL_MEDICINE_BOX_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_PICKUP_ITEM_ON_LEVEL_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_PLAYER_BED_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_PLAYER_SIT_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_PRODUCT_ITEM_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_RANK_UP_CHARACTER_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_RECOVER_OTOMO_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_REPAIR_ITEM_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_SHIPPING_ITEM_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(PAL_MAP_OBJECT_SIGNBOARD_MODEL.iter().map(|(k, v)| (*k, *v)));
        m.extend(
            PAL_MAP_OBJECT_SKIN_CHANGE_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(
            PAL_MAP_OBJECT_SUPPLY_STORAGE_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m.extend(PAL_MAP_OBJECT_TORCH_MODEL.iter().map(|(k, v)| (*k, *v)));
        m.extend(
            PAL_MAP_OBJECT_TREASURE_BOX_MODEL
                .iter()
                .map(|(k, v)| (*k, *v)),
        );
        m
    });

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize, T::SoftObjectPath: Serialize",
    deserialize = ""
))]
pub enum PalMapConcreteModelVariant<T: ArchiveType = SaveGameArchiveType> {
    CharacterTeamMission(PalMapObjectCharacterTeamMissionModel),
    FarmSkillFruits(PalMapObjectFarmSkillFruitsModel),
    SupplyStorage(PalMapObjectSupplyStorageModel),
    ItemBooth(PalMapObjectItemBoothModel),
    EnergyStorage(PalMapObjectEnergyStorageModel),
    DeathDroppedCharacter(PalMapObjectDeathDroppedCharacterModel),
    ConvertItem(PalMapObjectConvertItemModel),
    PickupItemOnLevel(PalMapObjectPickupItemOnLevelModel),
    DropItem(PalMapObjectDropItemModel),
    ItemDropOnDamag(PalMapObjectItemDropOnDamagModel),
    DeathPenaltyStorage(PalMapObjectDeathPenaltyStorageModel),
    DefenseBulletLauncher(PalMapObjectDefenseBulletLauncherModel),
    GenerateEnergy(PalMapObjectGenerateEnergyModel),
    FarmBlockV2(PalMapObjectFarmBlockV2Model),
    FastTravelPoint(PalMapObjectFastTravelPointModel),
    ShippingItem(PalMapObjectShippingItemModel),
    ProductItem(PalMapObjectProductItemModel),
    RecoverOtomo(PalMapObjectRecoverOtomoModel),
    HatchingEgg(PalMapObjectHatchingEggModel<T>),
    TreasureBox(PalMapObjectTreasureBoxModel),
    BreedFarm(PalMapObjectBreedFarmModel),
    Signboard(PalMapObjectSignboardModel),
    Lamp(PalMapObjectLampModel),
    Torch(PalMapObjectTorchModel),
    PalEgg(PalMapObjectPalEggModel),
    BaseCampPoint(PalMapObjectBaseCampPoint),
    ItemChest(PalMapObjectItemChestModel),
    ItemChestAffectCorruption(PalMapObjectItemChestAffectCorruption),
    Unknown(BaseModel),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize, T::SoftObjectPath: Serialize",
    deserialize = ""
))]
pub struct PalMapConcreteModel<T: ArchiveType = SaveGameArchiveType> {
    pub instance_id: FGuid,
    pub model_instance_id: FGuid,
    pub concrete_model_type: String,
    pub model_data: PalMapConcreteModelVariant<T>,
}

impl<T: ArchiveType> PalMapConcreteModel<T> {
    pub fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        eprintln!(
            "Warning: PalMapConcreteModel::read called without object_id context, using fallback"
        );
        Self::read_with_object_id(ar, "unknown")
    }

    pub(crate) fn read_with_object_id<A: ArchiveReader<ArchiveType = T>>(
        ar: &mut A,
        object_id: &str,
    ) -> Result<Self> {
        let instance_id = FGuid::read(ar)?;
        let model_instance_id = FGuid::read(ar)?;

        let concrete_model_type = MAP_OBJECT_TO_CONCRETE_MODEL
            .get(object_id.to_lowercase().as_str())
            .unwrap_or(&"BaseModel")
            .to_string();

        let model_data = match concrete_model_type.as_str() {
            "PalMapObjectCharacterTeamMissionModel" => {
                PalMapConcreteModelVariant::CharacterTeamMission(
                    PalMapObjectCharacterTeamMissionModel::read(ar)?,
                )
            }
            "PalMapObjectFarmSkillFruitsModel" => PalMapConcreteModelVariant::FarmSkillFruits(
                PalMapObjectFarmSkillFruitsModel::read(ar)?,
            ),
            "PalMapObjectSupplyStorageModel" => {
                PalMapConcreteModelVariant::SupplyStorage(PalMapObjectSupplyStorageModel::read(ar)?)
            }
            "PalMapObjectItemBoothModel" => {
                PalMapConcreteModelVariant::ItemBooth(PalMapObjectItemBoothModel::read(ar)?)
            }
            "PalMapObjectEnergyStorageModel" => {
                PalMapConcreteModelVariant::EnergyStorage(PalMapObjectEnergyStorageModel::read(ar)?)
            }
            "PalMapObjectDeathDroppedCharacterModel" => {
                PalMapConcreteModelVariant::DeathDroppedCharacter(
                    PalMapObjectDeathDroppedCharacterModel::read(ar)?,
                )
            }
            "PalMapObjectConvertItemModel" => {
                PalMapConcreteModelVariant::ConvertItem(PalMapObjectConvertItemModel::read(ar)?)
            }
            "PalMapObjectPickupItemOnLevelModel" => PalMapConcreteModelVariant::PickupItemOnLevel(
                PalMapObjectPickupItemOnLevelModel::read(ar)?,
            ),
            "PalMapObjectDropItemModel" => {
                PalMapConcreteModelVariant::DropItem(PalMapObjectDropItemModel::read(ar)?)
            }
            "PalMapObjectItemDropOnDamagModel" => PalMapConcreteModelVariant::ItemDropOnDamag(
                PalMapObjectItemDropOnDamagModel::read(ar)?,
            ),
            "PalMapObjectDeathPenaltyStorageModel" => {
                PalMapConcreteModelVariant::DeathPenaltyStorage(
                    PalMapObjectDeathPenaltyStorageModel::read(ar)?,
                )
            }
            "PalMapObjectDefenseBulletLauncherModel" => {
                PalMapConcreteModelVariant::DefenseBulletLauncher(
                    PalMapObjectDefenseBulletLauncherModel::read(ar)?,
                )
            }
            "PalMapObjectGenerateEnergyModel" => PalMapConcreteModelVariant::GenerateEnergy(
                PalMapObjectGenerateEnergyModel::read(ar)?,
            ),
            "PalMapObjectFarmBlockV2Model" => {
                PalMapConcreteModelVariant::FarmBlockV2(PalMapObjectFarmBlockV2Model::read(ar)?)
            }
            "PalMapObjectFastTravelPointModel" => PalMapConcreteModelVariant::FastTravelPoint(
                PalMapObjectFastTravelPointModel::read(ar)?,
            ),
            "PalMapObjectShippingItemModel" => {
                PalMapConcreteModelVariant::ShippingItem(PalMapObjectShippingItemModel::read(ar)?)
            }
            "PalMapObjectProductItemModel" => {
                PalMapConcreteModelVariant::ProductItem(PalMapObjectProductItemModel::read(ar)?)
            }
            "PalMapObjectRecoverOtomoModel" => {
                PalMapConcreteModelVariant::RecoverOtomo(PalMapObjectRecoverOtomoModel::read(ar)?)
            }
            "PalMapObjectHatchingEggModel" => {
                PalMapConcreteModelVariant::HatchingEgg(PalMapObjectHatchingEggModel::read(ar)?)
            }
            "PalMapObjectTreasureBoxModel" => {
                PalMapConcreteModelVariant::TreasureBox(PalMapObjectTreasureBoxModel::read(ar)?)
            }
            "PalMapObjectBreedFarmModel" => {
                PalMapConcreteModelVariant::BreedFarm(PalMapObjectBreedFarmModel::read(ar)?)
            }
            "PalMapObjectSignboardModel" => {
                PalMapConcreteModelVariant::Signboard(PalMapObjectSignboardModel::read(ar)?)
            }
            "PalMapObjectTorchModel" => {
                PalMapConcreteModelVariant::Torch(PalMapObjectTorchModel::read(ar)?)
            }
            "PalMapObjectPalEggModel" => {
                PalMapConcreteModelVariant::PalEgg(PalMapObjectPalEggModel::read(ar)?)
            }
            "PalMapObjectBaseCampPoint" => {
                PalMapConcreteModelVariant::BaseCampPoint(PalMapObjectBaseCampPoint::read(ar)?)
            }
            "PalMapObjectItemChestModel" => {
                PalMapConcreteModelVariant::ItemChest(PalMapObjectItemChestModel::read(ar)?)
            }
            "PalMapObjectItemChest_AffectCorruption" => {
                PalMapConcreteModelVariant::ItemChestAffectCorruption(
                    PalMapObjectItemChestAffectCorruption::read(ar)?,
                )
            }
            "PalMapObjectLampModel" => {
                PalMapConcreteModelVariant::Lamp(PalMapObjectLampModel::read(ar)?)
            }
            _ => PalMapConcreteModelVariant::Unknown(BaseModel::read(ar)?),
        };

        Ok(PalMapConcreteModel {
            instance_id,
            model_instance_id,
            concrete_model_type,
            model_data,
        })
    }

    pub fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        self.instance_id.write(ar)?;
        self.model_instance_id.write(ar)?;

        match &self.model_data {
            PalMapConcreteModelVariant::CharacterTeamMission(model) => model.write(ar)?,
            PalMapConcreteModelVariant::FarmSkillFruits(model) => model.write(ar)?,
            PalMapConcreteModelVariant::SupplyStorage(model) => model.write(ar)?,
            PalMapConcreteModelVariant::ItemBooth(model) => model.write(ar)?,
            PalMapConcreteModelVariant::EnergyStorage(model) => model.write(ar)?,
            PalMapConcreteModelVariant::DeathDroppedCharacter(model) => model.write(ar)?,
            PalMapConcreteModelVariant::ConvertItem(model) => model.write(ar)?,
            PalMapConcreteModelVariant::PickupItemOnLevel(model) => model.write(ar)?,
            PalMapConcreteModelVariant::DropItem(model) => model.write(ar)?,
            PalMapConcreteModelVariant::ItemDropOnDamag(model) => model.write(ar)?,
            PalMapConcreteModelVariant::DeathPenaltyStorage(model) => model.write(ar)?,
            PalMapConcreteModelVariant::DefenseBulletLauncher(model) => model.write(ar)?,
            PalMapConcreteModelVariant::GenerateEnergy(model) => model.write(ar)?,
            PalMapConcreteModelVariant::FarmBlockV2(model) => model.write(ar)?,
            PalMapConcreteModelVariant::FastTravelPoint(model) => model.write(ar)?,
            PalMapConcreteModelVariant::ShippingItem(model) => model.write(ar)?,
            PalMapConcreteModelVariant::ProductItem(model) => model.write(ar)?,
            PalMapConcreteModelVariant::RecoverOtomo(model) => model.write(ar)?,
            PalMapConcreteModelVariant::HatchingEgg(model) => model.write(ar)?,
            PalMapConcreteModelVariant::TreasureBox(model) => model.write(ar)?,
            PalMapConcreteModelVariant::BreedFarm(model) => model.write(ar)?,
            PalMapConcreteModelVariant::Signboard(model) => model.write(ar)?,
            PalMapConcreteModelVariant::Lamp(model) => model.write(ar)?,
            PalMapConcreteModelVariant::Torch(model) => model.write(ar)?,
            PalMapConcreteModelVariant::PalEgg(model) => model.write(ar)?,
            PalMapConcreteModelVariant::BaseCampPoint(model) => model.write(ar)?,
            PalMapConcreteModelVariant::ItemChest(model) => model.write(ar)?,
            PalMapConcreteModelVariant::ItemChestAffectCorruption(model) => model.write(ar)?,
            PalMapConcreteModelVariant::Unknown(model) => model.write(ar)?,
        }

        Ok(())
    }
}
