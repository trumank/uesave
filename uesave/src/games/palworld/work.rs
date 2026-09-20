use super::map_object::convert_embedded;
use super::pal_struct::PalStruct;
use super::types::PalInstanceId;
use super::Palworld;
use crate::{
    read_array, ArchiveReader, ArchiveWriter, Double, FGuid, Properties, Property, PropertyKey,
    Quat, Result, SaveGameArchive, SaveGameArchiveType, StructType, StructValue, Vector,
};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek};

/// Work types that serialize `UPalWorkBase` before their own data. Types absent
/// from this list (and from [`WORK_ASSIGN_TYPES`]) keep their payload raw.
const WORK_BASE_TYPES: [&str; 13] = [
    "EPalWorkableType::Progress",
    "EPalWorkableType::Progress_MultiType",
    "EPalWorkableType::TransportItemInBaseCamp",
    "EPalWorkableType::ReviveCharacter",
    "EPalWorkableType::Booth",
    "EPalWorkableType::LevelObject",
    "EPalWorkableType::Repair",
    "EPalWorkableType::Defense",
    "EPalWorkableType::BootUp",
    "EPalWorkableType::OnlyJoin",
    "EPalWorkableType::OnlyJoinAndWalkAround",
    "EPalWorkableType::RemoveMapObjectEffect",
    "EPalWorkableType::MonsterFarm",
];

/// Work types that serialize an assignment record instead of a work base.
/// `LevelObject` is listed here in the game's code too, but it serializes a base
/// and so never reaches this path.
const WORK_ASSIGN_TYPES: [&str; 1] = ["EPalWorkableType::Assign"];

const PROGRESS: &str = "EPalWorkableType::Progress";
const PROGRESS_MULTI_TYPE: &str = "EPalWorkableType::Progress_MultiType";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalWork {
    pub work_type: String,
    pub base_data: Option<PalWorkBase>,
    pub transform: Option<PalWorkTransform>,
    pub work_specific_data: PalWorkTypeSpecificData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalWorkBase {
    pub id: FGuid,
    pub workable_bounds: PalWorkableBounds,
    pub base_camp_id_belong_to: FGuid,
    pub owner_map_object_model_id: FGuid,
    pub owner_map_object_concrete_model_id: FGuid,
    pub current_state: u8,
    pub assign_locations: Vec<PalAssignLocation>,
    pub behaviour_type: u8,
    pub assign_define_data_id: String,
    pub override_work_type: u8,
    pub assignable_fixed_type: u8,
    pub assignable_otomo: u32,
    pub can_trigger_worker_event: u32,
    pub can_steal_assign: u32,
}

/// A `WorkAssignMap` entry: one worker assigned to one work.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalWorkAssign {
    pub id: FGuid,
    pub location_index: i32,
    pub assign_type: u8,
    pub assigned_individual_id: PalInstanceId,
    pub state: u8,
    pub fixed: u32,
    pub trailing_bytes: [u8; 4],
    pub multi_type: Option<PalWorkAssignMultiType>,
}

/// Assignments to a `Progress_MultiType` work use `UPalWorkAssign_WorkProgressMultiType`,
/// which records which of the work's suitabilities the worker took.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalWorkAssignMultiType {
    pub assigned_work_suitability: u8,
    pub assigned_work_type: u8,
    pub assigned_work_action_type: u8,
    pub trailing_bytes: [u8; 4],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalWorkableBounds {
    pub location: Vector,
    pub rotation: Quat,
    pub box_sphere_bounds: PalBoxSphereBounds,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalBoxSphereBounds {
    pub origin: Vector,
    pub box_extent: Vector,
    pub sphere_radius: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalAssignLocation {
    pub location: Vector,
    pub facing_direction: Vector,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalWorkTransform {
    pub transform_type: u8,
    pub map_object_instance_id: Option<FGuid>,
    pub trailing_bytes: Option<[u8; 8]>,
}

/// Progress of a `Progress_MultiType` work for a single work suitability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalWorkProgressEntry {
    pub work_suitability: u8,
    pub current_progress: f32,
    pub max_progress: f32,
    pub max_storable_progress: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalWorkSuitabilityInfo {
    pub work_suitability: u8,
    pub required_rank: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PalWorkTypeSpecificData {
    Unknown {
        data: Vec<u8>,
    },
    /// A work base with no data of its own.
    None,
    Defense {
        leading_bytes: [u8; 4],
        defense_combat_type: u8,
        trailing_bytes: [u8; 4],
    },
    Progress {
        required_work_amount: f32,
        current_work_amount: f32,
        work_exp: i32,
        work_exp_calc_type: u8,
        auto_work_self_amount_by_sec: f32,
        progress_time_since_last_tick: f32,
        tick_process_min_interval: f32,
    },
    ProgressMultiType {
        required_work_amount: f32,
        current_work_amount: f32,
        work_exp: i32,
        work_exp_calc_type: u8,
        auto_work_self_amount_by_sec: f32,
        progress_time_since_last_tick: f32,
        tick_process_min_interval: f32,
        progress_entries: Vec<PalWorkProgressEntry>,
        suitability_info_entries: Vec<PalWorkSuitabilityInfo>,
    },
    ReviveCharacter {
        target_individual_id: PalInstanceId,
    },
    SimpleWork {
        required_work_amount: f32,
    },
    Assign {
        handle_id: FGuid,
        location_index: i32,
        assign_type: u8,
        assigned_individual_id: PalInstanceId,
        state: u8,
        fixed: u32,
    },
}

/// Parses the embedded data of one `WorkSaveData` element: the work itself and
/// each of its assignment records, both of which are laid out according to the
/// element's `WorkableType`.
pub(crate) fn parse_work_with_context<R: Read + Seek>(
    ar: &mut SaveGameArchive<R, Palworld>,
    properties: &mut Properties<SaveGameArchiveType<Palworld>>,
) -> Result<()> {
    let work_type = match properties.0.get(&PropertyKey::from("WorkableType")) {
        Some(Property::Enum(t)) | Some(Property::Str(t)) | Some(Property::Name(t)) => t.clone(),
        other => {
            return Err(crate::Error::Other(format!(
                "WorkSaveData element expected a WorkableType name, but found: {other:?}"
            )))
        }
    };

    if let Some(raw_data_prop) = properties.0.get_mut(&PropertyKey::from("RawData")) {
        let work_type = work_type.clone();
        convert_embedded(
            ar,
            raw_data_prop,
            &["RawData"],
            StructType::Game("PalWork".to_owned()),
            move |nested| {
                Ok(StructValue::Game(PalStruct::Work(
                    PalWork::read_with_work_type(nested, &work_type)?.into(),
                )))
            },
        )?;
    }

    let Some(Property::Map(assign_entries)) =
        properties.0.get_mut(&PropertyKey::from("WorkAssignMap"))
    else {
        return Ok(());
    };
    for entry in assign_entries.iter_mut() {
        let Property::Struct(StructValue::Struct(assign_properties)) = &mut entry.value else {
            continue;
        };
        let Some(raw_data_prop) = assign_properties.0.get_mut(&PropertyKey::from("RawData")) else {
            continue;
        };
        let work_type = work_type.clone();
        convert_embedded(
            ar,
            raw_data_prop,
            // Map entries do not push Key/Value scope segments, so value
            // properties live directly under the map path
            &["WorkAssignMap", "RawData"],
            StructType::Game("PalWorkAssign".to_owned()),
            move |nested| {
                Ok(StructValue::Game(PalStruct::WorkAssign(
                    PalWorkAssign::read_with_work_type(nested, &work_type)?,
                )))
            },
        )?;
    }

    Ok(())
}

/// Palworld always serializes vectors and quaternions as doubles here,
/// regardless of the archive's large-world-coordinates flag.
fn read_vector<A: ArchiveReader>(ar: &mut A) -> Result<Vector> {
    Ok(Vector {
        x: Double(ar.read_f64::<LE>()?),
        y: Double(ar.read_f64::<LE>()?),
        z: Double(ar.read_f64::<LE>()?),
    })
}

fn write_vector<A: ArchiveWriter>(ar: &mut A, vector: &Vector) -> Result<()> {
    ar.write_f64::<LE>(vector.x.0)?;
    ar.write_f64::<LE>(vector.y.0)?;
    ar.write_f64::<LE>(vector.z.0)?;
    Ok(())
}

impl PalWork {
    /// Reads a work payload. The layout depends on the work's `WorkableType`,
    /// which lives in a sibling property, not in the payload itself.
    pub fn read_with_work_type<A: ArchiveReader>(ar: &mut A, work_type: &str) -> Result<Self> {
        let is_base = WORK_BASE_TYPES.contains(&work_type);
        let is_assign = WORK_ASSIGN_TYPES.contains(&work_type);

        if !is_base && !is_assign {
            let mut data = Vec::new();
            ar.read_to_end(&mut data)?;
            return Ok(PalWork {
                work_type: work_type.to_string(),
                base_data: None,
                transform: None,
                work_specific_data: PalWorkTypeSpecificData::Unknown { data },
            });
        }

        let base_data = if is_base {
            Some(PalWorkBase::read(ar)?)
        } else {
            None
        };
        let work_specific_data = PalWorkTypeSpecificData::read_with_work_type(ar, work_type)?;
        let transform = Some(PalWorkTransform::read(ar)?);

        Ok(PalWork {
            work_type: work_type.to_string(),
            base_data,
            transform,
            work_specific_data,
        })
    }

    /// Reads a work payload whose type is unknown, keeping the bytes raw.
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Self::read_with_work_type(ar, "Unknown")
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        if let PalWorkTypeSpecificData::Unknown { data } = &self.work_specific_data {
            ar.write_all(data)?;
            return Ok(());
        }
        if let Some(base_data) = &self.base_data {
            base_data.write(ar)?;
        }
        self.work_specific_data.write(ar)?;
        if let Some(transform) = &self.transform {
            transform.write(ar)?;
        }
        Ok(())
    }
}

impl PalWorkTypeSpecificData {
    fn read_with_work_type<A: ArchiveReader>(ar: &mut A, work_type: &str) -> Result<Self> {
        Ok(match work_type {
            "EPalWorkableType::Defense" => PalWorkTypeSpecificData::Defense {
                leading_bytes: read_bytes(ar)?,
                defense_combat_type: ar.read_u8()?,
                trailing_bytes: read_bytes(ar)?,
            },
            PROGRESS | PROGRESS_MULTI_TYPE => {
                let required_work_amount = ar.read_f32::<LE>()?;
                let current_work_amount = ar.read_f32::<LE>()?;
                let work_exp = ar.read_i32::<LE>()?;
                let work_exp_calc_type = ar.read_u8()?;
                let auto_work_self_amount_by_sec = ar.read_f32::<LE>()?;
                let progress_time_since_last_tick = ar.read_f32::<LE>()?;
                let tick_process_min_interval = ar.read_f32::<LE>()?;

                if work_type == PROGRESS {
                    PalWorkTypeSpecificData::Progress {
                        required_work_amount,
                        current_work_amount,
                        work_exp,
                        work_exp_calc_type,
                        auto_work_self_amount_by_sec,
                        progress_time_since_last_tick,
                        tick_process_min_interval,
                    }
                } else {
                    let progress_count = ar.read_u32::<LE>()?;
                    let progress_entries =
                        read_array(progress_count, ar, PalWorkProgressEntry::read)?;
                    let suitability_count = ar.read_u32::<LE>()?;
                    let suitability_info_entries =
                        read_array(suitability_count, ar, PalWorkSuitabilityInfo::read)?;
                    PalWorkTypeSpecificData::ProgressMultiType {
                        required_work_amount,
                        current_work_amount,
                        work_exp,
                        work_exp_calc_type,
                        auto_work_self_amount_by_sec,
                        progress_time_since_last_tick,
                        tick_process_min_interval,
                        progress_entries,
                        suitability_info_entries,
                    }
                }
            }
            "EPalWorkableType::ReviveCharacter" => PalWorkTypeSpecificData::ReviveCharacter {
                target_individual_id: PalInstanceId::read(ar)?,
            },
            "EPalWorkableType::Repair"
            | "EPalWorkableType::MonsterFarm"
            | "EPalWorkableType::OnlyJoinAndWalkAround"
            | "EPalWorkableType::OnlyJoin"
            | "EPalWorkableType::Booth" => PalWorkTypeSpecificData::SimpleWork {
                required_work_amount: ar.read_f32::<LE>()?,
            },
            "EPalWorkableType::Assign" => PalWorkTypeSpecificData::Assign {
                handle_id: FGuid::read(ar)?,
                location_index: ar.read_i32::<LE>()?,
                assign_type: ar.read_u8()?,
                assigned_individual_id: PalInstanceId::read(ar)?,
                state: ar.read_u8()?,
                fixed: ar.read_u32::<LE>()?,
            },
            _ => PalWorkTypeSpecificData::None,
        })
    }

    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match self {
            PalWorkTypeSpecificData::Unknown { data } => ar.write_all(data)?,
            PalWorkTypeSpecificData::None => {}
            PalWorkTypeSpecificData::Defense {
                leading_bytes,
                defense_combat_type,
                trailing_bytes,
            } => {
                ar.write_all(leading_bytes)?;
                ar.write_u8(*defense_combat_type)?;
                ar.write_all(trailing_bytes)?;
            }
            PalWorkTypeSpecificData::Progress {
                required_work_amount,
                current_work_amount,
                work_exp,
                work_exp_calc_type,
                auto_work_self_amount_by_sec,
                progress_time_since_last_tick,
                tick_process_min_interval,
            } => {
                ar.write_f32::<LE>(*required_work_amount)?;
                ar.write_f32::<LE>(*current_work_amount)?;
                ar.write_i32::<LE>(*work_exp)?;
                ar.write_u8(*work_exp_calc_type)?;
                ar.write_f32::<LE>(*auto_work_self_amount_by_sec)?;
                ar.write_f32::<LE>(*progress_time_since_last_tick)?;
                ar.write_f32::<LE>(*tick_process_min_interval)?;
            }
            PalWorkTypeSpecificData::ProgressMultiType {
                required_work_amount,
                current_work_amount,
                work_exp,
                work_exp_calc_type,
                auto_work_self_amount_by_sec,
                progress_time_since_last_tick,
                tick_process_min_interval,
                progress_entries,
                suitability_info_entries,
            } => {
                ar.write_f32::<LE>(*required_work_amount)?;
                ar.write_f32::<LE>(*current_work_amount)?;
                ar.write_i32::<LE>(*work_exp)?;
                ar.write_u8(*work_exp_calc_type)?;
                ar.write_f32::<LE>(*auto_work_self_amount_by_sec)?;
                ar.write_f32::<LE>(*progress_time_since_last_tick)?;
                ar.write_f32::<LE>(*tick_process_min_interval)?;
                ar.write_u32::<LE>(progress_entries.len() as u32)?;
                for entry in progress_entries {
                    entry.write(ar)?;
                }
                ar.write_u32::<LE>(suitability_info_entries.len() as u32)?;
                for entry in suitability_info_entries {
                    entry.write(ar)?;
                }
            }
            PalWorkTypeSpecificData::ReviveCharacter {
                target_individual_id,
            } => target_individual_id.write(ar)?,
            PalWorkTypeSpecificData::SimpleWork {
                required_work_amount,
            } => ar.write_f32::<LE>(*required_work_amount)?,
            PalWorkTypeSpecificData::Assign {
                handle_id,
                location_index,
                assign_type,
                assigned_individual_id,
                state,
                fixed,
            } => {
                handle_id.write(ar)?;
                ar.write_i32::<LE>(*location_index)?;
                ar.write_u8(*assign_type)?;
                assigned_individual_id.write(ar)?;
                ar.write_u8(*state)?;
                ar.write_u32::<LE>(*fixed)?;
            }
        }
        Ok(())
    }
}

fn read_bytes<A: ArchiveReader, const N: usize>(ar: &mut A) -> Result<[u8; N]> {
    let mut bytes = [0u8; N];
    ar.read_exact(&mut bytes)?;
    Ok(bytes)
}

impl PalWorkBase {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let id = FGuid::read(ar)?;
        let workable_bounds = PalWorkableBounds::read(ar)?;
        let base_camp_id_belong_to = FGuid::read(ar)?;
        let owner_map_object_model_id = FGuid::read(ar)?;
        let owner_map_object_concrete_model_id = FGuid::read(ar)?;
        let current_state = ar.read_u8()?;
        let location_count = ar.read_u32::<LE>()?;
        let assign_locations = read_array(location_count, ar, PalAssignLocation::read)?;

        Ok(PalWorkBase {
            id,
            workable_bounds,
            base_camp_id_belong_to,
            owner_map_object_model_id,
            owner_map_object_concrete_model_id,
            current_state,
            assign_locations,
            behaviour_type: ar.read_u8()?,
            assign_define_data_id: ar.read_string()?,
            override_work_type: ar.read_u8()?,
            assignable_fixed_type: ar.read_u8()?,
            assignable_otomo: ar.read_u32::<LE>()?,
            can_trigger_worker_event: ar.read_u32::<LE>()?,
            can_steal_assign: ar.read_u32::<LE>()?,
        })
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.id.write(ar)?;
        self.workable_bounds.write(ar)?;
        self.base_camp_id_belong_to.write(ar)?;
        self.owner_map_object_model_id.write(ar)?;
        self.owner_map_object_concrete_model_id.write(ar)?;
        ar.write_u8(self.current_state)?;
        ar.write_u32::<LE>(self.assign_locations.len() as u32)?;
        for location in &self.assign_locations {
            location.write(ar)?;
        }
        ar.write_u8(self.behaviour_type)?;
        ar.write_string(&self.assign_define_data_id)?;
        ar.write_u8(self.override_work_type)?;
        ar.write_u8(self.assignable_fixed_type)?;
        ar.write_u32::<LE>(self.assignable_otomo)?;
        ar.write_u32::<LE>(self.can_trigger_worker_event)?;
        ar.write_u32::<LE>(self.can_steal_assign)?;
        Ok(())
    }
}

impl PalWorkAssign {
    /// Reads a work assign payload. Its tail depends on the owning work's type.
    pub fn read_with_work_type<A: ArchiveReader>(ar: &mut A, work_type: &str) -> Result<Self> {
        Ok(PalWorkAssign {
            id: FGuid::read(ar)?,
            location_index: ar.read_i32::<LE>()?,
            assign_type: ar.read_u8()?,
            assigned_individual_id: PalInstanceId::read(ar)?,
            state: ar.read_u8()?,
            fixed: ar.read_u32::<LE>()?,
            trailing_bytes: read_bytes(ar)?,
            multi_type: if work_type == PROGRESS_MULTI_TYPE {
                Some(PalWorkAssignMultiType::read(ar)?)
            } else {
                None
            },
        })
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.id.write(ar)?;
        ar.write_i32::<LE>(self.location_index)?;
        ar.write_u8(self.assign_type)?;
        self.assigned_individual_id.write(ar)?;
        ar.write_u8(self.state)?;
        ar.write_u32::<LE>(self.fixed)?;
        ar.write_all(&self.trailing_bytes)?;
        if let Some(multi_type) = &self.multi_type {
            multi_type.write(ar)?;
        }
        Ok(())
    }
}

impl PalWorkAssignMultiType {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalWorkAssignMultiType {
            assigned_work_suitability: ar.read_u8()?,
            assigned_work_type: ar.read_u8()?,
            assigned_work_action_type: ar.read_u8()?,
            trailing_bytes: read_bytes(ar)?,
        })
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.assigned_work_suitability)?;
        ar.write_u8(self.assigned_work_type)?;
        ar.write_u8(self.assigned_work_action_type)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

impl PalWorkProgressEntry {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalWorkProgressEntry {
            work_suitability: ar.read_u8()?,
            current_progress: ar.read_f32::<LE>()?,
            max_progress: ar.read_f32::<LE>()?,
            max_storable_progress: ar.read_f32::<LE>()?,
        })
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.work_suitability)?;
        ar.write_f32::<LE>(self.current_progress)?;
        ar.write_f32::<LE>(self.max_progress)?;
        ar.write_f32::<LE>(self.max_storable_progress)?;
        Ok(())
    }
}

impl PalWorkSuitabilityInfo {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalWorkSuitabilityInfo {
            work_suitability: ar.read_u8()?,
            required_rank: ar.read_i32::<LE>()?,
        })
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.work_suitability)?;
        ar.write_i32::<LE>(self.required_rank)?;
        Ok(())
    }
}

impl PalWorkTransform {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let transform_type = ar.read_u8()?;
        let (map_object_instance_id, trailing_bytes) = if transform_type == 2 {
            (Some(FGuid::read(ar)?), Some(read_bytes(ar)?))
        } else {
            (None, None)
        };
        Ok(PalWorkTransform {
            transform_type,
            map_object_instance_id,
            trailing_bytes,
        })
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.transform_type)?;
        if let Some(id) = &self.map_object_instance_id {
            id.write(ar)?;
        }
        if let Some(bytes) = &self.trailing_bytes {
            ar.write_all(bytes)?;
        }
        Ok(())
    }
}

impl PalWorkableBounds {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalWorkableBounds {
            location: read_vector(ar)?,
            rotation: Quat {
                x: Double(ar.read_f64::<LE>()?),
                y: Double(ar.read_f64::<LE>()?),
                z: Double(ar.read_f64::<LE>()?),
                w: Double(ar.read_f64::<LE>()?),
            },
            box_sphere_bounds: PalBoxSphereBounds::read(ar)?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        write_vector(ar, &self.location)?;
        ar.write_f64::<LE>(self.rotation.x.0)?;
        ar.write_f64::<LE>(self.rotation.y.0)?;
        ar.write_f64::<LE>(self.rotation.z.0)?;
        ar.write_f64::<LE>(self.rotation.w.0)?;
        self.box_sphere_bounds.write(ar)?;
        Ok(())
    }
}

impl PalBoxSphereBounds {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalBoxSphereBounds {
            origin: read_vector(ar)?,
            box_extent: read_vector(ar)?,
            sphere_radius: ar.read_f64::<LE>()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        write_vector(ar, &self.origin)?;
        write_vector(ar, &self.box_extent)?;
        ar.write_f64::<LE>(self.sphere_radius)?;
        Ok(())
    }
}

impl PalAssignLocation {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalAssignLocation {
            location: read_vector(ar)?,
            facing_direction: read_vector(ar)?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        write_vector(ar, &self.location)?;
        write_vector(ar, &self.facing_direction)?;
        Ok(())
    }
}
