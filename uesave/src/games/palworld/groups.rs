use crate::games::palworld::bytes_remaining;
use crate::games::palworld::types::{PalInstanceId, PalPlayerInfo, PalPlayerInfoDetails};
use crate::{ArchiveReader, ArchiveWriter, Double, FGuid, Result, Vector};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};
use std::io::SeekFrom;

pub const GROUP_TYPE_GUILD: &str = "EPalGroupType::Guild";
pub const GROUP_TYPE_INDEPENDENT_GUILD: &str = "EPalGroupType::IndependentGuild";
pub const GROUP_TYPE_ORGANIZATION: &str = "EPalGroupType::Organization";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalGuildPlayerWithRole {
    pub player_uid: FGuid,
    pub player_info: PalPlayerInfoDetails,
    pub role: u8,
}

impl PalGuildPlayerWithRole {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let player = PalPlayerInfo::read(ar)?;
        Ok(PalGuildPlayerWithRole {
            player_uid: player.player_uid,
            player_info: player.player_info,
            role: ar.read_u8()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.player_uid.write(ar)?;
        ar.write_i64::<LE>(self.player_info.last_online_real_time)?;
        ar.write_string(&self.player_info.player_name)?;
        ar.write_u8(self.role)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalGuildMarker {
    pub marker_id: FGuid,
    pub icon_location: Vector,
    pub icon_type: i32,
    pub owner_player_uid: FGuid,
}

impl PalGuildMarker {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalGuildMarker {
            marker_id: FGuid::read(ar)?,
            icon_location: Vector {
                x: Double(ar.read_f64::<LE>()?),
                y: Double(ar.read_f64::<LE>()?),
                z: Double(ar.read_f64::<LE>()?),
            },
            icon_type: ar.read_i32::<LE>()?,
            owner_player_uid: FGuid::read(ar)?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.marker_id.write(ar)?;
        ar.write_f64::<LE>(self.icon_location.x.0)?;
        ar.write_f64::<LE>(self.icon_location.y.0)?;
        ar.write_f64::<LE>(self.icon_location.z.0)?;
        ar.write_i32::<LE>(self.icon_type)?;
        self.owner_player_uid.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalGuildRolePermission {
    pub role: u8,
    pub permissions: Vec<u8>,
}

impl PalGuildRolePermission {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let role = ar.read_u8()?;
        let permission_count = ar.read_u32::<LE>()?;
        let mut permissions =
            Vec::with_capacity(crate::bounded_prealloc(permission_count as usize));
        for _ in 0..permission_count {
            permissions.push(ar.read_u8()?);
        }
        Ok(PalGuildRolePermission { role, permissions })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.role)?;
        ar.write_u32::<LE>(self.permissions.len() as u32)?;
        for permission in &self.permissions {
            ar.write_u8(*permission)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalGuildTailPreUpdate {
    pub admin_player_uid: FGuid,
    pub players: Vec<PalPlayerInfo>,
    pub trailing_bytes: [u8; 4],
}

impl PalGuildTailPreUpdate {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let admin_player_uid = FGuid::read(ar)?;
        let player_count = ar.read_u32::<LE>()?;
        let players = crate::read_array(player_count, ar, PalPlayerInfo::read)?;
        let mut trailing_bytes = [0u8; 4];
        ar.read_exact(&mut trailing_bytes)?;
        Ok(PalGuildTailPreUpdate {
            admin_player_uid,
            players,
            trailing_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.admin_player_uid.write(ar)?;
        ar.write_u32::<LE>(self.players.len() as u32)?;
        for player in &self.players {
            player.write(ar)?;
        }
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalGuildTailPostUpdate {
    pub guild_chest_allowed_roles: Vec<u8>,
    pub unknown_i32: i32,
    pub admin_player_uid: FGuid,
    pub players: Vec<PalGuildPlayerWithRole>,
    pub role_permissions: Vec<PalGuildRolePermission>,
    pub trailing_bytes: [u8; 4],
}

impl PalGuildTailPostUpdate {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let chest_role_count = ar.read_u32::<LE>()?;
        let mut guild_chest_allowed_roles =
            Vec::with_capacity(crate::bounded_prealloc(chest_role_count as usize));
        for _ in 0..chest_role_count {
            guild_chest_allowed_roles.push(ar.read_u8()?);
        }
        let unknown_i32 = ar.read_i32::<LE>()?;
        let admin_player_uid = FGuid::read(ar)?;
        let player_count = ar.read_u32::<LE>()?;
        let players = crate::read_array(player_count, ar, PalGuildPlayerWithRole::read)?;
        let permission_count = ar.read_u32::<LE>()?;
        let role_permissions =
            crate::read_array(permission_count, ar, PalGuildRolePermission::read)?;
        let mut trailing_bytes = [0u8; 4];
        ar.read_exact(&mut trailing_bytes)?;
        Ok(PalGuildTailPostUpdate {
            guild_chest_allowed_roles,
            unknown_i32,
            admin_player_uid,
            players,
            role_permissions,
            trailing_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.guild_chest_allowed_roles.len() as u32)?;
        for role in &self.guild_chest_allowed_roles {
            ar.write_u8(*role)?;
        }
        ar.write_i32::<LE>(self.unknown_i32)?;
        self.admin_player_uid.write(ar)?;
        ar.write_u32::<LE>(self.players.len() as u32)?;
        for player in &self.players {
            player.write(ar)?;
        }
        ar.write_u32::<LE>(self.role_permissions.len() as u32)?;
        for role_permission in &self.role_permissions {
            role_permission.write(ar)?;
        }
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PalGuildTail {
    PostUpdate(PalGuildTailPostUpdate),
    PreUpdate(PalGuildTailPreUpdate),
}

impl PalGuildTail {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let start = ar.stream_position()?;

        if let Ok(tail) = PalGuildTailPostUpdate::read(ar) {
            if bytes_remaining(ar)? == 0 {
                return Ok(PalGuildTail::PostUpdate(tail));
            }
        }

        ar.seek(SeekFrom::Start(start))?;
        Ok(PalGuildTail::PreUpdate(PalGuildTailPreUpdate::read(ar)?))
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match self {
            PalGuildTail::PostUpdate(tail) => tail.write(ar),
            PalGuildTail::PreUpdate(tail) => tail.write(ar),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalGuildGroup {
    pub org_type: u8,
    pub leading_bytes: [u8; 4],
    pub base_ids: Vec<FGuid>,
    pub unknown_1: i32,
    pub base_camp_level: i32,
    pub map_object_instance_ids_base_camp_points: Vec<FGuid>,
    pub guild_name: String,
    pub last_guild_name_modifier_player_uid: FGuid,
    pub guild_markers: Vec<PalGuildMarker>,
    pub tail: PalGuildTail,
}

impl PalGuildGroup {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let org_type = ar.read_u8()?;
        let mut leading_bytes = [0u8; 4];
        ar.read_exact(&mut leading_bytes)?;
        let base_id_count = ar.read_u32::<LE>()?;
        let base_ids = crate::read_array(base_id_count, ar, FGuid::read)?;
        let unknown_1 = ar.read_i32::<LE>()?;
        let base_camp_level = ar.read_i32::<LE>()?;
        let base_camp_point_count = ar.read_u32::<LE>()?;
        let map_object_instance_ids_base_camp_points =
            crate::read_array(base_camp_point_count, ar, FGuid::read)?;
        let guild_name = ar.read_string()?;
        let last_guild_name_modifier_player_uid = FGuid::read(ar)?;
        let marker_count = ar.read_u32::<LE>()?;
        let guild_markers = crate::read_array(marker_count, ar, PalGuildMarker::read)?;
        let tail = PalGuildTail::read(ar)?;

        Ok(PalGuildGroup {
            org_type,
            leading_bytes,
            base_ids,
            unknown_1,
            base_camp_level,
            map_object_instance_ids_base_camp_points,
            guild_name,
            last_guild_name_modifier_player_uid,
            guild_markers,
            tail,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.org_type)?;
        ar.write_all(&self.leading_bytes)?;
        ar.write_u32::<LE>(self.base_ids.len() as u32)?;
        for base_id in &self.base_ids {
            base_id.write(ar)?;
        }
        ar.write_i32::<LE>(self.unknown_1)?;
        ar.write_i32::<LE>(self.base_camp_level)?;
        ar.write_u32::<LE>(self.map_object_instance_ids_base_camp_points.len() as u32)?;
        for point in &self.map_object_instance_ids_base_camp_points {
            point.write(ar)?;
        }
        ar.write_string(&self.guild_name)?;
        self.last_guild_name_modifier_player_uid.write(ar)?;
        ar.write_u32::<LE>(self.guild_markers.len() as u32)?;
        for marker in &self.guild_markers {
            marker.write(ar)?;
        }
        self.tail.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalIndependentGuildGroup {
    pub org_type: u8,
    pub base_camp_level: i32,
    pub map_object_instance_ids_base_camp_points: Vec<FGuid>,
    pub guild_name: String,
    pub player_uid: FGuid,
    pub guild_name_2: String,
    pub player_info: PalPlayerInfoDetails,
}

impl PalIndependentGuildGroup {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let org_type = ar.read_u8()?;
        let base_camp_level = ar.read_i32::<LE>()?;
        let base_camp_point_count = ar.read_u32::<LE>()?;
        let map_object_instance_ids_base_camp_points =
            crate::read_array(base_camp_point_count, ar, FGuid::read)?;
        Ok(PalIndependentGuildGroup {
            org_type,
            base_camp_level,
            map_object_instance_ids_base_camp_points,
            guild_name: ar.read_string()?,
            player_uid: FGuid::read(ar)?,
            guild_name_2: ar.read_string()?,
            player_info: PalPlayerInfoDetails {
                last_online_real_time: ar.read_i64::<LE>()?,
                player_name: ar.read_string()?,
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.org_type)?;
        ar.write_i32::<LE>(self.base_camp_level)?;
        ar.write_u32::<LE>(self.map_object_instance_ids_base_camp_points.len() as u32)?;
        for point in &self.map_object_instance_ids_base_camp_points {
            point.write(ar)?;
        }
        ar.write_string(&self.guild_name)?;
        self.player_uid.write(ar)?;
        ar.write_string(&self.guild_name_2)?;
        ar.write_i64::<LE>(self.player_info.last_online_real_time)?;
        ar.write_string(&self.player_info.player_name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalOrganizationGroup {
    pub org_type: u8,
    pub trailing_bytes: [u8; 12],
}

impl PalOrganizationGroup {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let org_type = ar.read_u8()?;
        let mut trailing_bytes = [0u8; 12];
        ar.read_exact(&mut trailing_bytes)?;
        Ok(PalOrganizationGroup {
            org_type,
            trailing_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.org_type)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PalGroupVariant {
    Guild(PalGuildGroup),
    IndependentGuild(PalIndependentGuildGroup),
    Organization(PalOrganizationGroup),
    Unknown { remaining_data: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalGroupData {
    pub group_id: FGuid,
    pub group_name: String,
    pub individual_character_handle_ids: Vec<PalInstanceId>,
    pub data: PalGroupVariant,
}

impl PalGroupData {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Self::read_with_group_type(ar, "")
    }

    pub fn read_with_group_type<A: ArchiveReader>(ar: &mut A, group_type: &str) -> Result<Self> {
        let group_id = FGuid::read(ar)?;
        let group_name = ar.read_string()?;
        let handle_ids_count = ar.read_u32::<LE>()?;
        let individual_character_handle_ids =
            crate::read_array(handle_ids_count, ar, PalInstanceId::read)?;

        let data = match group_type {
            GROUP_TYPE_GUILD => PalGroupVariant::Guild(PalGuildGroup::read(ar)?),
            GROUP_TYPE_INDEPENDENT_GUILD => {
                PalGroupVariant::IndependentGuild(PalIndependentGuildGroup::read(ar)?)
            }
            GROUP_TYPE_ORGANIZATION => {
                PalGroupVariant::Organization(PalOrganizationGroup::read(ar)?)
            }
            _ => {
                let mut remaining_data = Vec::new();
                ar.read_to_end(&mut remaining_data)?;
                PalGroupVariant::Unknown { remaining_data }
            }
        };

        Ok(PalGroupData {
            group_id,
            group_name,
            individual_character_handle_ids,
            data,
        })
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.group_id.write(ar)?;
        ar.write_string(&self.group_name)?;

        ar.write_u32::<LE>(self.individual_character_handle_ids.len() as u32)?;
        for handle_id in &self.individual_character_handle_ids {
            handle_id.write(ar)?;
        }

        match &self.data {
            PalGroupVariant::Guild(guild) => guild.write(ar)?,
            PalGroupVariant::IndependentGuild(guild) => guild.write(ar)?,
            PalGroupVariant::Organization(organization) => organization.write(ar)?,
            PalGroupVariant::Unknown { remaining_data } => ar.write_all(remaining_data)?,
        }

        Ok(())
    }
}
