use crate::games::palworld::types::PalTransform;
use crate::{ArchiveReader, ArchiveWriter, FGuid, Result};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalBaseCamp {
    pub id: FGuid,
    pub name: String,
    pub state: u8,
    pub transform: PalTransform,
    pub area_range: f32,
    pub group_id_belong_to: FGuid,
    pub fast_travel_local_transform: PalTransform,
    pub owner_map_object_instance_id: FGuid,
    pub trailing_bytes: [u8; 4],
}

impl PalBaseCamp {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalBaseCamp {
            id: FGuid::read(ar)?,
            name: ar.read_string()?,
            state: ar.read_u8()?,
            transform: PalTransform::read(ar)?,
            area_range: ar.read_f32::<LE>()?,
            group_id_belong_to: FGuid::read(ar)?,
            fast_travel_local_transform: PalTransform::read(ar)?,
            owner_map_object_instance_id: FGuid::read(ar)?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.id.write(ar)?;
        ar.write_string(&self.name)?;
        ar.write_u8(self.state)?;
        self.transform.write(ar)?;
        ar.write_f32::<LE>(self.area_range)?;
        self.group_id_belong_to.write(ar)?;
        self.fast_travel_local_transform.write(ar)?;
        self.owner_map_object_instance_id.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}
