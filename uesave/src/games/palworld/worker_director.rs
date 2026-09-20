use super::types::PalTransform;
use crate::{ArchiveReader, ArchiveWriter, FGuid, Result};
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

/// `BaseCampSaveData.WorkerDirector.RawData`: a fixed 118-byte payload that names
/// the base's worker character container a second time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalWorkerDirector {
    pub id: FGuid,
    pub spawn_transform: PalTransform,
    pub current_order_type: u8,
    pub current_battle_type: u8,
    pub container_id: FGuid,
    pub trailing_bytes: [u8; 4],
}

impl PalWorkerDirector {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalWorkerDirector {
            id: FGuid::read(ar)?,
            spawn_transform: PalTransform::read(ar)?,
            current_order_type: ar.read_u8()?,
            current_battle_type: ar.read_u8()?,
            container_id: FGuid::read(ar)?,
            trailing_bytes: {
                let mut bytes = [0u8; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.id.write(ar)?;
        self.spawn_transform.write(ar)?;
        ar.write_u8(self.current_order_type)?;
        ar.write_u8(self.current_battle_type)?;
        self.container_id.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}
