use crate::{ArchiveReader, ArchiveWriter, FGuid, Result};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};

/// `BaseCampSaveData.WorkCollection.RawData`: the work ids a base owns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalWorkCollection {
    pub own_id: FGuid,
    pub work_ids: Vec<FGuid>,
    pub trailing_bytes: u32,
}

impl PalWorkCollection {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let own_id = FGuid::read(ar)?;
        let count = ar.read_i32::<LE>()?;
        let count = usize::try_from(count).map_err(|_| {
            crate::Error::Other(format!("WorkCollection has a negative work count: {count}"))
        })?;
        let mut work_ids = Vec::with_capacity(crate::bounded_prealloc(count));
        for _ in 0..count {
            work_ids.push(FGuid::read(ar)?);
        }
        Ok(PalWorkCollection {
            own_id,
            work_ids,
            trailing_bytes: ar.read_u32::<LE>()?,
        })
    }

    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.own_id.write(ar)?;
        ar.write_i32::<LE>(self.work_ids.len() as i32)?;
        for id in &self.work_ids {
            id.write(ar)?;
        }
        ar.write_u32::<LE>(self.trailing_bytes)?;
        Ok(())
    }
}
