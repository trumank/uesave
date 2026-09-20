use crate::{ArchiveReader, ArchiveWriter, FGuid, Result};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalConnectInfoItem {
    pub connect_to_model_instance_id: FGuid,
    pub index: u8,
}

impl PalConnectInfoItem {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalConnectInfoItem {
            connect_to_model_instance_id: FGuid::read(ar)?,
            index: ar.read_u8()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.connect_to_model_instance_id.write(ar)?;
        ar.write_u8(self.index)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalConnect {
    pub index: u8,
    pub any_place: Vec<PalConnectInfoItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalConnector {
    pub supported_level: i32,
    pub connect: PalConnect,
    pub unknown_bytes: Vec<u8>,
}

impl PalConnector {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let supported_level = ar.read_i32::<LE>()?;
        let connect_index = ar.read_u8()?;

        let any_place_count = ar.read_u32::<LE>()?;
        let mut any_place = Vec::with_capacity(crate::bounded_prealloc(any_place_count as usize));

        for _ in 0..any_place_count {
            any_place.push(PalConnectInfoItem::read(ar)?);
        }

        let mut unknown_bytes = Vec::new();
        ar.read_to_end(&mut unknown_bytes)?;

        Ok(PalConnector {
            supported_level,
            connect: PalConnect {
                index: connect_index,
                any_place,
            },
            unknown_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.supported_level)?;
        ar.write_u8(self.connect.index)?;

        ar.write_u32::<LE>(self.connect.any_place.len() as u32)?;
        for item in &self.connect.any_place {
            item.write(ar)?;
        }

        ar.write_all(&self.unknown_bytes)?;
        Ok(())
    }
}
