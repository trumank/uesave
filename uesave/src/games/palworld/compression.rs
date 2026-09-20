use crate::{Error, Result};
use byteorder::{WriteBytesExt, LE};
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use std::io::{Read, Write};

#[cfg(feature = "oodle")]
use ooz_rs::{compress, decompress, OodleCompressor, OodleLevel};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionFormat {
    None,
    Oodle,
    Zlib,
    Chunk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MagicBytes {
    GVAS,
    PLM,
    PLZ,
    CNK,
}

impl MagicBytes {
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            MagicBytes::GVAS => b"GVAS",
            MagicBytes::PLM => b"PlM",
            MagicBytes::PLZ => b"PlZ",
            MagicBytes::CNK => b"CNK",
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            b"GVAS" => Some(MagicBytes::GVAS),
            b"PlM" => Some(MagicBytes::PLM),
            b"PlZ" => Some(MagicBytes::PLZ),
            b"CNK" => Some(MagicBytes::CNK),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct CompressionHeader {
    pub uncompressed_len: u32,
    pub compressed_len: u32,
    pub magic_bytes: MagicBytes,
    pub save_type: u8,
    pub data_offset: usize,
}

impl CompressionHeader {
    pub fn read<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut header_buf = vec![0u8; 24];
        let bytes_read = reader.read(&mut header_buf)?;

        if bytes_read < 12 {
            return Ok(None);
        }

        if &header_buf[0..4] == b"GVAS" {
            return Ok(None);
        }

        let mut uncompressed_len =
            u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let mut compressed_len =
            u32::from_le_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);
        let mut magic_offset = 8;
        let mut save_type_offset = 11;
        let mut data_offset = 12;

        // A CNK file is keyed on the OUTER magic at [8:11]. When present, the real
        // header is the NESTED inner one, and the magic/save_type used downstream
        // become the inner values.
        if bytes_read >= 24 && &header_buf[8..11] == b"CNK" {
            uncompressed_len = u32::from_le_bytes([
                header_buf[12],
                header_buf[13],
                header_buf[14],
                header_buf[15],
            ]);
            compressed_len = u32::from_le_bytes([
                header_buf[16],
                header_buf[17],
                header_buf[18],
                header_buf[19],
            ]);
            magic_offset = 20;
            save_type_offset = 23;
            data_offset = 24;
        }

        let magic_bytes = MagicBytes::from_bytes(&header_buf[magic_offset..magic_offset + 3])
            .ok_or_else(|| {
                Error::Other(format!(
                    "Unknown magic bytes: {:?}",
                    &header_buf[magic_offset..magic_offset + 3]
                ))
            })?;
        let save_type = header_buf[save_type_offset];

        Ok(Some(CompressionHeader {
            uncompressed_len,
            compressed_len,
            magic_bytes,
            save_type,
            data_offset,
        }))
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        if self.magic_bytes != MagicBytes::PLM && self.magic_bytes != MagicBytes::PLZ {
            return Err(Error::Other(
                "Only PLM and PLZ format writing is supported".into(),
            ));
        }

        writer.write_u32::<LE>(self.uncompressed_len)?;
        writer.write_u32::<LE>(self.compressed_len)?;
        writer.write_all(self.magic_bytes.as_bytes())?;
        writer.write_u8(self.save_type)?;

        Ok(())
    }
}

fn zlib_inflate(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(data);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| Error::Other(format!("zlib inflate failed: {e}")))?;
    Ok(out)
}

fn zlib_deflate(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(data)
        .map_err(|e| Error::Other(format!("zlib deflate failed: {e}")))?;
    encoder
        .finish()
        .map_err(|e| Error::Other(format!("zlib deflate failed: {e}")))
}

/// Zlib decompress for PLZ (double-zlib, save_type 0x32) and CNK (nested; the inner
/// save_type decides single vs. double inflate).
fn decompress_zlib(header: &CompressionHeader, data: &[u8]) -> Result<Vec<u8>> {
    let first = zlib_inflate(&data[header.data_offset..])?;

    let result = if header.save_type == 0x32 {
        // PLZ: the payload is double-zlib compressed.
        if header.compressed_len as usize != first.len() {
            return Err(Error::Other(format!(
                "incorrect compressed length: {}",
                header.compressed_len
            )));
        }
        zlib_inflate(&first)?
    } else {
        first
    };

    if header.uncompressed_len as usize != result.len() {
        return Err(Error::Other(format!(
            "incorrect uncompressed length: {} != {}",
            header.uncompressed_len,
            result.len()
        )));
    }

    Ok(result)
}

/// Zlib compress in the double-zlib PLZ (0x32) form, matching `Zlib.compress`. This is
/// the only zlib form the reference emits; modified gamepass saves are written as PlZ.
fn compress_zlib(data: &[u8]) -> Result<Vec<u8>> {
    let first = zlib_deflate(data)?;
    let second = zlib_deflate(&first)?;

    let header = CompressionHeader {
        uncompressed_len: data.len() as u32,
        compressed_len: first.len() as u32,
        magic_bytes: MagicBytes::PLZ,
        save_type: 0x32,
        data_offset: 12,
    };

    let mut result = Vec::new();
    header.write(&mut result)?;
    result.extend_from_slice(&second);

    Ok(result)
}

pub fn decompress_save<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;

    if let Some(header) = CompressionHeader::read(&mut &data[..])? {
        match header.magic_bytes {
            #[cfg(feature = "oodle")]
            MagicBytes::PLM => {
                let compressed_payload =
                    &data[header.data_offset..header.data_offset + header.compressed_len as usize];

                let decompressed = decompress(compressed_payload, header.uncompressed_len as usize)
                    .map_err(|e| Error::Other(format!("Oodle decompression failed: {e}")))?;

                Ok(decompressed)
            }
            #[cfg(not(feature = "oodle"))]
            MagicBytes::PLM => Err(Error::Other(
                "Oodle compression support not enabled. Rebuild with --features oodle".into(),
            )),
            MagicBytes::PLZ | MagicBytes::CNK => decompress_zlib(&header, &data),
            _ => Ok(data),
        }
    } else {
        Ok(data)
    }
}

#[cfg(feature = "oodle")]
pub fn compress_save(data: &[u8], format: CompressionFormat) -> Result<Vec<u8>> {
    match format {
        CompressionFormat::None => Ok(data.to_vec()),
        CompressionFormat::Oodle => {
            let compressed = compress(OodleCompressor::Mermaid, OodleLevel::Normal, data)
                .map_err(|e| Error::Other(format!("Oodle compression failed: {e}")))?;

            let header = CompressionHeader {
                uncompressed_len: data.len() as u32,
                compressed_len: compressed.len() as u32,
                magic_bytes: MagicBytes::PLM,
                save_type: 0x31,
                data_offset: 12,
            };

            let mut result = Vec::new();
            header.write(&mut result)?;
            result.extend_from_slice(&compressed);

            Ok(result)
        }
        CompressionFormat::Zlib => compress_zlib(data),
        CompressionFormat::Chunk => Err(Error::Other("Chunk format not yet supported".into())),
    }
}

#[cfg(not(feature = "oodle"))]
pub fn compress_save(data: &[u8], format: CompressionFormat) -> Result<Vec<u8>> {
    match format {
        CompressionFormat::None => Ok(data.to_vec()),
        CompressionFormat::Zlib => compress_zlib(data),
        _ => Err(Error::Other(
            "Compression support not enabled. Rebuild with --features oodle".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A CNK save is keyed on the outer magic but must report the inner header;
    // pin the offsets so that inversion cannot regress.
    #[test]
    fn header_parse_cnk_reads_inner() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // outer uncompressed_len (ignored)
        buf.extend_from_slice(&2u32.to_le_bytes()); // outer compressed_len (ignored)
        buf.extend_from_slice(b"CNK");
        buf.push(0x30);
        buf.extend_from_slice(&100u32.to_le_bytes()); // inner uncompressed_len
        buf.extend_from_slice(&50u32.to_le_bytes()); // inner compressed_len
        buf.extend_from_slice(b"PlZ");
        buf.push(0x32);

        let header = CompressionHeader::read(&mut &buf[..]).unwrap().unwrap();
        assert_eq!(header.data_offset, 24);
        assert_eq!(header.magic_bytes, MagicBytes::PLZ);
        assert_eq!(header.save_type, 0x32);
        assert_eq!(header.uncompressed_len, 100);
        assert_eq!(header.compressed_len, 50);
    }

    #[test]
    fn header_parse_plz_flat() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u32.to_le_bytes());
        buf.extend_from_slice(&50u32.to_le_bytes());
        buf.extend_from_slice(b"PlZ");
        buf.push(0x32);

        let header = CompressionHeader::read(&mut &buf[..]).unwrap().unwrap();
        assert_eq!(header.data_offset, 12);
        assert_eq!(header.magic_bytes, MagicBytes::PLZ);
        assert_eq!(header.save_type, 0x32);
        assert_eq!(header.uncompressed_len, 100);
        assert_eq!(header.compressed_len, 50);
    }

    #[test]
    fn plz_round_trip() {
        let mut original = b"GVAS".to_vec();
        // A repetitive-but-nontrivial payload so both deflate passes do real work.
        for i in 0..1000u32 {
            original.extend_from_slice(&i.to_le_bytes());
        }

        let compressed = compress_save(&original, CompressionFormat::Zlib).unwrap();

        // Header is ["PlZ"][0x32] with correct uncompressed_len.
        assert_eq!(&compressed[8..11], b"PlZ");
        assert_eq!(compressed[11], 0x32);
        let uncompressed_len = u32::from_le_bytes(compressed[0..4].try_into().unwrap());
        assert_eq!(uncompressed_len as usize, original.len());
        // compressed_len is the length of the first (inner) deflate.
        let compressed_len = u32::from_le_bytes(compressed[4..8].try_into().unwrap());
        assert!(compressed_len > 0);

        let back = decompress_save(&mut &compressed[..]).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn cnk_decompress_real_corpus() {
        let Some(path) = std::env::var_os("UESAVE_CNK_SAVE") else {
            eprintln!("skipping cnk_decompress_real_corpus: set UESAVE_CNK_SAVE to a CNK save");
            return;
        };
        let bytes = std::fs::read(path).unwrap();

        // Confirm it really is a CNK file with an inner PlZ header.
        let header = CompressionHeader::read(&mut &bytes[..]).unwrap().unwrap();
        assert_eq!(header.data_offset, 24);
        assert_eq!(header.magic_bytes, MagicBytes::PLZ);

        let result = decompress_save(&mut &bytes[..]).unwrap();
        assert_eq!(&result[0..4], b"GVAS");
        assert_eq!(result.len(), header.uncompressed_len as usize);
    }
}
