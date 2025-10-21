use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::{Context, Result};
use tempfile::NamedTempFile;

#[derive(Debug)]
struct StoredChunk {
    offset: u64,
    len: usize,
}

#[derive(Debug)]
pub struct ChunkStore {
    file: NamedTempFile,
    index: HashMap<String, StoredChunk>,
    order: Vec<String>,
}

impl ChunkStore {
    pub fn new() -> Result<Self> {
        let file = NamedTempFile::new().context("failed to create temporary chunk store")?;
        Ok(Self {
            file,
            index: HashMap::new(),
            order: Vec::new(),
        })
    }

    pub fn insert(&mut self, hash: String, content: String) -> Result<bool> {
        if self.index.contains_key(&hash) {
            return Ok(false);
        }

        let bytes = content.as_bytes();
        let offset = self
            .file
            .as_file_mut()
            .seek(SeekFrom::End(0))
            .context("failed to seek to end of chunk store")?;
        self.file
            .as_file_mut()
            .write_all(bytes)
            .context("failed to write chunk content to store")?;

        self.index.insert(
            hash.clone(),
            StoredChunk {
                offset,
                len: bytes.len(),
            },
        );
        self.order.push(hash);
        Ok(true)
    }

    pub fn hashes(&self) -> &[String] {
        &self.order
    }

    pub fn read_chunk(&self, hash: &str) -> Result<Option<String>> {
        let info = match self.index.get(hash) {
            Some(info) => info,
            None => return Ok(None),
        };

        let mut file = self
            .file
            .as_file()
            .try_clone()
            .context("failed to clone chunk store file handle")?;

        file.seek(SeekFrom::Start(info.offset))
            .context("failed to seek to chunk offset")?;

        let mut buf = vec![0u8; info.len];
        file.read_exact(&mut buf)
            .context("failed to read chunk content from store")?;

        let text = String::from_utf8(buf).context("chunk content is not valid UTF-8")?;
        Ok(Some(text))
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }
}
