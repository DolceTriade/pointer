use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tempfile::{Builder, NamedTempFile, TempPath};

use crate::chunk_store::ChunkStore;

const NEWLINE: &[u8] = b"\n";
const BUFFER_FLUSH_BYTES: usize = 512 * 1024;

// Represents a file's metadata. Content is stored separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentBlob {
    pub hash: String,
    pub language: Option<String>,
    pub byte_len: i64,
    pub line_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolRecord {
    pub content_hash: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceRecord {
    pub content_hash: String,
    pub namespace: Option<String>,
    pub name: String,
    pub fully_qualified: String,
    pub kind: Option<String>,
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolNamespaceRecord {
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilePointer {
    pub repository: String,
    pub commit_sha: String,
    pub file_path: String,
    pub content_hash: String,
}

// A report containing all the metadata extracted from a repository.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexReport {
    pub content_blobs: Vec<ContentBlob>,
    pub symbol_records: Vec<SymbolRecord>,
    pub file_pointers: Vec<FilePointer>,
    pub reference_records: Vec<ReferenceRecord>,
    pub branches: Vec<BranchHead>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchSnapshotPolicy {
    pub interval_seconds: u64,
    pub keep_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchPolicy {
    pub latest_keep_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_live: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshot_policies: Vec<BranchSnapshotPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchHead {
    pub repository: String,
    pub branch: String,
    pub commit_sha: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<BranchPolicy>,
}

// A unique, deduplicated chunk of text content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UniqueChunk {
    pub chunk_hash: String,
    pub text_content: String,
}

// Maps a file's content hash to a sequence of chunks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMapping {
    pub content_hash: String,
    pub chunk_hash: String,
    pub chunk_index: usize,
    pub chunk_line_count: i32,
}

#[derive(Debug)]
struct WriterState {
    file: NamedTempFile,
    buffer: Vec<u8>,
}

impl WriterState {
    fn new_in(dir: &Path) -> Result<Self> {
        Ok(Self {
            file: Builder::new()
                .prefix("pointer-records")
                .tempfile_in(dir)
                .context("failed to create temp file")?,
            buffer: Vec::with_capacity(BUFFER_FLUSH_BYTES),
        })
    }

    fn append_json<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        serde_json::to_writer(&mut self.buffer, value).context("failed to serialize record")?;
        self.buffer.extend_from_slice(NEWLINE);

        if self.buffer.len() >= BUFFER_FLUSH_BYTES {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        self.file
            .as_file_mut()
            .write_all(&self.buffer)
            .context("failed to write buffered records")?;
        self.buffer.clear();
        Ok(())
    }
}

#[derive(Debug)]
struct RecordWriterInner<T> {
    state: Mutex<WriterState>,
    count: AtomicUsize,
    _marker: PhantomData<T>,
}

impl<T> RecordWriterInner<T>
where
    T: Serialize,
{
    fn new_in(dir: &Path) -> Result<Self> {
        Ok(Self {
            state: Mutex::new(WriterState::new_in(dir)?),
            count: AtomicUsize::new(0),
            _marker: PhantomData,
        })
    }

    fn append(&self, value: &T) -> Result<()> {
        let mut state = self.state.lock().expect("record writer mutex poisoned");
        state.append_json(value)?;
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn finish(self) -> Result<RecordStore<T>> {
        let count = self.count.load(Ordering::Relaxed);
        let mut state = self
            .state
            .into_inner()
            .expect("record writer mutex poisoned");
        state.flush()?;
        let path = state.file.into_temp_path();
        Ok(RecordStore {
            path,
            count,
            _marker: PhantomData,
        })
    }
}

#[derive(Clone)]
pub struct RecordWriter<T> {
    inner: Arc<RecordWriterInner<T>>,
}

impl<T> RecordWriter<T>
where
    T: Serialize,
{
    pub fn new() -> Result<Self> {
        let temp_dir = std::env::temp_dir();
        Self::new_in(temp_dir.as_path())
    }

    pub fn new_in(dir: &Path) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RecordWriterInner::new_in(dir)?),
        })
    }

    pub fn append(&self, value: &T) -> Result<()> {
        self.inner.append(value)
    }

    pub fn into_store(self) -> Result<RecordStore<T>> {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner.finish(),
            Err(_) => Err(anyhow!(
                "attempted to finish record writer with outstanding references"
            )),
        }
    }
}

pub struct RecordStore<T> {
    path: TempPath,
    count: usize,
    _marker: PhantomData<T>,
}

impl<T> RecordStore<T> {
    pub fn count(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn file(&self) -> Result<File> {
        File::open(self.path())
            .with_context(|| format!("failed to open record store {}", self.path().display()))
    }

    pub fn path(&self) -> &Path {
        self.path.as_ref()
    }

    pub fn stream(&self) -> Result<RecordStream<T>> {
        Ok(RecordStream {
            reader: BufReader::new(self.file()?),
            buffer: String::new(),
            _marker: PhantomData,
        })
    }

    pub fn for_each_raw_line<F>(&self, mut func: F) -> Result<()>
    where
        F: FnMut(&str) -> Result<()>,
    {
        let file = self.file()?;
        let mut reader = BufReader::new(file);
        let mut buffer = String::new();

        loop {
            buffer.clear();
            let read = reader
                .read_line(&mut buffer)
                .context("failed to read record line")?;
            if read == 0 {
                break;
            }

            let line = buffer.trim_end_matches(['\n', '\r']);
            if line.is_empty() {
                continue;
            }

            func(line)?;
        }

        Ok(())
    }

    pub fn write_json_array<W>(&self, mut writer: W) -> Result<()>
    where
        W: Write,
    {
        let mut first = true;
        self.for_each_raw_line(|line| {
            if !first {
                writer
                    .write_all(b",")
                    .context("failed to write record separator")?;
            } else {
                first = false;
            }

            writer
                .write_all(line.as_bytes())
                .context("failed to write record data")?;
            Ok(())
        })?;

        Ok(())
    }
}

pub struct RecordStream<T> {
    reader: BufReader<File>,
    buffer: String,
    _marker: PhantomData<T>,
}

impl<T> RecordStream<T>
where
    T: DeserializeOwned,
{
    pub fn next_batch(&mut self, batch_size: usize) -> Result<Vec<T>> {
        if batch_size == 0 {
            return Ok(Vec::new());
        }

        let mut batch = Vec::with_capacity(batch_size.min(1024));
        while batch.len() < batch_size {
            self.buffer.clear();
            let read = self
                .reader
                .read_line(&mut self.buffer)
                .context("failed to read record line")?;
            if read == 0 {
                break;
            }

            let line = self.buffer.trim_end_matches(['\n', '\r']);
            if line.is_empty() {
                continue;
            }

            let item = serde_json::from_str(line).context("failed to parse record")?;
            batch.push(item);
        }

        Ok(batch)
    }
}

// The final output of the indexer.
pub struct IndexArtifacts {
    content_blobs: RecordStore<ContentBlob>,
    symbol_records: RecordStore<SymbolRecord>,
    symbol_namespaces: RecordStore<SymbolNamespaceRecord>,
    file_pointers: RecordStore<FilePointer>,
    reference_records: RecordStore<ReferenceRecord>,
    chunk_mappings: RecordStore<ChunkMapping>,
    chunk_store: ChunkStore,
    pub branches: Vec<BranchHead>,
    scratch_dir: PathBuf,
}

impl IndexArtifacts {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        content_blobs: RecordStore<ContentBlob>,
        symbol_records: RecordStore<SymbolRecord>,
        symbol_namespaces: RecordStore<SymbolNamespaceRecord>,
        file_pointers: RecordStore<FilePointer>,
        reference_records: RecordStore<ReferenceRecord>,
        chunk_mappings: RecordStore<ChunkMapping>,
        chunk_store: ChunkStore,
        branches: Vec<BranchHead>,
        scratch_dir: PathBuf,
    ) -> Self {
        Self {
            content_blobs,
            symbol_records,
            symbol_namespaces,
            file_pointers,
            reference_records,
            chunk_mappings,
            chunk_store,
            branches,
            scratch_dir,
        }
    }

    pub fn chunk_hashes(&self) -> &[String] {
        self.chunk_store.hashes()
    }

    pub fn chunk_count(&self) -> usize {
        self.chunk_store.len()
    }

    pub fn read_chunk(&self, hash: &str) -> Result<String> {
        match self.chunk_store.read_chunk(hash)? {
            Some(text) => Ok(text),
            None => Err(anyhow!("missing chunk content for hash {hash}")),
        }
    }

    pub fn content_blobs_stream(&self) -> Result<RecordStream<ContentBlob>> {
        self.content_blobs.stream()
    }

    pub fn symbol_records_stream(&self) -> Result<RecordStream<SymbolRecord>> {
        self.symbol_records.stream()
    }

    pub fn symbol_namespace_stream(&self) -> Result<RecordStream<SymbolNamespaceRecord>> {
        self.symbol_namespaces.stream()
    }

    pub fn file_pointers_stream(&self) -> Result<RecordStream<FilePointer>> {
        self.file_pointers.stream()
    }

    pub fn reference_records_stream(&self) -> Result<RecordStream<ReferenceRecord>> {
        self.reference_records.stream()
    }

    pub fn content_blob_count(&self) -> usize {
        self.content_blobs.count()
    }

    pub fn symbol_record_count(&self) -> usize {
        self.symbol_records.count()
    }

    pub fn symbol_namespace_count(&self) -> usize {
        self.symbol_namespaces.count()
    }

    pub fn file_pointer_count(&self) -> usize {
        self.file_pointers.count()
    }

    pub fn reference_record_count(&self) -> usize {
        self.reference_records.count()
    }

    pub fn chunk_mapping_count(&self) -> usize {
        self.chunk_mappings.count()
    }

    pub fn chunk_mappings_stream(&self) -> Result<RecordStream<ChunkMapping>> {
        self.chunk_mappings.stream()
    }

    pub fn content_blobs_path(&self) -> &Path {
        self.content_blobs.path()
    }

    pub fn symbol_records_path(&self) -> &Path {
        self.symbol_records.path()
    }

    pub fn symbol_namespaces_path(&self) -> &Path {
        self.symbol_namespaces.path()
    }

    pub fn file_pointers_path(&self) -> &Path {
        self.file_pointers.path()
    }

    pub fn reference_records_path(&self) -> &Path {
        self.reference_records.path()
    }

    pub fn chunk_mappings_path(&self) -> &Path {
        self.chunk_mappings.path()
    }

    pub fn scratch_dir(&self) -> &Path {
        &self.scratch_dir
    }

    pub fn write_manifest_ndjson<W: Write>(&self, mut writer: W) -> Result<()> {
        fn write_line<W: Write>(writer: &mut W, section: &str, payload: &str) -> Result<()> {
            writer
                .write_all(b"{\"section\":\"")
                .context("failed to write manifest header")?;
            writer
                .write_all(section.as_bytes())
                .context("failed to write manifest section")?;
            writer
                .write_all(b"\",\"payload\":")
                .context("failed to write manifest payload header")?;
            writer
                .write_all(payload.as_bytes())
                .context("failed to write manifest payload")?;
            writer
                .write_all(b"}\n")
                .context("failed to finalize manifest row")?;
            Ok(())
        }

        self.content_blobs
            .for_each_raw_line(|line| write_line(&mut writer, "content_blob", line))?;
        self.file_pointers
            .for_each_raw_line(|line| write_line(&mut writer, "file_pointer", line))?;
        self.symbol_records
            .for_each_raw_line(|line| write_line(&mut writer, "symbol_record", line))?;
        self.symbol_namespaces
            .for_each_raw_line(|line| write_line(&mut writer, "symbol_namespace", line))?;
        self.reference_records
            .for_each_raw_line(|line| write_line(&mut writer, "reference_record", line))?;

        for branch in &self.branches {
            let mut buf = Vec::new();
            serde_json::to_writer(&mut buf, branch).context("failed to serialize branch head")?;
            let payload =
                String::from_utf8(buf).context("serialized branch head was not valid UTF-8")?;
            write_line(&mut writer, "branch_head", &payload)?;
        }

        Ok(())
    }

    pub fn write_content_blobs_array<W: Write>(&self, mut writer: W) -> Result<()> {
        self.content_blobs
            .write_json_array(&mut writer)
            .context("failed to write content blobs")
    }

    pub fn write_symbol_records_array<W: Write>(&self, mut writer: W) -> Result<()> {
        self.symbol_records
            .write_json_array(&mut writer)
            .context("failed to write symbol records")
    }

    pub fn write_symbol_namespaces_array<W: Write>(&self, mut writer: W) -> Result<()> {
        self.symbol_namespaces
            .write_json_array(&mut writer)
            .context("failed to write symbol namespaces")
    }

    pub fn write_file_pointers_array<W: Write>(&self, mut writer: W) -> Result<()> {
        self.file_pointers
            .write_json_array(&mut writer)
            .context("failed to write file pointers")
    }

    pub fn write_reference_records_array<W: Write>(&self, mut writer: W) -> Result<()> {
        self.reference_records
            .write_json_array(&mut writer)
            .context("failed to write reference records")
    }
}
