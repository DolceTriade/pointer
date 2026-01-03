use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use crossbeam_channel::bounded;
use reqwest::blocking::{Client, Response};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use tracing::info;
use zstd::stream::Encoder;

use crate::models::{ChunkMapping, IndexArtifacts, ReferenceRecord, SymbolRecord, UniqueChunk};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
const MANIFEST_SHARD_RECORD_LIMIT: usize = 50_000;
const MANIFEST_SHARD_BYTE_LIMIT: usize = 4 * 1024 * 1024;
const UPLOAD_PARALLELISM: usize = 4;

const PROGRESS_STEP_PERCENT: u8 = 10;

#[derive(Debug)]
struct ManifestShard {
    index: u64,
    data: Vec<u8>,
}

pub fn upload_index(url: &str, api_key: Option<&str>, artifacts: &IndexArtifacts) -> Result<()> {
    upload_index_with_options(url, api_key, artifacts, &UploadOptions::default())
}

pub struct UploadOptions {
    pub incremental_symbols: bool,
}

impl Default for UploadOptions {
    fn default() -> Self {
        Self {
            incremental_symbols: true,
        }
    }
}

pub fn upload_index_with_options(
    url: &str,
    api_key: Option<&str>,
    artifacts: &IndexArtifacts,
    options: &UploadOptions,
) -> Result<()> {
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("failed to build HTTP client")?;

    let endpoints = Arc::new(Endpoints::new(url));

    let needed_hashes = if options.incremental_symbols {
        let content_hashes = collect_content_hashes(artifacts)?;
        Some(request_needed_content_hashes(
            &client,
            &endpoints,
            api_key,
            &content_hashes,
        )?)
    } else {
        None
    };

    // 1. Upload all content blob metadata
    upload_content_blobs(&client, &endpoints, api_key, artifacts)?;

    // 2. Check which unique chunks the server needs
    let chunk_hashes = artifacts.chunk_hashes().to_vec();
    let needed_chunk_hashes = request_needed_chunks(&client, &endpoints, api_key, &chunk_hashes)?;

    // 3. Upload the content of the needed chunks
    if !needed_chunk_hashes.is_empty() {
        upload_unique_chunks(
            &client,
            &endpoints,
            api_key,
            artifacts,
            &needed_chunk_hashes,
        )?;
    } else {
        info!("no new chunk content to upload");
    }

    // 4. Upload the mappings for how chunks belong to files
    upload_chunk_mappings(&client, &endpoints, api_key, artifacts)?;

    // 5. Upload manifest shards per section
    info!("uploading manifest shards");
    upload_manifest_shards(
        &client,
        &endpoints,
        api_key,
        artifacts,
        needed_hashes.as_ref(),
    )?;

    Ok(())
}

#[derive(Clone)]
struct Endpoints {
    blobs_upload: String,
    blobs_need: String,
    chunks_need: String,
    chunks_upload: String,
    mappings_upload: String,
    manifest_shard: String,
}

impl Endpoints {
    fn new(base: &str) -> Self {
        let trimmed = base.trim_end_matches('/');
        Self {
            blobs_upload: format!("{}/blobs/upload", trimmed),
            blobs_need: format!("{}/blobs/need", trimmed),
            chunks_need: format!("{}/chunks/need", trimmed),
            chunks_upload: format!("{}/chunks/upload", trimmed),
            mappings_upload: format!("{}/mappings/upload", trimmed),
            manifest_shard: format!("{}/manifest/shard", trimmed),
        }
    }
}

fn upload_content_blobs(
    client: &Client,
    endpoints: &Arc<Endpoints>,
    api_key: Option<&str>,
    artifacts: &IndexArtifacts,
) -> Result<()> {
    if artifacts.content_blob_count() == 0 {
        return Ok(());
    }

    info!(
        count = artifacts.content_blob_count(),
        "uploading content blob metadata"
    );

    let api_key_owned = Arc::new(api_key.map(|s| s.to_string()));
    let endpoints = Arc::clone(endpoints);
    let client = Arc::new(client.clone());

    let mut stream = artifacts.content_blobs_stream()?;
    let (tx, rx) =
        bounded::<Vec<crate::models::ContentBlob>>(UPLOAD_PARALLELISM.saturating_mul(2).max(1));

    let worker_func = Arc::new(
        move |batch: Vec<crate::models::ContentBlob>| -> Result<()> {
            let payload = ContentBlobUploadRequest { blobs: batch };
            let api = api_key_owned.as_ref().as_ref().map(|s| s.as_str());
            post_json(client.as_ref(), &endpoints.blobs_upload, api, &payload)?;
            Ok(())
        },
    );
    let workers = spawn_workers(rx, worker_func);

    let mut processed = 0usize;
    let mut last_percent = 0u8;
    loop {
        let batch = stream.next_batch(1000)?;
        if batch.is_empty() {
            break;
        }

        processed = processed.saturating_add(batch.len());
        maybe_log_progress(
            "content blobs",
            processed,
            artifacts.content_blob_count(),
            &mut last_percent,
        );

        tx.send(batch)
            .map_err(|_| anyhow!("content blob upload worker dropped"))?;
    }
    drop(tx);

    workers.wait()?;

    info!("content blob metadata uploaded");
    Ok(())
}

fn request_needed_chunks(
    client: &Client,
    endpoints: &Arc<Endpoints>,
    api_key: Option<&str>,
    chunk_hashes: &[String],
) -> Result<HashSet<String>> {
    if chunk_hashes.is_empty() {
        return Ok(HashSet::new());
    }
    info!(count = chunk_hashes.len(), "checking for needed chunks");

    let request = ChunkNeedRequest {
        hashes: chunk_hashes.to_vec(),
    };

    let response: ChunkNeedResponse = post_json(client, &endpoints.chunks_need, api_key, &request)?
        .json()
        .context("failed to deserialize chunk need response")?;

    info!(needed = response.missing.len(), "found chunks to upload");
    Ok(response.missing.into_iter().collect())
}

fn request_needed_content_hashes(
    client: &Client,
    endpoints: &Arc<Endpoints>,
    api_key: Option<&str>,
    content_hashes: &[String],
) -> Result<HashSet<String>> {
    if content_hashes.is_empty() {
        return Ok(HashSet::new());
    }

    info!(
        count = content_hashes.len(),
        "checking for needed content hashes"
    );

    let request = ContentNeedRequest {
        hashes: content_hashes.to_vec(),
    };

    let response: ContentNeedResponse =
        post_json(client, &endpoints.blobs_need, api_key, &request)?
            .json()
            .context("failed to deserialize content need response")?;

    info!(
        needed = response.missing.len(),
        "found content hashes to upload"
    );
    Ok(response.missing.into_iter().collect())
}

fn collect_content_hashes(artifacts: &IndexArtifacts) -> Result<Vec<String>> {
    let mut stream = artifacts.content_blobs_stream()?;
    let mut hashes = Vec::new();
    loop {
        let batch = stream.next_batch(1000)?;
        if batch.is_empty() {
            break;
        }
        hashes.extend(batch.into_iter().map(|blob| blob.hash));
    }
    Ok(hashes)
}

fn upload_unique_chunks(
    client: &Client,
    endpoints: &Arc<Endpoints>,
    api_key: Option<&str>,
    artifacts: &IndexArtifacts,
    needed_hashes: &HashSet<String>,
) -> Result<()> {
    let needed_chunks: Vec<&String> = artifacts
        .chunk_hashes()
        .iter()
        .filter(|hash| needed_hashes.contains(hash.as_str()))
        .collect();

    if needed_chunks.is_empty() {
        return Ok(());
    }

    info!(
        count = needed_chunks.len(),
        "uploading unique chunk content"
    );
    let api_key_owned = Arc::new(api_key.map(|s| s.to_string()));
    let endpoints = Arc::clone(endpoints);
    let client = Arc::new(client.clone());

    let (tx, rx) = bounded::<Vec<UniqueChunk>>(UPLOAD_PARALLELISM.saturating_mul(2).max(1));

    let worker_func = Arc::new(move |chunks: Vec<UniqueChunk>| -> Result<()> {
        let payload = UniqueChunkUploadRequest { chunks };
        let api = api_key_owned.as_ref().as_ref().map(|s| s.as_str());
        post_json(client.as_ref(), &endpoints.chunks_upload, api, &payload)?;
        Ok(())
    });
    let workers = spawn_workers(rx, worker_func);
    let mut processed = 0usize;
    let mut last_percent = 0u8;
    for batch in needed_chunks.chunks(100) {
        let mut chunks = Vec::with_capacity(batch.len());
        for hash in batch {
            let text_content = artifacts
                .read_chunk(hash)
                .with_context(|| format!("failed to read chunk content for {}", hash))?;
            chunks.push(UniqueChunk {
                chunk_hash: (*hash).clone(),
                text_content,
            });
        }

        processed = processed.saturating_add(chunks.len());
        maybe_log_progress(
            "unique chunks",
            processed,
            needed_chunks.len(),
            &mut last_percent,
        );

        tx.send(chunks)
            .map_err(|_| anyhow!("unique chunk upload worker dropped"))?;
    }
    drop(tx);

    workers.wait()?;
    info!("unique chunk content uploaded");

    Ok(())
}

fn upload_chunk_mappings(
    client: &Client,
    endpoints: &Arc<Endpoints>,
    api_key: Option<&str>,
    artifacts: &IndexArtifacts,
) -> Result<()> {
    if artifacts.chunk_mapping_count() == 0 {
        return Ok(());
    }

    info!(
        count = artifacts.chunk_mapping_count(),
        "uploading chunk mappings"
    );

    let api_key_owned = Arc::new(api_key.map(|s| s.to_string()));
    let endpoints = Arc::clone(endpoints);
    let client = Arc::new(client.clone());

    let mut stream = artifacts.chunk_mappings_stream()?;
    let (tx, rx) = bounded::<Vec<ChunkMapping>>(UPLOAD_PARALLELISM.saturating_mul(2).max(1));

    let worker_func = Arc::new(move |mappings: Vec<ChunkMapping>| -> Result<()> {
        let payload = ChunkMappingUploadRequest { mappings };
        let api = api_key_owned.as_ref().as_ref().map(|s| s.as_str());
        post_json(client.as_ref(), &endpoints.mappings_upload, api, &payload)?;
        Ok(())
    });
    let workers = spawn_workers(rx, worker_func);
    let mut processed = 0usize;
    let mut last_percent = 0u8;
    loop {
        let batch = stream.next_batch(1000)?;
        if batch.is_empty() {
            break;
        }

        processed = processed.saturating_add(batch.len());
        maybe_log_progress(
            "chunk mappings",
            processed,
            artifacts.chunk_mapping_count(),
            &mut last_percent,
        );

        tx.send(batch)
            .map_err(|_| anyhow!("chunk mapping upload worker dropped"))?;
    }
    drop(tx);

    workers.wait()?;
    info!("chunk mappings uploaded");
    Ok(())
}

fn upload_manifest_shards(
    client: &Client,
    endpoints: &Arc<Endpoints>,
    api_key: Option<&str>,
    artifacts: &IndexArtifacts,
    needed_hashes: Option<&HashSet<String>>,
) -> Result<()> {
    upload_record_store_shards(
        client,
        endpoints,
        api_key,
        artifacts.file_pointers_path(),
        "file_pointer",
        artifacts.file_pointer_count(),
    )?;

    if let Some(needed) = needed_hashes {
        if !needed.is_empty() {
            upload_filtered_record_store_shards(
                client,
                endpoints,
                api_key,
                artifacts.symbol_records_path(),
                "symbol_record",
                Some(artifacts.symbol_record_count()),
                |line| {
                    let record: SymbolRecord =
                        serde_json::from_str(line).context("failed to parse symbol record")?;
                    Ok(needed.contains(&record.content_hash))
                },
            )?;
        } else {
            info!("no new content hashes; skipping symbol record upload");
        }
    } else {
        upload_record_store_shards(
            client,
            endpoints,
            api_key,
            artifacts.symbol_records_path(),
            "symbol_record",
            artifacts.symbol_record_count(),
        )?;
    }

    upload_record_store_shards(
        client,
        endpoints,
        api_key,
        artifacts.symbol_namespaces_path(),
        "symbol_namespace",
        artifacts.symbol_namespace_count(),
    )?;

    if let Some(needed) = needed_hashes {
        if !needed.is_empty() {
            upload_filtered_record_store_shards(
                client,
                endpoints,
                api_key,
                artifacts.reference_records_path(),
                "reference_record",
                Some(artifacts.reference_record_count()),
                |line| {
                    let record: ReferenceRecord =
                        serde_json::from_str(line).context("failed to parse reference record")?;
                    Ok(needed.contains(&record.content_hash))
                },
            )?;
        } else {
            info!("no new content hashes; skipping reference record upload");
        }
    } else {
        upload_record_store_shards(
            client,
            endpoints,
            api_key,
            artifacts.reference_records_path(),
            "reference_record",
            artifacts.reference_record_count(),
        )?;
    }

    upload_branch_heads(client, endpoints, api_key, &artifacts.branches)?;

    info!(
        namespaces = artifacts.symbol_namespace_count(),
        references = artifacts.reference_record_count(),
        "manifest shards uploaded"
    );

    Ok(())
}

fn upload_record_store_shards(
    client: &Client,
    endpoints: &Arc<Endpoints>,
    api_key: Option<&str>,
    path: &std::path::Path,
    section: &str,
    total_records: usize,
) -> Result<()> {
    upload_filtered_record_store_shards(
        client,
        endpoints,
        api_key,
        path,
        section,
        Some(total_records),
        |_| Ok(true),
    )
}

fn upload_filtered_record_store_shards<F>(
    client: &Client,
    endpoints: &Arc<Endpoints>,
    api_key: Option<&str>,
    path: &std::path::Path,
    section: &str,
    total_records: Option<usize>,
    mut should_include: F,
) -> Result<()>
where
    F: FnMut(&str) -> Result<bool>,
{
    if !path.exists() {
        return Ok(());
    }

    let file = File::open(path)
        .with_context(|| format!("failed to open record store {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let api_key_owned = Arc::new(api_key.map(|s| s.to_string()));
    let endpoints = Arc::clone(endpoints);
    let client = Arc::new(client.clone());
    let section_owned = Arc::new(section.to_string());

    let (tx, rx) = bounded::<ManifestShard>(UPLOAD_PARALLELISM.saturating_mul(2).max(1));
    let worker_func = Arc::new(move |shard: ManifestShard| -> Result<()> {
        let api = api_key_owned.as_ref().as_ref().map(|s| s.as_str());
        send_manifest_shard(
            client.as_ref(),
            Arc::clone(&endpoints),
            api,
            section_owned.as_str(),
            shard.index,
            &shard.data,
        )?;
        Ok(())
    });
    let workers = spawn_workers(rx, worker_func);

    let mut line = String::new();
    let mut shard_index: u64 = 0;
    let mut eof = false;
    let mut processed_records: usize = 0;
    let mut last_percent = 0u8;

    while !eof {
        let mut shard_data = Vec::with_capacity(MANIFEST_SHARD_BYTE_LIMIT + 1024);
        let mut records: usize = 0;

        while records < MANIFEST_SHARD_RECORD_LIMIT && shard_data.len() < MANIFEST_SHARD_BYTE_LIMIT
        {
            line.clear();
            let read = reader
                .read_line(&mut line)
                .context("failed to read record shard line")?;
            if read == 0 {
                eof = true;
                break;
            }

            if line.trim().is_empty() {
                continue;
            }

            processed_records = processed_records.saturating_add(1);
            if let Some(total) = total_records {
                maybe_log_progress(section, processed_records, total, &mut last_percent);
            }

            if !should_include(line.trim_end_matches(['\n', '\r']))? {
                continue;
            }

            shard_data.extend_from_slice(line.as_bytes());
            records += 1;
        }

        if !shard_data.is_empty() {
            if tx
                .send(ManifestShard {
                    index: shard_index,
                    data: shard_data,
                })
                .is_err()
            {
                drop(tx);
                if let Err(err) = workers.wait() {
                    return Err(err);
                }
                return Err(anyhow!("manifest shard upload worker dropped"));
            }
            shard_index += 1;
        }
    }

    drop(tx);

    workers.wait()?;

    Ok(())
}

fn upload_branch_heads(
    client: &Client,
    endpoints: &Arc<Endpoints>,
    api_key: Option<&str>,
    branches: &[crate::models::BranchHead],
) -> Result<()> {
    if branches.is_empty() {
        return Ok(());
    }

    let mut buffer = Vec::with_capacity(branches.len() * 256);
    for branch in branches {
        serde_json::to_writer(&mut buffer, branch).context("failed to serialize branch head")?;
        buffer.push(b'\n');
    }

    send_manifest_shard(
        client,
        Arc::clone(endpoints),
        api_key,
        "branch_head",
        0,
        &buffer,
    )
}

fn send_manifest_shard(
    client: &Client,
    endpoints: Arc<Endpoints>,
    api_key: Option<&str>,
    section: &str,
    shard_index: u64,
    data: &[u8],
) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    let mut encoder = Encoder::new(Vec::new(), 0)?;
    encoder
        .write_all(data)
        .context("failed to compress manifest shard")?;
    let compressed = encoder
        .finish()
        .context("failed to finalize manifest shard compression")?;

    let payload = ManifestShardRequest {
        section: section.to_string(),
        shard_index,
        compressed: true,
        data: BASE64.encode(compressed),
    };

    post_json(client, &endpoints.manifest_shard, api_key, &payload).with_context(|| {
        format!(
            "manifest shard upload failed section={} shard={}",
            section, shard_index
        )
    })?;
    info!(
        section = section,
        shard = shard_index,
        "uploaded manifest shard"
    );
    Ok(())
}

fn post_json<T: Serialize>(
    client: &Client,
    url: &str,
    api_key: Option<&str>,
    body: &T,
) -> Result<Response> {
    let mut request = client
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .json(body);

    if let Some(key) = api_key {
        request = request.header(AUTHORIZATION, format!("Bearer {}", key));
    }

    let response = request
        .send()
        .with_context(|| format!("failed request to {}", url))?;
    if !response.status().is_success() {
        let status = response.status();
        let message = response.text().unwrap_or_default();
        anyhow::bail!("request to {url} failed with status {status}: {message}");
    }

    Ok(response)
}

struct WorkerGroup {
    handles: Vec<std::thread::JoinHandle<Result<()>>>,
}

impl WorkerGroup {
    fn wait(self) -> Result<()> {
        let mut first_err: Option<anyhow::Error> = None;
        for handle in self.handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
                Err(panic) => {
                    if first_err.is_none() {
                        first_err = Some(anyhow!("upload worker panicked: {:?}", panic));
                    }
                }
            }
        }

        if let Some(err) = first_err {
            Err(err)
        } else {
            Ok(())
        }
    }
}

fn spawn_workers<T, F>(receiver: crossbeam_channel::Receiver<T>, func: Arc<F>) -> WorkerGroup
where
    T: Send + 'static,
    F: Fn(T) -> Result<()> + Send + Sync + 'static,
{
    let worker_count = UPLOAD_PARALLELISM.max(1);
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let rx = receiver.clone();
        let func = Arc::clone(&func);
        handles.push(std::thread::spawn(move || -> Result<()> {
            while let Ok(item) = rx.recv() {
                func(item)?;
            }
            Ok(())
        }));
    }

    drop(receiver);

    WorkerGroup { handles }
}

fn maybe_log_progress(label: &str, processed: usize, total: usize, last_percent: &mut u8) {
    if total == 0 {
        return;
    }

    let mut percent = (processed.saturating_mul(100) / total) as u8;
    if percent > 100 {
        percent = 100;
    }
    let should_log =
        percent >= last_percent.saturating_add(PROGRESS_STEP_PERCENT) || percent == 100;

    if should_log {
        *last_percent = percent;
        info!(label = label, percent, processed, total, "upload progress");
    }
}

#[derive(Serialize)]
struct ContentBlobUploadRequest {
    blobs: Vec<crate::models::ContentBlob>,
}

#[derive(Serialize)]
struct ChunkNeedRequest {
    hashes: Vec<String>,
}

#[derive(Deserialize)]
struct ChunkNeedResponse {
    missing: Vec<String>,
}

#[derive(Serialize)]
struct ContentNeedRequest {
    hashes: Vec<String>,
}

#[derive(Deserialize)]
struct ContentNeedResponse {
    missing: Vec<String>,
}

#[derive(Serialize)]
struct UniqueChunkUploadRequest {
    chunks: Vec<UniqueChunk>,
}

#[derive(Serialize)]
struct ChunkMappingUploadRequest {
    mappings: Vec<ChunkMapping>,
}

#[derive(Serialize)]
struct ManifestShardRequest {
    section: String,
    shard_index: u64,
    compressed: bool,
    data: String,
}
