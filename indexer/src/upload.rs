use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::time::Duration;

use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use reqwest::blocking::{Client, Response};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use tracing::info;
use zstd::stream::Encoder;

use crate::models::{ChunkMapping, IndexArtifacts, UniqueChunk};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
const MANIFEST_SHARD_RECORD_LIMIT: usize = 50_000;
const MANIFEST_SHARD_BYTE_LIMIT: usize = 4 * 1024 * 1024;

pub fn upload_index(url: &str, api_key: Option<&str>, artifacts: &IndexArtifacts) -> Result<()> {
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("failed to build HTTP client")?;

    let endpoints = Endpoints::new(url);

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
    upload_manifest_shards(&client, &endpoints, api_key, artifacts)?;
    info!("manifest shards uploaded");

    Ok(())
}

struct Endpoints {
    blobs_upload: String,
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
            chunks_need: format!("{}/chunks/need", trimmed),
            chunks_upload: format!("{}/chunks/upload", trimmed),
            mappings_upload: format!("{}/mappings/upload", trimmed),
            manifest_shard: format!("{}/manifest/shard", trimmed),
        }
    }
}

fn upload_content_blobs(
    client: &Client,
    endpoints: &Endpoints,
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

    let mut stream = artifacts.content_blobs_stream()?;
    loop {
        let batch = stream.next_batch(1000)?;
        if batch.is_empty() {
            break;
        }

        let payload = ContentBlobUploadRequest {
            blobs: batch,
        };
        post_json(client, &endpoints.blobs_upload, api_key, &payload)?;
    }
    info!("content blob metadata uploaded");
    Ok(())
}

fn request_needed_chunks(
    client: &Client,
    endpoints: &Endpoints,
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

fn upload_unique_chunks(
    client: &Client,
    endpoints: &Endpoints,
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

        let payload = UniqueChunkUploadRequest { chunks };
        post_json(client, &endpoints.chunks_upload, api_key, &payload)?;
    }
    info!("unique chunk content uploaded");

    Ok(())
}

fn upload_chunk_mappings(
    client: &Client,
    endpoints: &Endpoints,
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

    let mut stream = artifacts.chunk_mappings_stream()?;
    loop {
        let batch = stream.next_batch(1000)?;
        if batch.is_empty() {
            break;
        }

        let payload = ChunkMappingUploadRequest {
            mappings: batch,
        };
        post_json(client, &endpoints.mappings_upload, api_key, &payload)?;
    }
    info!("chunk mappings uploaded");
    Ok(())
}

fn upload_manifest_shards(
    client: &Client,
    endpoints: &Endpoints,
    api_key: Option<&str>,
    artifacts: &IndexArtifacts,
) -> Result<()> {
    upload_record_store_shards(
        client,
        endpoints,
        api_key,
        artifacts.file_pointers_path(),
        "file_pointer",
    )?;

    upload_record_store_shards(
        client,
        endpoints,
        api_key,
        artifacts.symbol_records_path(),
        "symbol_record",
    )?;

    upload_record_store_shards(
        client,
        endpoints,
        api_key,
        artifacts.reference_records_path(),
        "reference_record",
    )?;

    upload_branch_heads(client, endpoints, api_key, &artifacts.branches)?;

    Ok(())
}

fn upload_record_store_shards(
    client: &Client,
    endpoints: &Endpoints,
    api_key: Option<&str>,
    path: &std::path::Path,
    section: &str,
) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let file = File::open(path)
        .with_context(|| format!("failed to open record store {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut shard_buffer = Vec::with_capacity(MANIFEST_SHARD_BYTE_LIMIT + 1024);
    let mut line = String::new();
    let mut record_count: usize = 0;
    let mut shard_index: u64 = 0;

    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .context("failed to read record shard line")?;
        if read == 0 {
            if !shard_buffer.is_empty() {
                send_manifest_shard(
                    client,
                    endpoints,
                    api_key,
                    section,
                    shard_index,
                    &shard_buffer,
                )?;
                shard_buffer.clear();
            }
            break;
        }

        if line.trim().is_empty() {
            continue;
        }

        shard_buffer.extend_from_slice(line.as_bytes());
        record_count += 1;

        if record_count >= MANIFEST_SHARD_RECORD_LIMIT
            || shard_buffer.len() >= MANIFEST_SHARD_BYTE_LIMIT
        {
            send_manifest_shard(
                client,
                endpoints,
                api_key,
                section,
                shard_index,
                &shard_buffer,
            )?;
            shard_buffer.clear();
            record_count = 0;
            shard_index += 1;
        }
    }

    Ok(())
}

fn upload_branch_heads(
    client: &Client,
    endpoints: &Endpoints,
    api_key: Option<&str>,
    branches: &[crate::models::BranchHead],
) -> Result<()> {
    if branches.is_empty() {
        return Ok(());
    }

    let mut buffer = Vec::with_capacity(branches.len() * 256);
    for branch in branches {
        serde_json::to_writer(&mut buffer, branch)
            .context("failed to serialize branch head")?;
        buffer.push(b'\n');
    }

    send_manifest_shard(client, endpoints, api_key, "branch_head", 0, &buffer)
}

fn send_manifest_shard(
    client: &Client,
    endpoints: &Endpoints,
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

    post_json(client, &endpoints.manifest_shard, api_key, &payload)?;
    info!(section = section, shard = shard_index, "uploaded manifest shard");
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
