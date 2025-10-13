use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;

use anyhow::{Context, Result};

use crate::models::IndexArtifacts;

pub fn write_report(output_dir: &Path, artifacts: &IndexArtifacts) -> Result<()> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    let report = &artifacts.report;

    write_json(output_dir.join("content_blobs.json"), &report.content_blobs)?;
    write_json(
        output_dir.join("symbol_records.json"),
        &report.symbol_records,
    )?;
    write_json(output_dir.join("file_pointers.json"), &report.file_pointers)?;
    write_json(
        output_dir.join("reference_records.json"),
        &report.reference_records,
    )?;


    Ok(())
}

fn write_json<T: serde::Serialize>(path: impl AsRef<Path>, value: &T) -> Result<()> {
    let path = path.as_ref();
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, value)
        .with_context(|| format!("failed to serialize {}", path.display()))
}
