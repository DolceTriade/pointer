use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};

use crate::models::IndexArtifacts;

pub fn write_report(output_dir: &Path, artifacts: &IndexArtifacts) -> Result<()> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    write_array_file(output_dir.join("content_blobs.json"), |writer| {
        artifacts.write_content_blobs_array(writer)
    })?;
    write_array_file(output_dir.join("symbol_records.json"), |writer| {
        artifacts.write_symbol_records_array(writer)
    })?;
    write_array_file(output_dir.join("symbol_namespaces.json"), |writer| {
        artifacts.write_symbol_namespaces_array(writer)
    })?;
    write_array_file(output_dir.join("file_pointers.json"), |writer| {
        artifacts.write_file_pointers_array(writer)
    })?;
    write_array_file(output_dir.join("reference_records.json"), |writer| {
        artifacts.write_reference_records_array(writer)
    })?;

    Ok(())
}

fn write_array_file<F>(path: impl AsRef<Path>, mut write_fn: F) -> Result<()>
where
    F: FnMut(&mut dyn Write) -> Result<()>,
{
    let path = path.as_ref();
    let file = File::create(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);

    writer
        .write_all(b"[")
        .with_context(|| format!("failed to start {}", path.display()))?;
    write_fn(&mut writer)?;
    writer
        .write_all(b"]")
        .with_context(|| format!("failed to finalize {}", path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", path.display()))?;

    Ok(())
}
