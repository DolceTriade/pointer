use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub repo_path: PathBuf,
    pub repository: String,
    pub branch: Option<String>,
    pub commit: String,
    pub output_dir: PathBuf,
}

impl IndexerConfig {
    pub fn new(
        repo_path: PathBuf,
        repository: String,
        branch: Option<String>,
        commit: String,
        output_dir: PathBuf,
    ) -> Self {
        Self {
            repo_path,
            repository,
            branch,
            commit,
            output_dir,
        }
    }
}
