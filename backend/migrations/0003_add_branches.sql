CREATE TABLE branches (
    repository TEXT NOT NULL,
    branch TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    indexed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (repository, branch)
);

CREATE INDEX idx_branches_repo_commit ON branches (repository, commit_sha);
