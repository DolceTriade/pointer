CREATE TABLE branch_policies (
    repository TEXT NOT NULL,
    branch TEXT NOT NULL,
    latest_keep_count INTEGER NOT NULL DEFAULT 1 CHECK (latest_keep_count >= 1),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (repository, branch)
);

CREATE TABLE branch_snapshot_policies (
    repository TEXT NOT NULL,
    branch TEXT NOT NULL,
    interval_seconds BIGINT NOT NULL CHECK (interval_seconds > 0),
    keep_count INTEGER NOT NULL CHECK (keep_count > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (repository, branch, interval_seconds),
    FOREIGN KEY (repository, branch) REFERENCES branch_policies(repository, branch) ON DELETE CASCADE
);

CREATE TABLE repo_live_branches (
    repository TEXT PRIMARY KEY,
    branch TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (repository, branch) REFERENCES branch_policies(repository, branch) ON DELETE CASCADE
);

CREATE TABLE branch_snapshots (
    repository TEXT NOT NULL,
    branch TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (repository, branch, commit_sha),
    FOREIGN KEY (repository, branch) REFERENCES branch_policies(repository, branch) ON DELETE CASCADE
);

CREATE INDEX idx_branch_snapshots_repo_commit ON branch_snapshots (repository, commit_sha);
CREATE INDEX idx_branch_snapshots_repo_branch ON branch_snapshots (repository, branch);
