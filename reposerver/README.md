# pointer-reposerver

`pointer-reposerver` polls configured Git repositories, fetches only configured branches at depth 1,
and runs `pointer-indexer` only when branch head commits change.

Logging is emitted to stderr via `tracing` (configure verbosity with `RUST_LOG`).

## Run

```bash
cargo run -p pointer-reposerver -- --config reposerver/example.reposerver.toml --validate-config
cargo run -p pointer-reposerver -- --config reposerver/example.reposerver.toml --once
cargo run -p pointer-reposerver -- --config reposerver/example.reposerver.toml
```

## Config

See `reposerver/example.reposerver.toml` for a complete example.

`global.indexer_args` are applied first for every invocation, then `repo.indexer_args` are appended.
Per-branch args can be set with `[[repo.per_branch]]`; those args are appended last.

Optional global finish hook:

```toml
[global.finish_hook]
command = "scripts/after-sweep.sh"
timeout = "30s"
```

The finish hook runs once after all repos complete in `--once` mode, and once per full sweep in forever mode.
