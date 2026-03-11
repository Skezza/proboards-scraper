# proboards-scraper

Production-minded archival crawler for the Oatcake Fanzine ProBoards forum.

The crawler now supports:
- SQLite canonical storage (`archive.db`) with idempotent merge logic.
- Delta crawling + global oldest backfill + periodic full-thread verification.
- Post revision history and deletion tombstones.
- Always-on worker mode with adaptive idle backoff.
- Fetch event tracking and optional health/metrics endpoint.
- Compressed JSON snapshot exports (`.json.zst`) for portable archives.

## Quickstart

1. Install dependencies:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Run one crawl cycle:
```bash
python scraper.py crawl once \
  --db-path output/archive.db \
  --output-dir output \
  --import-json-bootstrap
```

3. Run always-on worker:
```bash
python scraper.py worker run \
  --db-path output/archive.db \
  --output-dir output \
  --export-dir exports \
  --lock-file output/worker.lock \
  --health-port 8080 \
  --profile conservative \
  --import-json-bootstrap
```

4. Export compressed snapshot:
```bash
python scraper.py export snapshot \
  --db-path output/archive.db \
  --export-dir exports \
  --export-compression zstd \
  --export-keep-daily 30 \
  --export-keep-monthly
```

5. Run diagnostics:
```bash
python scraper.py doctor check --db-path output/archive.db
```

## CLI Commands

### `crawl once`
Single cycle run:
1. recent-delta phase,
2. global oldest backfill phase,
3. full verification phase.

### `worker run`
Continuous loop with runtime states:
- `CATCHUP`: higher throughput budgets.
- `MAINTENANCE`: reduced steady-state budgets.
- `IDLE`: adaptive sleep with exponential backoff + jitter.

### `export snapshot`
Writes point-in-time board snapshots from SQLite into `export_dir/<timestamp>/` and emits `manifest.json`.

### `doctor check`
Runs SQLite quick integrity check and prints summary counters.

## Important Flags

- `--db-path`: SQLite database path (source of truth).
- `--output-dir`: legacy JSON path (used for optional bootstrap import).
- `--profile {conservative,balanced}`: crawl aggressiveness defaults.
- `--recent-pages`, `--backfill-threads-per-run`, `--verify-threads-per-run`, `--full-verify-days`.
- `--tail-overlap-pages`: overlap pages for thread delta recrawl.
- `--idle-min-seconds`, `--idle-max-seconds`, `--caught-up-cycles`.
- `--max-consecutive-failures`, `--retry-after-cap-seconds`.
- `--health-port`: enable `/health` and `/metrics`.
- `--import-json-bootstrap --bootstrap-json-dir <dir>`: one-time migration from existing `board-*.json`.

## Storage Model

SQLite tables:
- `boards`
- `threads`
- `posts_current`
- `post_revisions`
- `crawl_state`
- `crawl_runs`
- `fetch_events`

Semantics:
- `posts_current` stores latest post state + tombstone markers.
- `post_revisions` is append-only for historical edits.
- Missing posts are tombstoned only during full verification.

## Docker

Build:
```bash
docker build -t oatcake-archiver .
```

Run:
```bash
docker run --rm -p 8080:8080 -v "$PWD/data:/data" oatcake-archiver
```

## Testing

```bash
./.venv/bin/python -m pytest -q
```
