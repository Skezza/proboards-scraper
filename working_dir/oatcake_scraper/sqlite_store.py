import json
import logging
import sqlite3
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from .parser import BoardInfo, PostData, ThreadSummary
from .persistence import MergeResult, ThreadArchive, utc_now_iso

logger = logging.getLogger(__name__)


def _numeric_sort_key(value: str) -> Tuple[int, str]:
    return (int(value) if str(value).isdigit() else 10**12, str(value))


class SQLiteArchiveStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._configure()
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def save(self) -> None:
        # API parity with file-based checkpoints.
        return None

    def _configure(self) -> None:
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS boards (
                board_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                category TEXT,
                description TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS threads (
                board_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                replies INTEGER,
                views INTEGER,
                last_post_time TEXT,
                last_page_crawled INTEGER NOT NULL DEFAULT 1,
                last_post_time_seen TEXT,
                last_replies_seen INTEGER,
                last_verified_full_at TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (board_id, thread_id),
                FOREIGN KEY (board_id) REFERENCES boards(board_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_threads_board ON threads(board_id);
            CREATE INDEX IF NOT EXISTS idx_threads_verified ON threads(last_verified_full_at);

            CREATE TABLE IF NOT EXISTS posts_current (
                board_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                post_id TEXT NOT NULL,
                author TEXT,
                current_content TEXT,
                current_raw_html TEXT,
                current_timestamp TEXT,
                current_timestamp_ms INTEGER,
                is_deleted INTEGER NOT NULL DEFAULT 0,
                deleted_first_seen_at TEXT,
                deleted_last_seen_at TEXT,
                restored_at TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (board_id, thread_id, post_id),
                FOREIGN KEY (board_id, thread_id) REFERENCES threads(board_id, thread_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_posts_thread ON posts_current(board_id, thread_id);
            CREATE INDEX IF NOT EXISTS idx_posts_deleted ON posts_current(is_deleted);

            CREATE TABLE IF NOT EXISTS post_revisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                board_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                post_id TEXT NOT NULL,
                content TEXT,
                raw_html TEXT,
                observed_at TEXT NOT NULL,
                FOREIGN KEY (board_id, thread_id, post_id)
                    REFERENCES posts_current(board_id, thread_id, post_id)
                    ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_revisions_post
                ON post_revisions(board_id, thread_id, post_id, id);

            CREATE TABLE IF NOT EXISTS crawl_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS crawl_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                mode TEXT NOT NULL,
                worker_state TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                success INTEGER,
                counters_json TEXT,
                notes TEXT
            );

            CREATE TABLE IF NOT EXISTS fetch_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                url TEXT NOT NULL,
                status_code INTEGER,
                outcome TEXT NOT NULL,
                transport TEXT NOT NULL,
                latency_ms INTEGER,
                retry_after_seconds INTEGER,
                error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_fetch_events_time ON fetch_events(observed_at);
            CREATE INDEX IF NOT EXISTS idx_fetch_events_outcome ON fetch_events(outcome);

            CREATE TABLE IF NOT EXISTS thread_pages (
                board_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                page INTEGER NOT NULL,
                PRIMARY KEY (board_id, thread_id, page)
            );

            CREATE TABLE IF NOT EXISTS board_pages (
                board_id TEXT NOT NULL,
                page INTEGER NOT NULL,
                PRIMARY KEY (board_id, page)
            );

            CREATE TABLE IF NOT EXISTS thread_done (
                board_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                done INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (board_id, thread_id)
            );
            """
        )

    def _set_state(self, key: str, value: object) -> None:
        now = utc_now_iso()
        payload = json.dumps(value, separators=(",", ":"))
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO crawl_state(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (key, payload, now),
            )

    def _get_state(self, key: str, default):
        row = self._conn.execute("SELECT value FROM crawl_state WHERE key = ?", (key,)).fetchone()
        if not row:
            return default
        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return default

    def get_runtime_state(self) -> Dict:
        default = {
            "state": "CATCHUP",
            "consecutive_no_change_cycles": 0,
            "idle_exponent": 0,
            "clean_backfill_pass_seen": False,
            "last_idle_sleep_seconds": 0,
        }
        loaded = self._get_state("worker_runtime_state", default)
        if not isinstance(loaded, dict):
            return default
        merged = dict(default)
        merged.update(loaded)
        return merged

    def set_runtime_state(self, runtime_state: Dict) -> None:
        self._set_state("worker_runtime_state", runtime_state)

    # Checkpoint API compatibility
    def get_global_backfill_cursor(self, default_page: int) -> Tuple[int, int, int]:
        cursor = self._get_state(
            "global_backfill_cursor",
            {"page": default_page, "board_pos": 0, "thread_pos": 0},
        )
        if not isinstance(cursor, dict):
            cursor = {}
        page = cursor.get("page", default_page)
        board_pos = cursor.get("board_pos", 0)
        thread_pos = cursor.get("thread_pos", 0)
        if not isinstance(page, int) or page < 1:
            page = max(1, default_page)
        if not isinstance(board_pos, int) or board_pos < 0:
            board_pos = 0
        if not isinstance(thread_pos, int) or thread_pos < 0:
            thread_pos = 0
        return page, board_pos, thread_pos

    def set_global_backfill_cursor(self, page: int, board_pos: int, thread_pos: int) -> None:
        self._set_state(
            "global_backfill_cursor",
            {"page": max(1, int(page)), "board_pos": max(0, int(board_pos)), "thread_pos": max(0, int(thread_pos))},
        )

    def get_verify_cursor(self) -> int:
        cursor = self._get_state("verify_cursor", 0)
        if isinstance(cursor, int) and cursor >= 0:
            return cursor
        return 0

    def set_verify_cursor(self, cursor: int) -> None:
        self._set_state("verify_cursor", max(0, int(cursor)))

    def mark_board_page(self, board_id: str, page: int) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO board_pages(board_id, page) VALUES(?, ?)",
                (str(board_id), int(page)),
            )

    def board_page_done(self, board_id: str, page: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM board_pages WHERE board_id = ? AND page = ?",
            (str(board_id), int(page)),
        ).fetchone()
        return row is not None

    def mark_thread_page(self, board_id: str, thread_id: str, page: int) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO thread_pages(board_id, thread_id, page) VALUES(?, ?, ?)",
                (str(board_id), str(thread_id), int(page)),
            )

    def thread_page_done(self, board_id: str, thread_id: str, page: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM thread_pages WHERE board_id = ? AND thread_id = ? AND page = ?",
            (str(board_id), str(thread_id), int(page)),
        ).fetchone()
        return row is not None

    def mark_thread_done(self, board_id: str, thread_id: str) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO thread_done(board_id, thread_id, done) VALUES(?, ?, 1)",
                (str(board_id), str(thread_id)),
            )

    def thread_done(self, board_id: str, thread_id: str) -> bool:
        row = self._conn.execute(
            "SELECT done FROM thread_done WHERE board_id = ? AND thread_id = ?",
            (str(board_id), str(thread_id)),
        ).fetchone()
        return bool(row and row["done"])

    def count_threads(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) AS c FROM threads").fetchone()
        return int(row["c"]) if row else 0

    def count_posts(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) AS c FROM posts_current").fetchone()
        return int(row["c"]) if row else 0

    def _upsert_board(self, board_info: BoardInfo, observed_at: str) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO boards(board_id, name, url, category, description, first_seen_at, last_seen_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(board_id) DO UPDATE SET
                    name=excluded.name,
                    url=excluded.url,
                    category=excluded.category,
                    description=excluded.description,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    str(board_info.board_id),
                    board_info.name or "",
                    board_info.url or "",
                    board_info.category,
                    board_info.description,
                    observed_at,
                    observed_at,
                ),
            )

    def get_thread_snapshot(self, board_id: str, thread_id: str) -> Optional[Dict]:
        row = self._conn.execute(
            """
            SELECT board_id, thread_id, title, url, replies, views, last_post_time,
                   last_page_crawled, last_post_time_seen, last_replies_seen, last_verified_full_at
            FROM threads
            WHERE board_id = ? AND thread_id = ?
            """,
            (str(board_id), str(thread_id)),
        ).fetchone()
        if not row:
            return None
        return dict(row)

    def list_thread_refs(self, board_id: str) -> List[Dict]:
        rows = self._conn.execute(
            """
            SELECT board_id, thread_id, url, title, replies, views, last_post_time, last_verified_full_at
            FROM threads
            WHERE board_id = ?
            ORDER BY CAST(thread_id AS INTEGER), thread_id
            """,
            (str(board_id),),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_board_ids(self) -> List[str]:
        rows = self._conn.execute("SELECT board_id FROM boards ORDER BY board_id").fetchall()
        return [str(row["board_id"]) for row in rows]

    def merge_thread_snapshot(
        self,
        board_info: BoardInfo,
        archive: ThreadArchive,
        observed_at: Optional[str] = None,
        full_verify: bool = False,
    ) -> MergeResult:
        observed_at = observed_at or utc_now_iso()
        result = MergeResult()
        board_id = str(board_info.board_id)
        thread_id = str(archive.summary.thread_id)

        self._upsert_board(board_info, observed_at)

        existing_thread = self._conn.execute(
            "SELECT * FROM threads WHERE board_id = ? AND thread_id = ?",
            (board_id, thread_id),
        ).fetchone()

        with self._conn:
            if existing_thread is None:
                result.created = True
                result.updated = True
                self._conn.execute(
                    """
                    INSERT INTO threads(
                        board_id, thread_id, title, url, replies, views, last_post_time,
                        last_page_crawled, last_post_time_seen, last_replies_seen,
                        last_verified_full_at, first_seen_at, last_seen_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        board_id,
                        thread_id,
                        archive.summary.title,
                        archive.summary.url,
                        archive.summary.replies,
                        archive.summary.views,
                        archive.summary.last_post_time,
                        archive.last_page_crawled,
                        archive.summary.last_post_time,
                        archive.summary.replies,
                        observed_at if full_verify else None,
                        observed_at,
                        observed_at,
                    ),
                )
            else:
                before = dict(existing_thread)
                after = {
                    "title": archive.summary.title,
                    "url": archive.summary.url,
                    "replies": archive.summary.replies,
                    "views": archive.summary.views,
                    "last_post_time": archive.summary.last_post_time,
                    "last_page_crawled": archive.last_page_crawled,
                    "last_post_time_seen": archive.summary.last_post_time,
                    "last_replies_seen": archive.summary.replies,
                    "last_seen_at": observed_at,
                    "last_verified_full_at": observed_at if full_verify else before["last_verified_full_at"],
                }
                if (
                    before["title"] != after["title"]
                    or before["url"] != after["url"]
                    or before["replies"] != after["replies"]
                    or before["views"] != after["views"]
                    or before["last_post_time"] != after["last_post_time"]
                    or before["last_page_crawled"] != after["last_page_crawled"]
                    or before["last_post_time_seen"] != after["last_post_time_seen"]
                    or before["last_replies_seen"] != after["last_replies_seen"]
                    or before["last_verified_full_at"] != after["last_verified_full_at"]
                ):
                    result.updated = True
                self._conn.execute(
                    """
                    UPDATE threads
                    SET title = ?, url = ?, replies = ?, views = ?, last_post_time = ?,
                        last_page_crawled = ?, last_post_time_seen = ?, last_replies_seen = ?,
                        last_verified_full_at = ?, last_seen_at = ?
                    WHERE board_id = ? AND thread_id = ?
                    """,
                    (
                        after["title"],
                        after["url"],
                        after["replies"],
                        after["views"],
                        after["last_post_time"],
                        after["last_page_crawled"],
                        after["last_post_time_seen"],
                        after["last_replies_seen"],
                        after["last_verified_full_at"],
                        after["last_seen_at"],
                        board_id,
                        thread_id,
                    ),
                )

            post_rows = self._conn.execute(
                """
                SELECT * FROM posts_current
                WHERE board_id = ? AND thread_id = ?
                """,
                (board_id, thread_id),
            ).fetchall()
            post_index = {str(row["post_id"]): dict(row) for row in post_rows}
            seen_post_ids: Set[str] = set()

            for live in archive.posts:
                post_id = str(live.post_id)
                seen_post_ids.add(post_id)
                existing = post_index.get(post_id)
                if existing is None:
                    self._conn.execute(
                        """
                        INSERT INTO posts_current(
                            board_id, thread_id, post_id, author, current_content, current_raw_html,
                            current_timestamp, current_timestamp_ms, is_deleted,
                            deleted_first_seen_at, deleted_last_seen_at, restored_at,
                            first_seen_at, last_seen_at
                        )
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, ?, ?)
                        """,
                        (
                            board_id,
                            thread_id,
                            post_id,
                            live.author,
                            live.content,
                            live.raw_html,
                            live.timestamp,
                            live.timestamp_ms,
                            observed_at,
                            observed_at,
                        ),
                    )
                    result.posts_new += 1
                    result.updated = True
                    continue

                content_changed = (
                    existing.get("current_content") != live.content
                    or existing.get("current_raw_html") != live.raw_html
                )
                if content_changed:
                    self._conn.execute(
                        """
                        INSERT INTO post_revisions(board_id, thread_id, post_id, content, raw_html, observed_at)
                        VALUES(?, ?, ?, ?, ?, ?)
                        """,
                        (
                            board_id,
                            thread_id,
                            post_id,
                            existing.get("current_content"),
                            existing.get("current_raw_html"),
                            observed_at,
                        ),
                    )
                    result.posts_edited += 1
                    result.updated = True

                restored = bool(existing.get("is_deleted"))
                if restored:
                    result.posts_restored += 1
                    result.updated = True

                if (
                    existing.get("author") != live.author
                    or existing.get("current_timestamp") != live.timestamp
                    or existing.get("current_timestamp_ms") != live.timestamp_ms
                ):
                    result.updated = True

                self._conn.execute(
                    """
                    UPDATE posts_current
                    SET author = ?, current_content = ?, current_raw_html = ?,
                        current_timestamp = ?, current_timestamp_ms = ?,
                        is_deleted = 0, restored_at = CASE WHEN is_deleted = 1 THEN ? ELSE restored_at END,
                        last_seen_at = ?
                    WHERE board_id = ? AND thread_id = ? AND post_id = ?
                    """,
                    (
                        live.author,
                        live.content,
                        live.raw_html,
                        live.timestamp,
                        live.timestamp_ms,
                        observed_at,
                        observed_at,
                        board_id,
                        thread_id,
                        post_id,
                    ),
                )

            if full_verify:
                for post_id, existing in post_index.items():
                    if post_id in seen_post_ids:
                        continue
                    if not existing.get("is_deleted"):
                        self._conn.execute(
                            """
                            UPDATE posts_current
                            SET is_deleted = 1,
                                deleted_first_seen_at = COALESCE(deleted_first_seen_at, ?),
                                deleted_last_seen_at = ?
                            WHERE board_id = ? AND thread_id = ? AND post_id = ?
                            """,
                            (observed_at, observed_at, board_id, thread_id, post_id),
                        )
                        result.posts_tombstoned += 1
                        result.updated = True
                    else:
                        self._conn.execute(
                            """
                            UPDATE posts_current
                            SET deleted_last_seen_at = ?
                            WHERE board_id = ? AND thread_id = ? AND post_id = ?
                            """,
                            (observed_at, board_id, thread_id, post_id),
                        )

                self._conn.execute(
                    """
                    UPDATE threads
                    SET last_verified_full_at = ?
                    WHERE board_id = ? AND thread_id = ?
                    """,
                    (observed_at, board_id, thread_id),
                )
                result.updated = True

        return result

    def _post_dict(self, row: sqlite3.Row) -> Dict:
        revisions = self._conn.execute(
            """
            SELECT content, raw_html, observed_at
            FROM post_revisions
            WHERE board_id = ? AND thread_id = ? AND post_id = ?
            ORDER BY id ASC
            """,
            (row["board_id"], row["thread_id"], row["post_id"]),
        ).fetchall()
        return {
            "post_id": row["post_id"],
            "author": row["author"],
            "current_content": row["current_content"],
            "current_raw_html": row["current_raw_html"],
            "current_timestamp": row["current_timestamp"],
            "current_timestamp_ms": row["current_timestamp_ms"],
            "revisions": [dict(rev) for rev in revisions],
            "is_deleted": bool(row["is_deleted"]),
            "deleted_first_seen_at": row["deleted_first_seen_at"],
            "deleted_last_seen_at": row["deleted_last_seen_at"],
            "restored_at": row["restored_at"],
            "first_seen_at": row["first_seen_at"],
            "last_seen_at": row["last_seen_at"],
        }

    def export_board_snapshot(self, board_id: str) -> Dict:
        board_row = self._conn.execute("SELECT * FROM boards WHERE board_id = ?", (str(board_id),)).fetchone()
        if not board_row:
            return {"board": {}, "threads": []}
        thread_rows = self._conn.execute(
            """
            SELECT * FROM threads
            WHERE board_id = ?
            ORDER BY CAST(thread_id AS INTEGER), thread_id
            """,
            (str(board_id),),
        ).fetchall()

        threads: List[Dict] = []
        for thread_row in thread_rows:
            post_rows = self._conn.execute(
                """
                SELECT * FROM posts_current
                WHERE board_id = ? AND thread_id = ?
                ORDER BY CAST(post_id AS INTEGER), post_id
                """,
                (thread_row["board_id"], thread_row["thread_id"]),
            ).fetchall()
            thread_dict = {
                "thread_id": thread_row["thread_id"],
                "title": thread_row["title"],
                "url": thread_row["url"],
                "replies": thread_row["replies"],
                "views": thread_row["views"],
                "last_post_time": thread_row["last_post_time"],
                "posts": [self._post_dict(row) for row in post_rows],
                "last_page_crawled": thread_row["last_page_crawled"],
                "last_post_time_seen": thread_row["last_post_time_seen"],
                "last_replies_seen": thread_row["last_replies_seen"],
                "last_verified_full_at": thread_row["last_verified_full_at"],
            }
            threads.append(thread_dict)

        board_dict = {
            "board_id": board_row["board_id"],
            "name": board_row["name"],
            "url": board_row["url"],
            "category": board_row["category"],
            "description": board_row["description"],
        }
        return {"board": board_dict, "threads": threads}

    def export_snapshot(self, export_dir: Path, compression: str = "zstd") -> Dict:
        export_dir = Path(export_dir)
        export_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        target_dir = export_dir / stamp
        target_dir.mkdir(parents=True, exist_ok=True)

        boards = self.list_board_ids()
        exported_files: List[str] = []
        for board_id in boards:
            payload = self.export_board_snapshot(board_id)
            filename = f"board-{board_id}.json"
            path = target_dir / filename
            text = json.dumps(payload, indent=2)
            if compression == "zstd":
                try:
                    import zstandard as zstd  # type: ignore
                except ImportError as exc:  # pragma: no cover - dependency issue
                    raise RuntimeError("zstandard package required for zstd exports") from exc
                compressed = zstd.ZstdCompressor(level=8).compress(text.encode("utf8"))
                path = target_dir / f"{filename}.zst"
                path.write_bytes(compressed)
            else:
                path.write_text(text, encoding="utf8")
            exported_files.append(path.name)

        manifest = {
            "generated_at": utc_now_iso(),
            "db_path": str(self.db_path),
            "compression": compression,
            "boards": boards,
            "files": exported_files,
        }
        (target_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf8")
        return {"directory": str(target_dir), "boards": len(boards), "files": exported_files}

    def prune_export_snapshots(
        self,
        export_dir: Path,
        keep_daily: int = 30,
        keep_monthly: bool = True,
    ) -> Dict:
        export_dir = Path(export_dir)
        if not export_dir.exists():
            return {"deleted": 0, "kept": 0}

        snapshots = []
        for child in export_dir.iterdir():
            if not child.is_dir():
                continue
            manifest = child / "manifest.json"
            if not manifest.exists():
                continue
            try:
                ts = datetime.strptime(child.name[:16], "%Y%m%dT%H%M%SZ")
            except ValueError:
                continue
            snapshots.append((ts.replace(tzinfo=timezone.utc), child))

        snapshots.sort(key=lambda item: item[0], reverse=True)
        if not snapshots:
            return {"deleted": 0, "kept": 0}

        keep_daily = max(1, int(keep_daily))
        keep_set: Set[Path] = set()
        daily_days: Set[str] = set()
        monthly_months: Set[str] = set()
        now = datetime.now(timezone.utc)

        for ts, path in snapshots:
            day_key = ts.strftime("%Y-%m-%d")
            month_key = ts.strftime("%Y-%m")
            age_days = (now - ts).days
            if age_days <= keep_daily:
                if day_key not in daily_days:
                    keep_set.add(path)
                    daily_days.add(day_key)
                continue
            if keep_monthly and month_key not in monthly_months:
                keep_set.add(path)
                monthly_months.add(month_key)

        deleted = 0
        for _, path in snapshots:
            if path in keep_set:
                continue
            shutil.rmtree(path, ignore_errors=True)
            deleted += 1
        return {"deleted": deleted, "kept": len(keep_set)}

    def bootstrap_from_json_archives(self, directory: Path) -> Dict:
        directory = Path(directory)
        if not directory.exists():
            return {"imported": False, "reason": "missing_directory", "boards": 0, "threads": 0, "posts": 0}
        if self.count_threads() > 0:
            return {"imported": False, "reason": "db_not_empty", "boards": 0, "threads": 0, "posts": 0}

        files = sorted(directory.glob("board-*.json"))
        boards_count = 0
        threads_count = 0
        posts_count = 0
        for file_path in files:
            try:
                data = json.loads(file_path.read_text(encoding="utf8"))
            except (OSError, json.JSONDecodeError):
                continue
            board = data.get("board") if isinstance(data.get("board"), dict) else {}
            board_id = str(board.get("board_id") or file_path.stem.replace("board-", ""))
            board_info = BoardInfo(
                board_id=board_id,
                name=board.get("name") or f"Board {board_id}",
                url=board.get("url") or "",
                category=board.get("category"),
                description=board.get("description"),
            )
            self._upsert_board(board_info, utc_now_iso())
            boards_count += 1

            for thread in data.get("threads", []):
                if not isinstance(thread, dict):
                    continue
                summary = ThreadSummary(
                    thread_id=str(thread.get("thread_id") or ""),
                    title=thread.get("title") or "",
                    url=thread.get("url") or "",
                    replies=thread.get("replies"),
                    views=thread.get("views"),
                    last_post_time=thread.get("last_post_time"),
                )
                posts: List[PostData] = []
                for post in thread.get("posts", []):
                    if not isinstance(post, dict):
                        continue
                    posts.append(
                        PostData(
                            post_id=str(post.get("post_id") or ""),
                            author=post.get("author"),
                            timestamp=post.get("current_timestamp", post.get("timestamp")),
                            timestamp_ms=post.get("current_timestamp_ms", post.get("timestamp_ms")),
                            content=post.get("current_content", post.get("content")),
                            raw_html=post.get("current_raw_html", post.get("raw_html") or ""),
                        )
                    )
                archive = ThreadArchive(
                    summary=summary,
                    posts=posts,
                    last_page_crawled=int(thread.get("last_page_crawled") or 1),
                )
                observed_at = thread.get("last_seen_at") or utc_now_iso()
                self.merge_thread_snapshot(board_info, archive, observed_at=observed_at, full_verify=False)
                threads_count += 1
                posts_count += len(posts)

                # Preserve previously observed revisions and deletion metadata.
                for post in thread.get("posts", []):
                    if not isinstance(post, dict):
                        continue
                    post_id = str(post.get("post_id") or "")
                    with self._conn:
                        self._conn.execute(
                            """
                            UPDATE posts_current
                            SET is_deleted = ?, deleted_first_seen_at = ?, deleted_last_seen_at = ?,
                                restored_at = ?, first_seen_at = ?, last_seen_at = ?
                            WHERE board_id = ? AND thread_id = ? AND post_id = ?
                            """,
                            (
                                1 if post.get("is_deleted") else 0,
                                post.get("deleted_first_seen_at"),
                                post.get("deleted_last_seen_at"),
                                post.get("restored_at"),
                                post.get("first_seen_at") or observed_at,
                                post.get("last_seen_at") or observed_at,
                                board_id,
                                summary.thread_id,
                                post_id,
                            ),
                        )
                    revisions = post.get("revisions", [])
                    if isinstance(revisions, list):
                        for rev in revisions:
                            if not isinstance(rev, dict):
                                continue
                            with self._conn:
                                self._conn.execute(
                                    """
                                    INSERT INTO post_revisions(board_id, thread_id, post_id, content, raw_html, observed_at)
                                    VALUES(?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        board_id,
                                        summary.thread_id,
                                        post_id,
                                        rev.get("content"),
                                        rev.get("raw_html"),
                                        rev.get("observed_at") or observed_at,
                                    ),
                                )
                with self._conn:
                    self._conn.execute(
                        """
                        UPDATE threads
                        SET last_verified_full_at = ?,
                            last_post_time_seen = COALESCE(?, last_post_time_seen),
                            last_replies_seen = COALESCE(?, last_replies_seen)
                        WHERE board_id = ? AND thread_id = ?
                        """,
                        (
                            thread.get("last_verified_full_at"),
                            thread.get("last_post_time_seen"),
                            thread.get("last_replies_seen"),
                            board_id,
                            summary.thread_id,
                        ),
                    )

        return {
            "imported": bool(files),
            "reason": "ok" if files else "no_board_json_files",
            "boards": boards_count,
            "threads": threads_count,
            "posts": posts_count,
        }

    def start_run(self, mode: str, worker_state: str) -> int:
        started_at = utc_now_iso()
        with self._conn:
            cur = self._conn.execute(
                """
                INSERT INTO crawl_runs(mode, worker_state, started_at, success)
                VALUES(?, ?, ?, NULL)
                """,
                (mode, worker_state, started_at),
            )
            run_id = cur.lastrowid
        return int(run_id)

    def finish_run(self, run_id: int, success: bool, counters_json: Dict, notes: str = "") -> None:
        with self._conn:
            self._conn.execute(
                """
                UPDATE crawl_runs
                SET ended_at = ?, success = ?, counters_json = ?, notes = ?
                WHERE run_id = ?
                """,
                (utc_now_iso(), 1 if success else 0, json.dumps(counters_json, separators=(",", ":")), notes, run_id),
            )

    def record_fetch_event(
        self,
        url: str,
        outcome: str,
        transport: str,
        status_code: Optional[int],
        latency_ms: Optional[int],
        retry_after_seconds: Optional[int],
        error: Optional[str],
    ) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO fetch_events(
                    observed_at, url, status_code, outcome, transport, latency_ms, retry_after_seconds, error
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    utc_now_iso(),
                    url,
                    status_code,
                    outcome,
                    transport,
                    latency_ms,
                    retry_after_seconds,
                    error,
                ),
            )

    def doctor_summary(self) -> Dict:
        integrity = self._conn.execute("PRAGMA quick_check").fetchone()
        threads = self.count_threads()
        posts = self.count_posts()
        revisions_row = self._conn.execute("SELECT COUNT(*) AS c FROM post_revisions").fetchone()
        deleted_row = self._conn.execute("SELECT COUNT(*) AS c FROM posts_current WHERE is_deleted = 1").fetchone()
        boards_row = self._conn.execute("SELECT COUNT(*) AS c FROM boards").fetchone()
        runs_row = self._conn.execute("SELECT COUNT(*) AS c FROM crawl_runs").fetchone()
        return {
            "db_path": str(self.db_path),
            "integrity": integrity[0] if integrity else "unknown",
            "boards": int(boards_row["c"]) if boards_row else 0,
            "threads": threads,
            "posts_current": posts,
            "post_revisions": int(revisions_row["c"]) if revisions_row else 0,
            "posts_deleted": int(deleted_row["c"]) if deleted_row else 0,
            "crawl_runs": int(runs_row["c"]) if runs_row else 0,
        }

    def progress_snapshot(self, expected_board_pages: Optional[int] = None) -> Dict:
        threads_discovered = self.count_threads()
        posts_archived_total = self.count_posts()
        boards_row = self._conn.execute("SELECT COUNT(*) AS c FROM boards").fetchone()
        done_row = self._conn.execute("SELECT COUNT(*) AS c FROM thread_done WHERE done = 1").fetchone()
        scanned_pages_row = self._conn.execute("SELECT COUNT(*) AS c FROM board_pages").fetchone()

        boards_discovered = int(boards_row["c"]) if boards_row else 0
        threads_archived = int(done_row["c"]) if done_row else 0
        scanned_board_pages = int(scanned_pages_row["c"]) if scanned_pages_row else 0

        page_coverage = None
        confidence = "low"
        threads_total_estimated = threads_discovered
        if expected_board_pages and expected_board_pages > 0 and scanned_board_pages > 0:
            page_coverage = min(1.0, scanned_board_pages / float(expected_board_pages))
            bounded_coverage = max(0.05, page_coverage)
            threads_total_estimated = max(
                threads_discovered,
                int(round(threads_discovered / bounded_coverage)),
            )
            if page_coverage >= 0.95:
                confidence = "high"
            elif page_coverage >= 0.50:
                confidence = "medium"
        elif threads_discovered > 0:
            confidence = "medium"

        runtime_state = self.get_runtime_state()
        if runtime_state.get("clean_backfill_pass_seen"):
            threads_total_estimated = max(threads_total_estimated, threads_discovered)
            confidence = "high" if threads_discovered > 0 else confidence

        global_cursor = self._get_state("global_backfill_cursor", {})
        if not isinstance(global_cursor, dict):
            global_cursor = {}

        board_progress_rows = self._conn.execute(
            """
            SELECT t.board_id AS board_id,
                   COUNT(*) AS discovered,
                   COALESCE(SUM(CASE WHEN td.done = 1 THEN 1 ELSE 0 END), 0) AS archived
            FROM threads t
            LEFT JOIN thread_done td
              ON td.board_id = t.board_id AND td.thread_id = t.thread_id
            GROUP BY t.board_id
            ORDER BY (COUNT(*) - COALESCE(SUM(CASE WHEN td.done = 1 THEN 1 ELSE 0 END), 0)) DESC,
                     COUNT(*) DESC
            LIMIT 5
            """
        ).fetchall()
        coverage_by_board = []
        for row in board_progress_rows:
            discovered = int(row["discovered"] or 0)
            archived = int(row["archived"] or 0)
            remaining = max(0, discovered - archived)
            coverage_by_board.append(
                {
                    "board_id": str(row["board_id"]),
                    "threads_discovered": discovered,
                    "threads_archived": archived,
                    "threads_remaining": remaining,
                }
            )

        threads_remaining_estimated = max(0, threads_total_estimated - threads_archived)
        return {
            "boards_discovered": boards_discovered,
            "threads_discovered": threads_discovered,
            "threads_archived": threads_archived,
            "threads_total_estimated": threads_total_estimated,
            "threads_remaining_estimated": threads_remaining_estimated,
            "posts_archived_total": posts_archived_total,
            "scanned_board_pages": scanned_board_pages,
            "expected_board_pages": expected_board_pages or 0,
            "page_coverage_ratio": page_coverage,
            "confidence": confidence,
            "next_work_hint": {
                "page": int(global_cursor.get("page", 1) or 1),
                "board_pos": int(global_cursor.get("board_pos", 0) or 0),
                "thread_pos": int(global_cursor.get("thread_pos", 0) or 0),
            },
            "coverage_by_board": coverage_by_board,
            "updated_at": utc_now_iso(),
        }
