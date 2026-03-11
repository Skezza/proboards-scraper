import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .parser import BoardInfo, PostData, ThreadSummary, to_dict


META_KEY = "__meta__"


@dataclass
class ThreadArchive:
    summary: ThreadSummary
    posts: List[PostData]
    last_page_crawled: int = 1


@dataclass
class MergeResult:
    created: bool = False
    updated: bool = False
    posts_new: int = 0
    posts_edited: int = 0
    posts_tombstoned: int = 0
    posts_restored: int = 0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


class CheckpointManager:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._state: Dict[str, Dict] = self._load()

    def _load(self) -> Dict[str, Dict]:
        if self._path.exists():
            try:
                with self._path.open("r", encoding="utf8") as fh:
                    loaded = json.load(fh)
                    if isinstance(loaded, dict):
                        return loaded
            except json.JSONDecodeError:
                return {}
        return {}

    def save(self) -> None:
        _ensure_parent(self._path)
        with self._path.open("w", encoding="utf8") as fh:
            json.dump(self._state, fh, indent=2)

    def _meta_entry(self) -> Dict:
        meta = self._state.setdefault(META_KEY, {})
        meta.setdefault("global_backfill_cursor", {"page": None, "board_pos": 0, "thread_pos": 0})
        meta.setdefault("verify_cursor", 0)
        meta.setdefault(
            "worker_runtime_state",
            {
                "state": "CATCHUP",
                "consecutive_no_change_cycles": 0,
                "idle_exponent": 0,
                "clean_backfill_pass_seen": False,
                "last_idle_sleep_seconds": 0,
            },
        )
        return meta

    def _board_entry(self, board_id: str) -> Dict:
        entry = self._state.setdefault(board_id, {})
        entry.setdefault("pages", [])
        entry.setdefault("threads", {})
        return entry

    def _thread_entry(self, board_id: str, thread_id: str) -> Dict:
        board = self._board_entry(board_id)
        threads = board.setdefault("threads", {})
        entry = threads.setdefault(thread_id, {})
        entry.setdefault("pages", [])
        entry.setdefault("done", False)
        return entry

    def get_global_backfill_cursor(self, default_page: int) -> Tuple[int, int, int]:
        meta = self._meta_entry()
        cursor = meta.get("global_backfill_cursor", {})
        page = cursor.get("page")
        if not isinstance(page, int) or page < 1:
            page = max(1, default_page)
        board_pos = cursor.get("board_pos", 0)
        thread_pos = cursor.get("thread_pos", 0)
        if not isinstance(board_pos, int) or board_pos < 0:
            board_pos = 0
        if not isinstance(thread_pos, int) or thread_pos < 0:
            thread_pos = 0
        return page, board_pos, thread_pos

    def set_global_backfill_cursor(self, page: int, board_pos: int, thread_pos: int) -> None:
        meta = self._meta_entry()
        meta["global_backfill_cursor"] = {
            "page": max(1, int(page)),
            "board_pos": max(0, int(board_pos)),
            "thread_pos": max(0, int(thread_pos)),
        }

    def get_verify_cursor(self) -> int:
        meta = self._meta_entry()
        cursor = meta.get("verify_cursor", 0)
        if isinstance(cursor, int) and cursor >= 0:
            return cursor
        return 0

    def set_verify_cursor(self, cursor: int) -> None:
        meta = self._meta_entry()
        meta["verify_cursor"] = max(0, int(cursor))

    def get_runtime_state(self) -> Dict:
        meta = self._meta_entry()
        runtime = meta.get("worker_runtime_state", {})
        if not isinstance(runtime, dict):
            runtime = {}
        default = {
            "state": "CATCHUP",
            "consecutive_no_change_cycles": 0,
            "idle_exponent": 0,
            "clean_backfill_pass_seen": False,
            "last_idle_sleep_seconds": 0,
        }
        merged = dict(default)
        merged.update(runtime)
        return merged

    def set_runtime_state(self, runtime_state: Dict) -> None:
        meta = self._meta_entry()
        meta["worker_runtime_state"] = dict(runtime_state)

    def mark_board_page(self, board_id: str, page: int) -> None:
        entry = self._board_entry(board_id)
        if page not in entry["pages"]:
            entry["pages"].append(page)

    def board_page_done(self, board_id: str, page: int) -> bool:
        entry = self._board_entry(board_id)
        return page in entry["pages"]

    def mark_thread_page(self, board_id: str, thread_id: str, page: int) -> None:
        entry = self._thread_entry(board_id, thread_id)
        if page not in entry["pages"]:
            entry["pages"].append(page)

    def thread_page_done(self, board_id: str, thread_id: str, page: int) -> bool:
        entry = self._thread_entry(board_id, thread_id)
        return page in entry["pages"]

    def mark_thread_done(self, board_id: str, thread_id: str) -> None:
        entry = self._thread_entry(board_id, thread_id)
        entry["done"] = True

    def thread_done(self, board_id: str, thread_id: str) -> bool:
        entry = self._thread_entry(board_id, thread_id)
        return bool(entry.get("done"))


class BoardWriter:
    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, board_id: str) -> Path:
        return self._output_dir / f"board-{board_id}.json"

    def _migrate_post_entry(self, post: Dict, observed_at: str) -> Dict:
        post_id = str(post.get("post_id") or "")
        content = post.get("current_content", post.get("content"))
        raw_html = post.get("current_raw_html", post.get("raw_html"))
        timestamp = post.get("current_timestamp", post.get("timestamp"))
        timestamp_ms = post.get("current_timestamp_ms", post.get("timestamp_ms"))
        revisions = post.get("revisions") if isinstance(post.get("revisions"), list) else []

        migrated = {
            "post_id": post_id,
            "author": post.get("author"),
            "current_content": content,
            "current_raw_html": raw_html,
            "current_timestamp": timestamp,
            "current_timestamp_ms": timestamp_ms,
            "revisions": revisions,
            "is_deleted": bool(post.get("is_deleted", False)),
            "deleted_first_seen_at": post.get("deleted_first_seen_at"),
            "deleted_last_seen_at": post.get("deleted_last_seen_at"),
            "restored_at": post.get("restored_at"),
            "first_seen_at": post.get("first_seen_at") or observed_at,
            "last_seen_at": post.get("last_seen_at") or observed_at,
        }
        return migrated

    def _migrate_thread_entry(self, thread: Dict, observed_at: str) -> Dict:
        migrated = dict(thread)
        migrated["thread_id"] = str(migrated.get("thread_id") or "")
        posts = migrated.get("posts") if isinstance(migrated.get("posts"), list) else []
        migrated_posts = [
            self._migrate_post_entry(post, observed_at)
            for post in posts
            if isinstance(post, dict)
        ]
        migrated["posts"] = migrated_posts
        last_page_crawled = migrated.get("last_page_crawled")
        if not isinstance(last_page_crawled, int) or last_page_crawled < 1:
            migrated["last_page_crawled"] = 1
        migrated["last_post_time_seen"] = migrated.get("last_post_time_seen", migrated.get("last_post_time"))
        migrated["last_replies_seen"] = migrated.get("last_replies_seen", migrated.get("replies"))
        migrated.setdefault("last_verified_full_at", None)
        return migrated

    def _read(self, board_id: str, observed_at: Optional[str] = None) -> Dict:
        observed_at = observed_at or utc_now_iso()
        path = self._path(board_id)
        if path.exists():
            try:
                with path.open("r", encoding="utf8") as fh:
                    data = json.load(fh)
            except json.JSONDecodeError:
                data = {}
        else:
            data = {}
        if not isinstance(data, dict):
            data = {}
        board = data.get("board") if isinstance(data.get("board"), dict) else {}
        threads = data.get("threads") if isinstance(data.get("threads"), list) else []
        migrated_threads = [
            self._migrate_thread_entry(thread, observed_at)
            for thread in threads
            if isinstance(thread, dict)
        ]
        return {"board": board, "threads": migrated_threads}

    def _write(self, board_id: str, data: Dict) -> None:
        with self._path(board_id).open("w", encoding="utf8") as fh:
            json.dump(data, fh, indent=2)

    def existing_thread_ids(self, board_id: str) -> Set[str]:
        data = self._read(board_id)
        return {
            str(thread.get("thread_id"))
            for thread in data.get("threads", [])
            if thread.get("thread_id")
        }

    def get_thread_snapshot(self, board_id: str, thread_id: str) -> Optional[Dict]:
        data = self._read(board_id)
        for thread in data.get("threads", []):
            if thread.get("thread_id") == str(thread_id):
                return thread
        return None

    def list_threads(self, board_id: str) -> List[Dict]:
        data = self._read(board_id)
        return data.get("threads", [])

    def list_thread_refs(self, board_id: str) -> List[Dict]:
        refs: List[Dict] = []
        for thread in self.list_threads(board_id):
            if not thread.get("thread_id"):
                continue
            refs.append(
                {
                    "board_id": str(board_id),
                    "thread_id": str(thread.get("thread_id")),
                    "url": thread.get("url"),
                    "title": thread.get("title"),
                    "replies": thread.get("replies"),
                    "views": thread.get("views"),
                    "last_post_time": thread.get("last_post_time"),
                    "last_verified_full_at": thread.get("last_verified_full_at"),
                }
            )
        return refs

    def merge_thread_snapshot(
        self,
        board_info: BoardInfo,
        archive: ThreadArchive,
        observed_at: Optional[str] = None,
        full_verify: bool = False,
    ) -> MergeResult:
        observed_at = observed_at or utc_now_iso()
        data = self._read(board_info.board_id, observed_at=observed_at)
        data["board"] = to_dict(board_info)
        threads = data.setdefault("threads", [])

        entry: Optional[Dict] = None
        for existing in threads:
            if existing.get("thread_id") == str(archive.summary.thread_id):
                entry = existing
                break

        result = MergeResult()
        if entry is None:
            result.created = True
            result.updated = True
            entry = {
                "thread_id": str(archive.summary.thread_id),
                "title": archive.summary.title,
                "url": archive.summary.url,
                "replies": archive.summary.replies,
                "views": archive.summary.views,
                "last_post_time": archive.summary.last_post_time,
                "posts": [],
                "last_page_crawled": archive.last_page_crawled,
                "last_post_time_seen": archive.summary.last_post_time,
                "last_replies_seen": archive.summary.replies,
                "last_verified_full_at": None,
            }
            threads.append(entry)
        else:
            prior_summary = (
                entry.get("title"),
                entry.get("url"),
                entry.get("replies"),
                entry.get("views"),
                entry.get("last_post_time"),
                entry.get("last_page_crawled"),
                entry.get("last_post_time_seen"),
                entry.get("last_replies_seen"),
            )
            entry["title"] = archive.summary.title
            entry["url"] = archive.summary.url
            entry["replies"] = archive.summary.replies
            entry["views"] = archive.summary.views
            entry["last_post_time"] = archive.summary.last_post_time
            entry["last_page_crawled"] = archive.last_page_crawled
            entry["last_post_time_seen"] = archive.summary.last_post_time
            entry["last_replies_seen"] = archive.summary.replies
            new_summary = (
                entry.get("title"),
                entry.get("url"),
                entry.get("replies"),
                entry.get("views"),
                entry.get("last_post_time"),
                entry.get("last_page_crawled"),
                entry.get("last_post_time_seen"),
                entry.get("last_replies_seen"),
            )
            if prior_summary != new_summary:
                result.updated = True

        existing_posts = entry.setdefault("posts", [])
        post_index: Dict[str, Dict] = {
            str(post.get("post_id")): post
            for post in existing_posts
            if isinstance(post, dict) and post.get("post_id") is not None
        }

        seen_post_ids: Set[str] = set()
        for live in archive.posts:
            post_id = str(live.post_id)
            seen_post_ids.add(post_id)
            existing = post_index.get(post_id)
            if existing is None:
                created_post = {
                    "post_id": post_id,
                    "author": live.author,
                    "current_content": live.content,
                    "current_raw_html": live.raw_html,
                    "current_timestamp": live.timestamp,
                    "current_timestamp_ms": live.timestamp_ms,
                    "revisions": [],
                    "is_deleted": False,
                    "deleted_first_seen_at": None,
                    "deleted_last_seen_at": None,
                    "restored_at": None,
                    "first_seen_at": observed_at,
                    "last_seen_at": observed_at,
                }
                existing_posts.append(created_post)
                post_index[post_id] = created_post
                result.posts_new += 1
                result.updated = True
                continue

            content_changed = (
                existing.get("current_content") != live.content
                or existing.get("current_raw_html") != live.raw_html
            )
            if content_changed:
                revisions = existing.setdefault("revisions", [])
                revisions.append(
                    {
                        "content": existing.get("current_content"),
                        "raw_html": existing.get("current_raw_html"),
                        "observed_at": observed_at,
                    }
                )
                existing["current_content"] = live.content
                existing["current_raw_html"] = live.raw_html
                result.posts_edited += 1
                result.updated = True

            if existing.get("is_deleted"):
                existing["is_deleted"] = False
                existing["restored_at"] = observed_at
                result.posts_restored += 1
                result.updated = True

            if (
                existing.get("author") != live.author
                or existing.get("current_timestamp") != live.timestamp
                or existing.get("current_timestamp_ms") != live.timestamp_ms
            ):
                result.updated = True

            existing["author"] = live.author
            existing["current_timestamp"] = live.timestamp
            existing["current_timestamp_ms"] = live.timestamp_ms
            existing["last_seen_at"] = observed_at
            existing.setdefault("first_seen_at", observed_at)

        if full_verify:
            for post_id, existing in post_index.items():
                if post_id in seen_post_ids:
                    continue
                if not existing.get("is_deleted"):
                    existing["is_deleted"] = True
                    existing["deleted_first_seen_at"] = existing.get("deleted_first_seen_at") or observed_at
                    existing["deleted_last_seen_at"] = observed_at
                    result.posts_tombstoned += 1
                    result.updated = True
                else:
                    existing["deleted_last_seen_at"] = observed_at
            entry["last_verified_full_at"] = observed_at
            result.updated = True

        existing_posts.sort(
            key=lambda post: int(post["post_id"]) if str(post.get("post_id", "")).isdigit() else 10**18
        )

        self._write(board_info.board_id, data)
        return result

    def persist_threads(self, board_info: BoardInfo, archives: Iterable[ThreadArchive]) -> List[str]:
        created: List[str] = []
        for archive in archives:
            result = self.merge_thread_snapshot(board_info, archive, observed_at=utc_now_iso(), full_verify=False)
            if result.created:
                created.append(archive.summary.thread_id)
        return created

    def upsert_thread(self, board_info: BoardInfo, archive: ThreadArchive) -> Tuple[int, bool]:
        result = self.merge_thread_snapshot(board_info, archive, observed_at=utc_now_iso(), full_verify=False)
        return result.posts_new, result.created
