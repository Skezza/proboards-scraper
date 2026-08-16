import argparse
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Thread
from typing import Dict, List, Optional, Tuple

from .config import ScraperConfig
from .fetcher import FetchError, Fetcher
from .parser import (
    BoardInfo,
    BoardPageData,
    PostData,
    ThreadSummary,
    discover_boards,
    parse_board_page,
    parse_thread_page,
)
from .persistence import BoardWriter, CheckpointManager, MergeResult, ThreadArchive, utc_now_iso
from .sqlite_store import SQLiteArchiveStore

logger = logging.getLogger(__name__)


@dataclass
class BoardContext:
    board: BoardInfo
    first_page: BoardPageData
    last_page: int


@dataclass
class RunCounters:
    threads_new: int = 0
    threads_updated: int = 0
    posts_new: int = 0
    posts_edited: int = 0
    posts_tombstoned: int = 0
    posts_restored: int = 0


@dataclass
class PhaseStats:
    changed_threads: int = 0
    wrapped: bool = False


@dataclass
class CycleResult:
    counters: RunCounters
    recent_changed_threads: int
    backfill_changed_threads: int
    backfill_wrapped: bool
    verify_changed_threads: int
    consumed: int

    @property
    def any_changes(self) -> bool:
        return (self.recent_changed_threads + self.backfill_changed_threads + self.verify_changed_threads) > 0


@dataclass
class RuntimeMetrics:
    cycles_total: int = 0
    cycles_with_changes: int = 0
    requests_total: int = 0
    requests_success: int = 0
    requests_limited: int = 0
    requests_fail: int = 0
    last_cycle_started_at: Optional[str] = None
    last_cycle_ended_at: Optional[str] = None
    last_worker_state: str = "CATCHUP"
    last_idle_sleep_seconds: float = 0.0
    expected_board_pages: int = 0
    progress_refresh_requests: int = 25
    progress: Dict[str, object] = field(default_factory=dict)


class SingleInstanceLock:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh = None

    def __enter__(self):
        import fcntl

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._path.open("a+", encoding="utf8")
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError(f"worker lock already held: {self._path}") from exc
        self._fh.write(f"{utc_now_iso()} pid={os.getpid()}\n")
        self._fh.flush()
        return self

    def __exit__(self, exc_type, exc, tb):
        import fcntl

        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None

def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _structured_log(event: str, **kwargs) -> None:
    payload = {"event": event, **kwargs}
    logger.info(json.dumps(payload, separators=(",", ":"), sort_keys=True))


def _randomized_sleep_seconds(base_seconds: float, jitter_ratio: float) -> float:
    if base_seconds <= 0:
        return 0.0
    jitter_ratio = max(0.0, min(1.0, jitter_ratio))
    delta = base_seconds * jitter_ratio
    return max(0.0, base_seconds + random.uniform(-delta, delta))


def _idle_sleep_seconds(
    idle_exponent: int,
    idle_min_seconds: int,
    idle_max_seconds: int,
    jitter_ratio: float = 0.2,
) -> float:
    idle_min_seconds = max(1, int(idle_min_seconds))
    idle_max_seconds = max(idle_min_seconds, int(idle_max_seconds))
    exponent = max(0, int(idle_exponent))
    base = idle_min_seconds * (2 ** exponent)
    capped = min(idle_max_seconds, base)
    return _randomized_sleep_seconds(float(capped), jitter_ratio)


class _MetricsHandler(BaseHTTPRequestHandler):
    metrics: RuntimeMetrics = RuntimeMetrics()

    def _write(self, status: int, body: str, content_type: str) -> None:
        encoded = body.encode("utf8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            payload = {
                "status": "ok",
                "last_cycle_started_at": self.metrics.last_cycle_started_at,
                "last_cycle_ended_at": self.metrics.last_cycle_ended_at,
                "last_worker_state": self.metrics.last_worker_state,
                "progress": self.metrics.progress,
            }
            self._write(200, json.dumps(payload), "application/json")
            return
        if self.path == "/metrics":
            progress = self.metrics.progress or {}
            confidence = str(progress.get("confidence", "low"))
            confidence_value = {"low": 0, "medium": 1, "high": 2}.get(confidence, 0)
            lines = [
                f"oatcake_cycles_total {self.metrics.cycles_total}",
                f"oatcake_cycles_with_changes_total {self.metrics.cycles_with_changes}",
                f"oatcake_requests_total {self.metrics.requests_total}",
                f"oatcake_requests_success_total {self.metrics.requests_success}",
                f"oatcake_requests_limited_total {self.metrics.requests_limited}",
                f"oatcake_requests_fail_total {self.metrics.requests_fail}",
                f"oatcake_last_idle_sleep_seconds {self.metrics.last_idle_sleep_seconds:.2f}",
                f"oatcake_threads_discovered {int(progress.get('threads_discovered', 0))}",
                f"oatcake_threads_archived {int(progress.get('threads_archived', 0))}",
                f"oatcake_threads_total_estimated {int(progress.get('threads_total_estimated', 0))}",
                f"oatcake_threads_remaining_estimated {int(progress.get('threads_remaining_estimated', 0))}",
                f"oatcake_posts_archived_total {int(progress.get('posts_archived_total', 0))}",
                f"oatcake_board_page_coverage_ratio {float(progress.get('page_coverage_ratio') or 0.0):.6f}",
                f"oatcake_progress_confidence {confidence_value}",
            ]
            self._write(200, "\n".join(lines) + "\n", "text/plain; version=0.0.4")
            return
        self._write(404, "not found\n", "text/plain; charset=utf-8")

    def log_message(self, format, *args):  # noqa: A003
        return None


def _start_health_server(port: int, metrics: RuntimeMetrics):
    if port <= 0:
        return None
    _MetricsHandler.metrics = metrics
    server = ThreadingHTTPServer(("0.0.0.0", int(port)), _MetricsHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health endpoint listening on :%s", port)
    return server


def _refresh_progress(metrics: RuntimeMetrics, store: Optional[SQLiteArchiveStore], contexts: List[BoardContext]) -> None:
    if store is None:
        return
    expected_board_pages = sum(max(1, context.last_page) for context in contexts)
    metrics.expected_board_pages = expected_board_pages
    metrics.progress = store.progress_snapshot(expected_board_pages=expected_board_pages)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive Oatcake Fanzine forums")
    parser.add_argument(
        "command",
        nargs="?",
        default="crawl",
        choices=["crawl", "worker", "export", "doctor"],
        help="Command group (crawl/worker/export/doctor)",
    )
    parser.add_argument(
        "subcommand",
        nargs="?",
        default=None,
        help="Subcommand (crawl once | worker run | export snapshot)",
    )
    parser.add_argument(
        "--mode",
        choices=["once", "worker"],
        help="Legacy mode flag; overrides command group",
    )
    parser.add_argument(
        "--base-url",
        default=ScraperConfig().base_url,
        help="Base forum URL (defaults to Oatcake Fanzine)",
    )
    parser.add_argument(
        "--boards",
        nargs="+",
        help="Board IDs to archive (defaults to every discovered board)",
    )
    parser.add_argument(
        "--output-dir",
        default=ScraperConfig().output_dir,
        help="Directory where board JSON files are written",
    )
    parser.add_argument(
        "--checkpoint-file",
        default=ScraperConfig().checkpoint_file,
        help="Path to checkpoint JSON file (legacy JSON storage mode)",
    )
    parser.add_argument(
        "--db-path",
        default=ScraperConfig().db_path,
        help="Path to SQLite database (canonical production storage)",
    )
    parser.add_argument(
        "--export-dir",
        default=ScraperConfig().export_dir,
        help="Directory where snapshot exports are written",
    )
    parser.add_argument(
        "--bootstrap-json-dir",
        default=ScraperConfig().output_dir,
        help="Directory containing existing board-*.json files for one-time DB bootstrap",
    )
    parser.add_argument(
        "--profile",
        choices=["conservative", "balanced"],
        default="conservative",
        help="Runtime crawl profile",
    )
    parser.add_argument("--delay", type=float, default=ScraperConfig().delay, help="Delay between requests")
    parser.add_argument(
        "--rate-limit-delay",
        type=float,
        default=None,
        help="Override base delay used by fetcher (seconds)",
    )
    parser.add_argument("--max-retries", type=int, default=ScraperConfig().max_retries, help="Maximum fetch retries")
    parser.add_argument(
        "--backoff",
        type=float,
        default=ScraperConfig().backoff_factor,
        help="Backoff multiplier for retries",
    )
    parser.add_argument(
        "--user-agent",
        default=ScraperConfig().user_agent,
        help="User-Agent header sent with each request",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous checkpoint state (legacy JSON mode or existing DB state)",
    )
    parser.add_argument(
        "--max-threads",
        type=int,
        default=0,
        help="Optional cap on threads crawled per run (0 means no cap)",
    )
    parser.add_argument(
        "--recent-pages",
        type=int,
        default=1,
        help="How many newest board pages to scan every run for deltas",
    )
    parser.add_argument(
        "--backfill-pages",
        type=int,
        default=1,
        help="Legacy per-board oldest-page scan count to keep compatibility",
    )
    parser.add_argument(
        "--backfill-threads-per-run",
        type=int,
        default=25,
        help="Global oldest-backfill thread budget per run",
    )
    parser.add_argument(
        "--verify-threads-per-run",
        type=int,
        default=5,
        help="Full verification thread budget per run",
    )
    parser.add_argument(
        "--full-verify-days",
        type=int,
        default=30,
        help="Days between full verification sweeps for each thread",
    )
    parser.add_argument(
        "--tail-overlap-pages",
        type=int,
        default=1,
        help="When updating existing threads, start this many pages before last_page_crawled",
    )
    parser.add_argument(
        "--max-pages-per-thread",
        type=int,
        default=200,
        help="Cap pages fetched per thread in recent/backfill phases per cycle (0 disables cap)",
    )
    parser.add_argument(
        "--progress-refresh-requests",
        type=int,
        default=25,
        help="Refresh health progress every N fetch requests during a long cycle (0 disables)",
    )
    parser.add_argument(
        "--idle-min-seconds",
        type=int,
        default=ScraperConfig().idle_min_seconds,
        help="Initial worker idle sleep when caught up",
    )
    parser.add_argument(
        "--idle-max-seconds",
        type=int,
        default=ScraperConfig().idle_max_seconds,
        help="Maximum worker idle sleep when caught up",
    )
    parser.add_argument(
        "--caught-up-cycles",
        type=int,
        default=ScraperConfig().caught_up_cycles,
        help="Consecutive no-change cycles before transitioning to IDLE",
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=ScraperConfig().max_consecutive_failures,
        help="Circuit breaker threshold for consecutive fetch failures",
    )
    parser.add_argument(
        "--retry-after-cap-seconds",
        type=int,
        default=ScraperConfig().retry_after_cap_seconds,
        help="Maximum Retry-After delay honored from upstream",
    )
    parser.add_argument(
        "--lock-file",
        default=ScraperConfig().lock_file,
        help="Single-instance lock file used by worker mode",
    )
    parser.add_argument(
        "--health-port",
        type=int,
        default=0,
        help="Optional health/metrics HTTP port for worker mode",
    )
    parser.add_argument(
        "--export-compression",
        choices=["zstd", "none"],
        default="zstd",
        help="Compression format for snapshot exports",
    )
    parser.add_argument(
        "--auto-export-hours",
        type=int,
        default=0,
        help="Worker: export compressed snapshot every N hours (0 disables)",
    )
    parser.add_argument(
        "--export-keep-daily",
        type=int,
        default=30,
        help="Retention: keep one snapshot per day for this many days",
    )
    parser.add_argument(
        "--export-keep-monthly",
        action="store_true",
        help="Retention: keep one snapshot per month for older exports",
    )
    parser.add_argument(
        "--enable-json-storage",
        action="store_true",
        help="Use legacy JSON storage instead of SQLite",
    )
    parser.add_argument(
        "--import-json-bootstrap",
        action="store_true",
        help="If DB is empty, import existing board JSON files before crawling",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args(argv)

    if args.mode:
        args.command = "worker" if args.mode == "worker" else "crawl"
        if args.command == "worker":
            args.subcommand = "run"
        else:
            args.subcommand = "once"

    valid_subcommands = {
        "crawl": {None, "once"},
        "worker": {None, "run"},
        "export": {None, "snapshot"},
        "doctor": {None, "check"},
    }
    if args.subcommand not in valid_subcommands.get(args.command, set()):
        parser.error(f"Unsupported subcommand '{args.subcommand}' for command '{args.command}'")

    if args.profile == "balanced":
        args.delay = min(args.delay, 1.0)
        if args.backfill_threads_per_run < 50:
            args.backfill_threads_per_run = 50
        if args.verify_threads_per_run < 8:
            args.verify_threads_per_run = 8
    if args.rate_limit_delay is not None:
        args.delay = max(0.1, float(args.rate_limit_delay))

    args.subcommand = args.subcommand or {
        "crawl": "once",
        "worker": "run",
        "export": "snapshot",
        "doctor": "check",
    }[args.command]
    return args


def _board_sort_key(board: BoardInfo) -> Tuple[int, str]:
    return (int(board.board_id) if str(board.board_id).isdigit() else 10**9, board.board_id)


def _board_id_sort_key(board_id: str) -> Tuple[int, str]:
    return (int(board_id) if str(board_id).isdigit() else 10**9, str(board_id))


def _remaining_budget(max_threads: int, consumed: int) -> Optional[int]:
    if max_threads <= 0:
        return None
    remaining = max_threads - consumed
    return max(0, remaining)


def _apply_merge_result(counters: RunCounters, result: MergeResult) -> None:
    if result.created:
        counters.threads_new += 1
    elif result.updated:
        counters.threads_updated += 1
    counters.posts_new += result.posts_new
    counters.posts_edited += result.posts_edited
    counters.posts_tombstoned += result.posts_tombstoned
    counters.posts_restored += result.posts_restored


def _prepare_board_contexts(
    boards: List[BoardInfo],
    fetcher: Fetcher,
    base_url: str,
) -> List[BoardContext]:
    contexts: List[BoardContext] = []
    for board in boards:
        try:
            html = fetcher.fetch(board.url)
        except FetchError as exc:
            logger.error("Failed fetching board %s page 1: %s", board.board_id, exc)
            continue
        page_data = parse_board_page(html, board, base_url)
        contexts.append(BoardContext(board=board, first_page=page_data, last_page=max(1, page_data.last_page)))
    return contexts


def _get_board_page(
    context: BoardContext,
    page: int,
    fetcher: Fetcher,
    base_url: str,
    cache: Dict[Tuple[str, int], BoardPageData],
) -> Optional[BoardPageData]:
    key = (context.board.board_id, page)
    if key in cache:
        return cache[key]
    if page == 1:
        cache[key] = context.first_page
        return context.first_page
    page_url = f"{context.board.url}?page={page}"
    try:
        html = fetcher.fetch(page_url)
    except FetchError as exc:
        logger.error("Failed fetching board %s page %s: %s", context.board.board_id, page, exc)
        return None
    page_data = parse_board_page(html, context.board, base_url)
    cache[key] = page_data
    return page_data


def _thread_needs_delta(snapshot: Optional[dict], summary: ThreadSummary) -> bool:
    if not snapshot:
        return True
    stored_post_time = snapshot.get("last_post_time_seen", snapshot.get("last_post_time"))
    stored_replies = snapshot.get("last_replies_seen", snapshot.get("replies"))
    if stored_post_time != summary.last_post_time:
        return True
    if stored_replies != summary.replies:
        return True
    return False


def _delta_start_page(snapshot: Optional[dict], tail_overlap_pages: int) -> int:
    if not snapshot:
        return 1
    last_page = snapshot.get("last_page_crawled")
    if not isinstance(last_page, int) or last_page < 1:
        return 1
    return max(1, last_page - max(0, tail_overlap_pages))


def _crawl_thread(
    summary: ThreadSummary,
    fetcher: Fetcher,
    checkpoint: CheckpointManager,
    board_id: str,
    start_page: int,
    max_pages: int = 0,
) -> Tuple[List[PostData], int, bool, bool]:
    posts: List[PostData] = []
    page = max(1, start_page)
    max_page = page
    last_crawled = page - 1
    complete = True
    pages_fetched = 0
    capped = False
    while page <= max_page:
        if max_pages > 0 and pages_fetched >= max_pages:
            capped = True
            break
        url = summary.url if page == 1 else f"{summary.url}?page={page}"
        try:
            html = fetcher.fetch(url)
        except FetchError as exc:
            logger.error("Failed fetching thread %s page %s: %s", summary.thread_id, page, exc)
            complete = False
            break
        page_data = parse_thread_page(html)
        if not page_data.posts:
            break
        posts.extend(page_data.posts)
        max_page = max(max_page, page_data.last_page)
        last_crawled = page
        checkpoint.mark_thread_page(board_id, summary.thread_id, page)
        pages_fetched += 1
        page += 1
    return posts, max(last_crawled, 1), complete, capped


def _process_thread(
    board: BoardInfo,
    summary: ThreadSummary,
    start_page: int,
    fetcher: Fetcher,
    checkpoint: CheckpointManager,
    writer: BoardWriter,
    counters: RunCounters,
    full_verify: bool,
    max_pages: int = 0,
) -> bool:
    posts, last_page_crawled, complete, capped = _crawl_thread(
        summary,
        fetcher,
        checkpoint,
        board.board_id,
        start_page,
        max_pages=max_pages,
    )
    if not complete:
        logger.warning(
            "Skipping merge for thread %s because crawl was incomplete (start_page=%s)",
            summary.thread_id,
            start_page,
        )
        return False
    result = writer.merge_thread_snapshot(
        board,
        ThreadArchive(summary=summary, posts=posts, last_page_crawled=last_page_crawled),
        observed_at=utc_now_iso(),
        full_verify=full_verify,
    )
    _apply_merge_result(counters, result)
    if not capped:
        checkpoint.mark_thread_done(board.board_id, summary.thread_id)
    else:
        logger.info(
            "Thread %s hit per-cycle page cap (%s pages); merged partial progress to page %s",
            summary.thread_id,
            max_pages,
            last_page_crawled,
        )
    logger.info(
        "Merged thread %s (full_verify=%s, created=%s, new=%s, edited=%s, tombstoned=%s, restored=%s)",
        summary.thread_id,
        full_verify,
        result.created,
        result.posts_new,
        result.posts_edited,
        result.posts_tombstoned,
        result.posts_restored,
    )
    return True


def _run_recent_delta_phase(
    contexts: List[BoardContext],
    fetcher: Fetcher,
    checkpoint: CheckpointManager,
    writer: BoardWriter,
    counters: RunCounters,
    base_url: str,
    recent_pages: int,
    backfill_pages: int,
    tail_overlap_pages: int,
    max_pages_per_thread: int,
    max_threads: int,
    consumed: int,
) -> int:
    cache: Dict[Tuple[str, int], BoardPageData] = {}
    for context in contexts:
        pages = set(range(1, min(context.last_page, max(1, recent_pages)) + 1))
        if backfill_pages > 0:
            oldest_start = max(1, context.last_page - backfill_pages + 1)
            pages.update(range(oldest_start, context.last_page + 1))
        for page in sorted(pages):
            remaining = _remaining_budget(max_threads, consumed)
            if remaining == 0:
                return consumed
            page_data = _get_board_page(context, page, fetcher, base_url, cache)
            if page_data is None:
                continue
            checkpoint.mark_board_page(context.board.board_id, page)
            logger.info(
                "Recent phase: board %s page %s/%s (%s threads)",
                context.board.board_id,
                page,
                context.last_page,
                len(page_data.threads),
            )
            for summary in page_data.threads:
                remaining = _remaining_budget(max_threads, consumed)
                if remaining == 0:
                    return consumed
                snapshot = writer.get_thread_snapshot(context.board.board_id, summary.thread_id)
                if not _thread_needs_delta(snapshot, summary):
                    continue
                start_page = _delta_start_page(snapshot, tail_overlap_pages)
                if _process_thread(
                    context.board,
                    summary,
                    start_page,
                    fetcher,
                    checkpoint,
                    writer,
                    counters,
                    full_verify=False,
                    max_pages=max_pages_per_thread,
                ):
                    consumed += 1
                    checkpoint.save()
    return consumed


def _run_global_backfill_phase(
    contexts: List[BoardContext],
    fetcher: Fetcher,
    checkpoint: CheckpointManager,
    writer: BoardWriter,
    counters: RunCounters,
    base_url: str,
    backfill_threads_per_run: int,
    tail_overlap_pages: int,
    max_pages_per_thread: int,
    max_threads: int,
    consumed: int,
    return_stats: bool = False,
) -> int | Tuple[int, PhaseStats]:
    if backfill_threads_per_run <= 0 or not contexts:
        if return_stats:
            return consumed, PhaseStats()
        return consumed

    cache: Dict[Tuple[str, int], BoardPageData] = {}
    max_global_page = max(context.last_page for context in contexts)
    page, board_pos, thread_pos = checkpoint.get_global_backfill_cursor(max_global_page)
    processed = 0
    changed_threads = 0
    wrapped = False
    safety = 0
    safety_limit = max(100, backfill_threads_per_run * max(10, len(contexts)))

    while processed < backfill_threads_per_run and safety < safety_limit:
        safety += 1
        remaining = _remaining_budget(max_threads, consumed)
        if remaining == 0:
            break

        if page < 1:
            page = max_global_page
            board_pos = 0
            thread_pos = 0
            wrapped = True

        if board_pos >= len(contexts):
            page -= 1
            board_pos = 0
            thread_pos = 0
            checkpoint.set_global_backfill_cursor(page, board_pos, thread_pos)
            continue

        context = contexts[board_pos]
        if page > context.last_page:
            board_pos += 1
            thread_pos = 0
            checkpoint.set_global_backfill_cursor(page, board_pos, thread_pos)
            continue

        page_data = _get_board_page(context, page, fetcher, base_url, cache)
        if page_data is None:
            board_pos += 1
            thread_pos = 0
            checkpoint.set_global_backfill_cursor(page, board_pos, thread_pos)
            continue
        checkpoint.mark_board_page(context.board.board_id, page)

        oldest_first_threads = list(reversed(page_data.threads))
        if thread_pos >= len(oldest_first_threads):
            board_pos += 1
            thread_pos = 0
            checkpoint.set_global_backfill_cursor(page, board_pos, thread_pos)
            continue

        summary = oldest_first_threads[thread_pos]
        thread_pos += 1
        checkpoint.set_global_backfill_cursor(page, board_pos, thread_pos)

        snapshot = writer.get_thread_snapshot(context.board.board_id, summary.thread_id)
        if snapshot and not _thread_needs_delta(snapshot, summary):
            checkpoint.save()
            continue

        start_page = _delta_start_page(snapshot, tail_overlap_pages)
        logger.info(
            "Backfill phase: board %s page %s thread %s",
            context.board.board_id,
            page,
            summary.thread_id,
        )
        if _process_thread(
            context.board,
            summary,
            start_page,
            fetcher,
            checkpoint,
            writer,
            counters,
            full_verify=False,
            max_pages=max_pages_per_thread,
        ):
            consumed += 1
            processed += 1
            changed_threads += 1
        checkpoint.save()

    checkpoint.set_global_backfill_cursor(page, board_pos, thread_pos)
    if return_stats:
        return consumed, PhaseStats(changed_threads=changed_threads, wrapped=wrapped)
    return consumed


def _verification_due(last_verified_full_at: Optional[str], full_verify_days: int, now: datetime) -> bool:
    if not last_verified_full_at:
        return True
    try:
        verified_at = datetime.fromisoformat(last_verified_full_at)
    except ValueError:
        return True
    threshold = now - timedelta(days=max(0, full_verify_days))
    return verified_at < threshold


def _run_full_verification_phase(
    contexts: List[BoardContext],
    fetcher: Fetcher,
    checkpoint: CheckpointManager,
    writer: BoardWriter,
    counters: RunCounters,
    verify_threads_per_run: int,
    full_verify_days: int,
    max_threads: int,
    consumed: int,
) -> int:
    if verify_threads_per_run <= 0:
        return consumed

    all_refs: List[Dict] = []
    for context in contexts:
        all_refs.extend(writer.list_thread_refs(context.board.board_id))
    all_refs.sort(
        key=lambda ref: (
            _board_id_sort_key(str(ref.get("board_id", ""))),
            int(ref.get("thread_id")) if str(ref.get("thread_id", "")).isdigit() else 10**9,
        )
    )
    if not all_refs:
        return consumed

    cursor = checkpoint.get_verify_cursor() % len(all_refs)
    scanned = 0
    processed = 0
    now = datetime.now(timezone.utc)

    while scanned < len(all_refs) and processed < verify_threads_per_run:
        remaining = _remaining_budget(max_threads, consumed)
        if remaining == 0:
            break

        ref = all_refs[cursor]
        cursor = (cursor + 1) % len(all_refs)
        scanned += 1

        if not _verification_due(ref.get("last_verified_full_at"), full_verify_days, now):
            continue

        board_context = next((context for context in contexts if context.board.board_id == ref["board_id"]), None)
        if board_context is None:
            continue

        summary = ThreadSummary(
            thread_id=ref["thread_id"],
            title=ref.get("title") or "",
            url=ref.get("url") or "",
            replies=ref.get("replies"),
            views=ref.get("views"),
            last_post_time=ref.get("last_post_time"),
        )
        if not summary.url:
            continue

        logger.info("Verification phase: full thread verification for %s", summary.thread_id)
        if _process_thread(
            board_context.board,
            summary,
            1,
            fetcher,
            checkpoint,
            writer,
            counters,
            full_verify=True,
        ):
            consumed += 1
            processed += 1
            checkpoint.save()

    checkpoint.set_verify_cursor(cursor)
    return consumed


def _changed_thread_count(counters: RunCounters) -> int:
    return counters.threads_new + counters.threads_updated


def _run_cycle(
    args: argparse.Namespace,
    config: ScraperConfig,
    fetcher: Fetcher,
    checkpoint,
    writer,
    contexts: List[BoardContext],
) -> CycleResult:
    counters = RunCounters()
    consumed = 0

    recent_before = _changed_thread_count(counters)
    consumed = _run_recent_delta_phase(
        contexts,
        fetcher,
        checkpoint,
        writer,
        counters,
        config.base_url,
        args.recent_pages,
        args.backfill_pages,
        args.tail_overlap_pages,
        args.max_pages_per_thread,
        args.max_threads,
        consumed,
    )
    recent_changed = _changed_thread_count(counters) - recent_before

    backfill_before = _changed_thread_count(counters)
    backfill_result = _run_global_backfill_phase(
        contexts,
        fetcher,
        checkpoint,
        writer,
        counters,
        config.base_url,
        args.backfill_threads_per_run,
        args.tail_overlap_pages,
        args.max_pages_per_thread,
        args.max_threads,
        consumed,
        return_stats=True,
    )
    if isinstance(backfill_result, tuple):
        consumed, backfill_stats = backfill_result
    else:  # pragma: no cover - compatibility guard
        consumed = backfill_result
        backfill_stats = PhaseStats()
    backfill_changed = (_changed_thread_count(counters) - backfill_before) or backfill_stats.changed_threads

    verify_before = _changed_thread_count(counters)
    consumed = _run_full_verification_phase(
        contexts,
        fetcher,
        checkpoint,
        writer,
        counters,
        args.verify_threads_per_run,
        args.full_verify_days,
        args.max_threads,
        consumed,
    )
    verify_changed = _changed_thread_count(counters) - verify_before

    checkpoint.save()
    return CycleResult(
        counters=counters,
        recent_changed_threads=max(0, recent_changed),
        backfill_changed_threads=max(0, backfill_changed),
        backfill_wrapped=backfill_stats.wrapped,
        verify_changed_threads=max(0, verify_changed),
        consumed=consumed,
    )


def _open_storage(args: argparse.Namespace, output_dir: Path):
    if args.enable_json_storage:
        checkpoint = CheckpointManager(Path(args.checkpoint_file))
        writer = BoardWriter(output_dir)
        if not args.resume and Path(args.checkpoint_file).exists():
            Path(args.checkpoint_file).unlink()
        return checkpoint, writer, None

    store = SQLiteArchiveStore(Path(args.db_path))
    if args.import_json_bootstrap:
        bootstrap = store.bootstrap_from_json_archives(Path(args.bootstrap_json_dir))
        _structured_log("bootstrap", **bootstrap)
    return store, store, store


def _build_config(args: argparse.Namespace, output_dir: Path, checkpoint_path: Path) -> ScraperConfig:
    return ScraperConfig(
        base_url=args.base_url.rstrip("/"),
        delay=args.delay,
        max_retries=args.max_retries,
        backoff_factor=args.backoff,
        user_agent=args.user_agent,
        output_dir=output_dir,
        checkpoint_file=checkpoint_path,
        db_path=Path(args.db_path),
        export_dir=Path(args.export_dir),
        max_consecutive_failures=args.max_consecutive_failures,
        retry_after_cap_seconds=args.retry_after_cap_seconds,
        idle_min_seconds=args.idle_min_seconds,
        idle_max_seconds=args.idle_max_seconds,
        lock_file=Path(args.lock_file),
        caught_up_cycles=args.caught_up_cycles,
    )


def _prepare_cycle_contexts(args: argparse.Namespace, config: ScraperConfig, fetcher: Fetcher) -> List[BoardContext]:
    home_html = fetcher.fetch(config.base_url)
    boards = discover_boards(home_html, config.base_url)
    if not boards:
        raise RuntimeError(f"No boards discovered at {config.base_url}")

    target_ids = set(args.boards) if args.boards else {board.board_id for board in boards}
    selected = [board for board in boards if board.board_id in target_ids]
    selected.sort(key=_board_sort_key)
    if not selected:
        raise RuntimeError(f"No matching boards found for {target_ids}")

    contexts = _prepare_board_contexts(selected, fetcher, config.base_url)
    if not contexts:
        raise RuntimeError("Failed to prepare board contexts")
    return contexts


def _wire_fetch_events(fetcher: Fetcher, store: Optional[SQLiteArchiveStore], metrics: RuntimeMetrics) -> None:
    def _on_event(event) -> None:
        metrics.requests_total += 1
        if event.outcome == "success":
            metrics.requests_success += 1
        elif event.outcome == "limited":
            metrics.requests_limited += 1
        else:
            metrics.requests_fail += 1
        if store is not None:
            store.record_fetch_event(
                url=event.url,
                outcome=event.outcome,
                transport=event.transport,
                status_code=event.status_code,
                latency_ms=event.latency_ms,
                retry_after_seconds=event.retry_after_seconds,
                error=event.error,
            )
            refresh_every = max(0, int(metrics.progress_refresh_requests))
            if refresh_every and (metrics.requests_total % refresh_every == 0):
                metrics.progress = store.progress_snapshot(expected_board_pages=metrics.expected_board_pages)

    fetcher.set_event_hook(_on_event)


def _run_crawl_once(args: argparse.Namespace) -> int:
    _configure_logging(args.log_level)
    output_dir = Path(args.output_dir)
    checkpoint_path = Path(args.checkpoint_file)
    config = _build_config(args, output_dir, checkpoint_path)
    fetcher = Fetcher(config)
    checkpoint, writer, store = _open_storage(args, output_dir)
    metrics = RuntimeMetrics()
    metrics.progress_refresh_requests = max(0, int(args.progress_refresh_requests))
    _wire_fetch_events(fetcher, store, metrics)
    run_id = None
    if store is not None:
        run_id = store.start_run(mode="once", worker_state="CATCHUP")
    try:
        contexts = _prepare_cycle_contexts(args, config, fetcher)
        _refresh_progress(metrics, store, contexts)
        result = _run_cycle(args, config, fetcher, checkpoint, writer, contexts)
        _refresh_progress(metrics, store, contexts)
        _structured_log(
            "run_summary",
            threads_new=result.counters.threads_new,
            threads_updated=result.counters.threads_updated,
            posts_new=result.counters.posts_new,
            posts_edited=result.counters.posts_edited,
            posts_tombstoned=result.counters.posts_tombstoned,
            posts_restored=result.counters.posts_restored,
            backfill_wrapped=result.backfill_wrapped,
        )
        if store is not None and run_id is not None:
            store.finish_run(
                run_id=run_id,
                success=True,
                counters_json={
                    "threads_new": result.counters.threads_new,
                    "threads_updated": result.counters.threads_updated,
                    "posts_new": result.counters.posts_new,
                    "posts_edited": result.counters.posts_edited,
                    "posts_tombstoned": result.counters.posts_tombstoned,
                    "posts_restored": result.counters.posts_restored,
                },
            )
        return 0
    except Exception as exc:
        logger.error("crawl once failed: %s", exc)
        if store is not None and run_id is not None:
            store.finish_run(
                run_id=run_id,
                success=False,
                counters_json={"requests_total": metrics.requests_total},
                notes=str(exc),
            )
        return 1
    finally:
        if store is not None:
            store.close()


def _run_worker(args: argparse.Namespace) -> int:
    _configure_logging(args.log_level)
    output_dir = Path(args.output_dir)
    checkpoint_path = Path(args.checkpoint_file)
    config = _build_config(args, output_dir, checkpoint_path)
    stop_event = Event()
    metrics = RuntimeMetrics()
    metrics.progress_refresh_requests = max(0, int(args.progress_refresh_requests))

    def _signal_handler(signum, frame):  # noqa: ARG001
        logger.warning("Received signal %s, shutting down worker", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        with SingleInstanceLock(Path(args.lock_file)):
            checkpoint, writer, store = _open_storage(args, output_dir)
            fetcher = Fetcher(config)
            _wire_fetch_events(fetcher, store, metrics)
            health_server = _start_health_server(args.health_port, metrics)
            try:
                runtime_state = checkpoint.get_runtime_state()
                last_export_at = 0.0
                while not stop_event.is_set():
                    metrics.cycles_total += 1
                    metrics.last_cycle_started_at = utc_now_iso()
                    metrics.last_worker_state = runtime_state.get("state", "CATCHUP")
                    run_id = None
                    if store is not None:
                        run_id = store.start_run(mode="worker", worker_state=runtime_state.get("state", "CATCHUP"))

                    cycle_success = True
                    notes = ""
                    try:
                        cycle_args = argparse.Namespace(**vars(args))
                        state = runtime_state.get("state", "CATCHUP")
                        if state in {"MAINTENANCE", "IDLE"}:
                            cycle_args.backfill_threads_per_run = max(5, args.backfill_threads_per_run // 4)
                            cycle_args.verify_threads_per_run = max(1, args.verify_threads_per_run // 2)
                            cycle_args.recent_pages = max(1, min(args.recent_pages, 2))
                        if state == "IDLE":
                            cycle_args.backfill_threads_per_run = max(1, min(cycle_args.backfill_threads_per_run, 5))
                            cycle_args.verify_threads_per_run = max(1, min(cycle_args.verify_threads_per_run, 2))
                            cycle_args.max_threads = max(1, min(args.max_threads or 20, 20))
                        contexts = _prepare_cycle_contexts(args, config, fetcher)
                        _refresh_progress(metrics, store, contexts)
                        result = _run_cycle(cycle_args, config, fetcher, checkpoint, writer, contexts)
                        _refresh_progress(metrics, store, contexts)
                        cycle_changed = result.any_changes
                        if cycle_changed:
                            metrics.cycles_with_changes += 1
                            runtime_state["state"] = "CATCHUP"
                            runtime_state["consecutive_no_change_cycles"] = 0
                            runtime_state["idle_exponent"] = 0
                            runtime_state["clean_backfill_pass_seen"] = False
                            runtime_state["last_idle_sleep_seconds"] = 0
                        else:
                            runtime_state["consecutive_no_change_cycles"] = (
                                int(runtime_state.get("consecutive_no_change_cycles", 0)) + 1
                            )
                            if result.backfill_wrapped and result.backfill_changed_threads == 0:
                                runtime_state["clean_backfill_pass_seen"] = True
                            if (
                                runtime_state.get("clean_backfill_pass_seen")
                                and int(runtime_state.get("consecutive_no_change_cycles", 0)) >= args.caught_up_cycles
                            ):
                                runtime_state["state"] = "IDLE"
                            else:
                                runtime_state["state"] = "MAINTENANCE"

                        checkpoint.set_runtime_state(runtime_state)
                        checkpoint.save()
                        _structured_log(
                            "cycle_summary",
                            state=runtime_state["state"],
                            recent_changed=result.recent_changed_threads,
                            backfill_changed=result.backfill_changed_threads,
                            verify_changed=result.verify_changed_threads,
                            backfill_wrapped=result.backfill_wrapped,
                            threads_new=result.counters.threads_new,
                            threads_updated=result.counters.threads_updated,
                            posts_new=result.counters.posts_new,
                            posts_edited=result.counters.posts_edited,
                            posts_tombstoned=result.counters.posts_tombstoned,
                            posts_restored=result.counters.posts_restored,
                        )
                    except Exception as exc:
                        cycle_success = False
                        notes = str(exc)
                        logger.error("Worker cycle failed: %s", exc, exc_info=True)

                    if store is not None and run_id is not None:
                        store.finish_run(
                            run_id=run_id,
                            success=cycle_success,
                            counters_json={
                                "cycles_total": metrics.cycles_total,
                                "requests_total": metrics.requests_total,
                                "requests_success": metrics.requests_success,
                                "requests_limited": metrics.requests_limited,
                                "requests_fail": metrics.requests_fail,
                            },
                            notes=notes,
                        )

                    metrics.last_cycle_ended_at = utc_now_iso()
                    metrics.last_worker_state = runtime_state.get("state", "CATCHUP")

                    if (
                        store is not None
                        and args.auto_export_hours > 0
                        and time.monotonic() - last_export_at >= args.auto_export_hours * 3600
                    ):
                        try:
                            export_result = store.export_snapshot(
                                Path(args.export_dir),
                                compression=args.export_compression,
                            )
                            prune_result = store.prune_export_snapshots(
                                Path(args.export_dir),
                                keep_daily=args.export_keep_daily,
                                keep_monthly=args.export_keep_monthly,
                            )
                            _structured_log("auto_export", **export_result, **prune_result)
                            last_export_at = time.monotonic()
                        except Exception as exc:
                            logger.error("Auto export failed: %s", exc)

                    if stop_event.is_set():
                        break

                    if runtime_state.get("state") == "IDLE":
                        sleep_seconds = _idle_sleep_seconds(
                            idle_exponent=int(runtime_state.get("idle_exponent", 0)),
                            idle_min_seconds=args.idle_min_seconds,
                            idle_max_seconds=args.idle_max_seconds,
                        )
                        runtime_state["idle_exponent"] = int(runtime_state.get("idle_exponent", 0)) + 1
                        runtime_state["last_idle_sleep_seconds"] = sleep_seconds
                        checkpoint.set_runtime_state(runtime_state)
                        checkpoint.save()
                        metrics.last_idle_sleep_seconds = sleep_seconds
                        logger.info("Worker idle sleep %.2f seconds", sleep_seconds)
                        stop_event.wait(timeout=sleep_seconds)
                    elif runtime_state.get("state") == "MAINTENANCE":
                        stop_event.wait(timeout=60)
                    else:
                        # CATCHUP mode runs immediately to maximize throughput.
                        stop_event.wait(timeout=0)
            finally:
                if health_server is not None:
                    health_server.shutdown()
                    health_server.server_close()
                if store is not None:
                    store.close()
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 2
    return 0


def _run_export_snapshot(args: argparse.Namespace) -> int:
    _configure_logging(args.log_level)
    store = SQLiteArchiveStore(Path(args.db_path))
    try:
        result = store.export_snapshot(Path(args.export_dir), compression=args.export_compression)
        prune_result = store.prune_export_snapshots(
            Path(args.export_dir),
            keep_daily=args.export_keep_daily,
            keep_monthly=args.export_keep_monthly,
        )
        _structured_log("export_snapshot", **result, **prune_result)
        return 0
    except Exception as exc:
        logger.error("Snapshot export failed: %s", exc)
        return 1
    finally:
        store.close()


def _run_doctor(args: argparse.Namespace) -> int:
    _configure_logging(args.log_level)
    store = SQLiteArchiveStore(Path(args.db_path))
    try:
        summary = store.doctor_summary()
        print(json.dumps(summary, indent=2))
        return 0 if summary.get("integrity") == "ok" else 1
    finally:
        store.close()


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)

    if args.command == "crawl":
        sys.exit(_run_crawl_once(args))
    if args.command == "worker":
        sys.exit(_run_worker(args))
    if args.command == "export":
        sys.exit(_run_export_snapshot(args))
    if args.command == "doctor":
        sys.exit(_run_doctor(args))
    sys.exit(2)


if __name__ == "__main__":
    main()
