#!/usr/bin/env python3
import argparse
import logging
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from oatcake_scraper.cli import RunCounters, _process_thread
from oatcake_scraper.config import ScraperConfig
from oatcake_scraper.fetcher import Fetcher
from oatcake_scraper.parser import BoardInfo, ThreadSummary
from oatcake_scraper.sqlite_store import SQLiteArchiveStore


def _incomplete_threads(store: SQLiteArchiveStore, limit: int) -> list[dict]:
    rows = store._conn.execute(
        """
        SELECT t.board_id, COALESCE(b.name, 'Board ' || t.board_id) AS board_name,
               COALESCE(b.url, '') AS board_url, b.category, b.description,
               t.thread_id, t.title, t.url, t.replies, t.views, t.last_post_time,
               t.last_page_crawled
        FROM threads t
        LEFT JOIN boards b ON b.board_id = t.board_id
        LEFT JOIN thread_done td ON td.board_id = t.board_id AND td.thread_id = t.thread_id
        WHERE COALESCE(td.done, 0) != 1
        ORDER BY
          CAST(t.board_id AS INTEGER),
          CAST(t.thread_id AS INTEGER)
        """
    ).fetchall()
    items = [dict(row) for row in rows]
    return items[:limit] if limit > 0 else items


def _remaining_count(store: SQLiteArchiveStore) -> int:
    row = store._conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM threads t
        LEFT JOIN thread_done td ON td.board_id = t.board_id AND td.thread_id = t.thread_id
        WHERE COALESCE(td.done, 0) != 1
        """
    ).fetchone()
    return int(row["c"] if row else 0)


def _board(row: dict) -> BoardInfo:
    return BoardInfo(
        board_id=str(row["board_id"]),
        name=row.get("board_name") or f"Board {row['board_id']}",
        url=row.get("board_url") or "",
        category=row.get("category"),
        description=row.get("description"),
    )


def _summary(row: dict) -> ThreadSummary:
    return ThreadSummary(
        thread_id=str(row["thread_id"]),
        title=row.get("title") or "",
        url=row.get("url") or "",
        replies=row.get("replies"),
        views=row.get("views"),
        last_post_time=row.get("last_post_time"),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill incomplete SQLite thread archives in bounded chunks.")
    parser.add_argument("--db-path", default="output/archive.db")
    parser.add_argument("--delay", type=float, default=2.5)
    parser.add_argument("--chunk-pages", type=int, default=200)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=25)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("gap_backfill")

    store = SQLiteArchiveStore(Path(args.db_path))
    fetcher = Fetcher(ScraperConfig(delay=args.delay))
    counters = RunCounters()
    event_counts = {"total": 0, "success": 0, "limited": 0, "fail": 0}

    def on_event(event) -> None:
        event_counts["total"] += 1
        if event.outcome == "success":
            event_counts["success"] += 1
        elif event.outcome == "limited":
            event_counts["limited"] += 1
        else:
            event_counts["fail"] += 1
        store.record_fetch_event(
            url=event.url,
            outcome=event.outcome,
            transport=event.transport,
            status_code=event.status_code,
            latency_ms=event.latency_ms,
            retry_after_seconds=event.retry_after_seconds,
            error=event.error,
        )
        if args.progress_every > 0 and event_counts["total"] % args.progress_every == 0:
            logger.info(
                "fetch progress total=%s success=%s limited=%s fail=%s latest=%s",
                event_counts["total"],
                event_counts["success"],
                event_counts["limited"],
                event_counts["fail"],
                event.url,
            )

    fetcher.set_event_hook(on_event)

    try:
        rows = _incomplete_threads(store, args.limit)
        logger.info("starting incomplete-thread backfill count=%s remaining_total=%s", len(rows), _remaining_count(store))
        for index, row in enumerate(rows, start=1):
            board = _board(row)
            summary = _summary(row)
            if not summary.url:
                logger.error("thread %s/%s has no URL", board.board_id, summary.thread_id)
                return 1

            while not store.thread_done(board.board_id, summary.thread_id):
                snapshot = store.get_thread_snapshot(board.board_id, summary.thread_id) or {}
                before_page = int(snapshot.get("last_page_crawled") or row.get("last_page_crawled") or 0)
                start_page = max(1, before_page + 1)
                logger.info(
                    "thread %s/%s (%s/%s) chunk start_page=%s previous_last_page=%s title=%r",
                    board.board_id,
                    summary.thread_id,
                    index,
                    len(rows),
                    start_page,
                    before_page,
                    summary.title,
                )
                ok = _process_thread(
                    board,
                    summary,
                    start_page,
                    fetcher,
                    store,
                    store,
                    counters,
                    full_verify=False,
                    max_pages=max(1, int(args.chunk_pages)),
                )
                store.save()
                after = store.get_thread_snapshot(board.board_id, summary.thread_id) or {}
                after_page = int(after.get("last_page_crawled") or 0)
                done = store.thread_done(board.board_id, summary.thread_id)
                logger.info(
                    "thread %s/%s chunk result ok=%s done=%s last_page=%s counters=%s",
                    board.board_id,
                    summary.thread_id,
                    ok,
                    done,
                    after_page,
                    counters,
                )
                if not ok:
                    logger.error("aborting after incomplete fetch for thread %s/%s", board.board_id, summary.thread_id)
                    return 1
                if not done and after_page <= before_page:
                    logger.error(
                        "aborting because thread %s/%s made no page progress: before=%s after=%s",
                        board.board_id,
                        summary.thread_id,
                        before_page,
                        after_page,
                    )
                    return 1

        logger.info(
            "backfill complete remaining_total=%s fetches=%s success=%s limited=%s fail=%s counters=%s",
            _remaining_count(store),
            event_counts["total"],
            event_counts["success"],
            event_counts["limited"],
            event_counts["fail"],
            counters,
        )
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
