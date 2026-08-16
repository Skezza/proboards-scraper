import json
from pathlib import Path

from oatcake_scraper.cli import _idle_sleep_seconds
from oatcake_scraper.parser import BoardInfo, ThreadSummary, parse_thread_page
from oatcake_scraper.persistence import ThreadArchive
from oatcake_scraper.sqlite_store import SQLiteArchiveStore

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf8")


def test_sqlite_merge_edits_tombstones_and_restore(tmp_path):
    store = SQLiteArchiveStore(tmp_path / "archive.db")
    board = BoardInfo("9", "Archive", "https://example.com/board/9")
    summary = ThreadSummary("777", "Delta", "https://example.com/thread/777", 2, 10, "t1")

    v1 = parse_thread_page(_load("thread_page.html"))
    result1 = store.merge_thread_snapshot(
        board,
        ThreadArchive(summary=summary, posts=v1.posts, last_page_crawled=2),
        observed_at="2026-01-01T00:00:00+00:00",
        full_verify=False,
    )
    assert result1.created is True
    assert result1.posts_new == 2

    v2 = parse_thread_page(_load("thread_page_v2.html"))
    result2 = store.merge_thread_snapshot(
        board,
        ThreadArchive(
            summary=ThreadSummary("777", "Delta", "https://example.com/thread/777", 1, 11, "t2"),
            posts=v2.posts,
            last_page_crawled=2,
        ),
        observed_at="2026-01-02T00:00:00+00:00",
        full_verify=True,
    )
    assert result2.posts_edited == 1
    assert result2.posts_tombstoned == 1

    snapshot = store.export_board_snapshot("9")
    thread = snapshot["threads"][0]
    post11 = next(post for post in thread["posts"] if post["post_id"] == "11")
    post12 = next(post for post in thread["posts"] if post["post_id"] == "12")
    assert post11["current_content"] == "First message edited."
    assert len(post11["revisions"]) == 1
    assert post12["is_deleted"] is True
    assert thread["last_verified_full_at"] == "2026-01-02T00:00:00+00:00"

    v3 = parse_thread_page(_load("thread_page_v3.html"))
    result3 = store.merge_thread_snapshot(
        board,
        ThreadArchive(
            summary=ThreadSummary("777", "Delta", "https://example.com/thread/777", 2, 12, "t3"),
            posts=v3.posts,
            last_page_crawled=2,
        ),
        observed_at="2026-01-03T00:00:00+00:00",
        full_verify=False,
    )
    assert result3.posts_restored == 1

    store.close()


def test_state_cursors_and_runtime_state(tmp_path):
    store = SQLiteArchiveStore(tmp_path / "archive.db")
    assert store.get_global_backfill_cursor(default_page=9) == (9, 0, 0)
    store.set_global_backfill_cursor(8, 2, 5)
    store.set_verify_cursor(7)
    state = store.get_runtime_state()
    state["state"] = "IDLE"
    store.set_runtime_state(state)

    store2 = SQLiteArchiveStore(tmp_path / "archive.db")
    assert store2.get_global_backfill_cursor(default_page=9) == (8, 2, 5)
    assert store2.get_verify_cursor() == 7
    assert store2.get_runtime_state()["state"] == "IDLE"
    store.close()
    store2.close()


def test_bootstrap_and_export(tmp_path):
    src = tmp_path / "legacy"
    src.mkdir()
    payload = {
        "board": {"board_id": "2", "name": "Legacy", "url": "https://example.com/board/2"},
        "threads": [
            {
                "thread_id": "123",
                "title": "Legacy Thread",
                "url": "https://example.com/thread/123",
                "replies": 1,
                "views": 2,
                "last_post_time": "ts",
                "last_page_crawled": 1,
                "posts": [
                    {
                        "post_id": "11",
                        "author": "A",
                        "current_content": "hello",
                        "current_raw_html": "<div>hello</div>",
                        "current_timestamp": "ts",
                        "current_timestamp_ms": 1,
                        "revisions": [],
                        "is_deleted": False,
                        "first_seen_at": "2026-01-01T00:00:00+00:00",
                        "last_seen_at": "2026-01-01T00:00:00+00:00",
                    }
                ],
            }
        ],
    }
    (src / "board-2.json").write_text(json.dumps(payload), encoding="utf8")

    store = SQLiteArchiveStore(tmp_path / "archive.db")
    result = store.bootstrap_from_json_archives(src)
    assert result["imported"] is True
    assert store.count_threads() == 1
    exported = store.export_snapshot(tmp_path / "exports", compression="none")
    assert exported["boards"] == 1
    store.close()


def test_idle_sleep_bounds():
    for exponent in range(0, 6):
        seconds = _idle_sleep_seconds(exponent, idle_min_seconds=600, idle_max_seconds=21600, jitter_ratio=0.2)
        assert 0 < seconds <= 21600 * 1.2
