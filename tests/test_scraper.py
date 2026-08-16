import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from oatcake_scraper.cli import (
    BoardContext,
    RunCounters,
    _delta_start_page,
    _run_full_verification_phase,
    _run_global_backfill_phase,
    _run_recent_delta_phase,
    _verification_due,
)
from oatcake_scraper.parser import (
    BoardInfo,
    BoardPageData,
    ThreadSummary,
    discover_boards,
    parse_board_page,
    parse_thread_page,
)
from oatcake_scraper.persistence import BoardWriter, CheckpointManager, ThreadArchive

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf8")


def test_discover_boards():
    html = _load("home.html")
    boards = discover_boards(html, "https://oatcakefanzine.proboards.com")
    assert any(board.board_id == "2" and board.category == "Football" for board in boards)
    assert any(board.board_id == "1" for board in boards)


def test_parse_board_page():
    html = _load("board_page.html")
    board = BoardInfo("2", "Stoke City FC", "https://oatcakefanzine.proboards.com/board/2/stoke-city-fc")
    data = parse_board_page(html, board, "https://oatcakefanzine.proboards.com")
    assert data.current_page == 1
    assert data.last_page == 3
    assert board.description == "Stoke City discussion"
    assert len(data.threads) == 1
    summary = data.threads[0]
    assert summary.thread_id == "123"
    assert summary.title == "First Thread"
    assert summary.views == 1000


def test_parse_thread_page():
    html = _load("thread_page.html")
    data = parse_thread_page(html)
    assert data.current_page == 1
    assert data.last_page == 2
    assert len(data.posts) == 2
    assert data.posts[0].author == "UserOne"
    assert data.posts[0].timestamp_ms == 1772937600000


def test_schema_migration_from_old_archive(tmp_path):
    board_file = tmp_path / "board-2.json"
    old_archive = {
        "board": {"board_id": "2", "name": "Old", "url": "https://example.com/board/2"},
        "threads": [
            {
                "thread_id": "123",
                "title": "Old Thread",
                "url": "https://example.com/thread/123",
                "replies": 1,
                "views": 5,
                "last_post_time": "old-time",
                "posts": [
                    {
                        "post_id": "11",
                        "author": "OldUser",
                        "timestamp": "old-ts",
                        "timestamp_ms": 1,
                        "content": "old content",
                        "raw_html": "<div>old content</div>",
                    }
                ],
            }
        ],
    }
    board_file.write_text(json.dumps(old_archive), encoding="utf8")

    writer = BoardWriter(tmp_path)
    snapshot = writer.get_thread_snapshot("2", "123")
    assert snapshot is not None
    assert snapshot["last_post_time_seen"] == "old-time"
    assert snapshot["last_replies_seen"] == 1
    assert snapshot["posts"][0]["current_content"] == "old content"
    assert snapshot["posts"][0]["revisions"] == []
    assert snapshot["posts"][0]["is_deleted"] is False


def test_merge_edits_tombstones_and_restore(tmp_path):
    writer = BoardWriter(tmp_path)
    board = BoardInfo("9", "Archive", "https://example.com/board/9")
    summary = ThreadSummary("777", "Delta", "https://example.com/thread/777", 2, 10, "t1")

    v1 = parse_thread_page(_load("thread_page.html"))
    result1 = writer.merge_thread_snapshot(
        board,
        ThreadArchive(summary=summary, posts=v1.posts, last_page_crawled=2),
        observed_at="2026-01-01T00:00:00+00:00",
        full_verify=False,
    )
    assert result1.created is True
    assert result1.posts_new == 2

    summary2 = ThreadSummary("777", "Delta", "https://example.com/thread/777", 1, 11, "t2")
    v2 = parse_thread_page(_load("thread_page_v2.html"))
    result2 = writer.merge_thread_snapshot(
        board,
        ThreadArchive(summary=summary2, posts=v2.posts, last_page_crawled=2),
        observed_at="2026-01-02T00:00:00+00:00",
        full_verify=True,
    )
    assert result2.posts_edited == 1
    assert result2.posts_tombstoned == 1

    snapshot = writer.get_thread_snapshot("9", "777")
    assert snapshot is not None
    post11 = next(post for post in snapshot["posts"] if post["post_id"] == "11")
    post12 = next(post for post in snapshot["posts"] if post["post_id"] == "12")
    assert post11["current_content"] == "First message edited."
    assert len(post11["revisions"]) == 1
    assert post12["is_deleted"] is True
    assert snapshot["last_verified_full_at"] == "2026-01-02T00:00:00+00:00"

    summary3 = ThreadSummary("777", "Delta", "https://example.com/thread/777", 2, 12, "t3")
    v3 = parse_thread_page(_load("thread_page_v3.html"))
    result3 = writer.merge_thread_snapshot(
        board,
        ThreadArchive(summary=summary3, posts=v3.posts, last_page_crawled=2),
        observed_at="2026-01-03T00:00:00+00:00",
        full_verify=False,
    )
    assert result3.posts_restored == 1

    snapshot3 = writer.get_thread_snapshot("9", "777")
    assert snapshot3 is not None
    restored = next(post for post in snapshot3["posts"] if post["post_id"] == "12")
    assert restored["is_deleted"] is False
    assert restored["restored_at"] == "2026-01-03T00:00:00+00:00"


def test_checkpoint_global_and_verify_cursors(tmp_path):
    checkpoint = CheckpointManager(tmp_path / "checkpoint.json")
    page, board_pos, thread_pos = checkpoint.get_global_backfill_cursor(default_page=9)
    assert (page, board_pos, thread_pos) == (9, 0, 0)

    checkpoint.set_global_backfill_cursor(8, 2, 5)
    checkpoint.set_verify_cursor(7)
    checkpoint.save()

    loaded = CheckpointManager(tmp_path / "checkpoint.json")
    assert loaded.get_global_backfill_cursor(default_page=9) == (8, 2, 5)
    assert loaded.get_verify_cursor() == 7


def test_delta_start_page_and_verification_due():
    assert _delta_start_page(None, 1) == 1
    assert _delta_start_page({"last_page_crawled": 10}, 1) == 9
    assert _delta_start_page({"last_page_crawled": 10}, 3) == 7

    now = datetime(2026, 1, 31, tzinfo=timezone.utc)
    old = (now - timedelta(days=40)).isoformat()
    fresh = (now - timedelta(days=5)).isoformat()
    assert _verification_due(None, 30, now) is True
    assert _verification_due(old, 30, now) is True
    assert _verification_due(fresh, 30, now) is False


def test_recent_phase_scans_pages_1_to_n(monkeypatch, tmp_path):
    board = BoardInfo("2", "Board", "https://example.com/board/2")
    thread = ThreadSummary("100", "T", "https://example.com/thread/100", 0, 1, "ts")
    context = BoardContext(
        board=board,
        first_page=BoardPageData(board=board, threads=[thread], current_page=1, last_page=5),
        last_page=5,
    )

    seen_pages = []
    processed_threads = []

    def fake_get_board_page(context, page, fetcher, base_url, cache):
        seen_pages.append(page)
        return BoardPageData(board=context.board, threads=[thread], current_page=page, last_page=context.last_page)

    def fake_process_thread(board, summary, start_page, fetcher, checkpoint, writer, counters, full_verify):
        processed_threads.append((summary.thread_id, start_page))
        return True

    monkeypatch.setattr("oatcake_scraper.cli._get_board_page", fake_get_board_page)
    monkeypatch.setattr("oatcake_scraper.cli._process_thread", fake_process_thread)

    writer = BoardWriter(tmp_path)
    checkpoint = CheckpointManager(tmp_path / "checkpoint.json")
    counters = RunCounters()

    consumed = _run_recent_delta_phase(
        [context],
        fetcher=None,
        checkpoint=checkpoint,
        writer=writer,
        counters=counters,
        base_url="https://example.com",
        recent_pages=2,
        backfill_pages=0,
        tail_overlap_pages=1,
        max_threads=0,
        consumed=0,
    )

    assert seen_pages == [1, 2]
    assert consumed == 2
    assert [thread_id for thread_id, _ in processed_threads] == ["100", "100"]


def test_global_backfill_cursor_resumes_across_boards(monkeypatch, tmp_path):
    board_a = BoardInfo("2", "A", "https://example.com/board/2")
    board_b = BoardInfo("3", "B", "https://example.com/board/3")
    a_new = ThreadSummary("201", "A-new", "https://example.com/thread/201", 0, 1, "t")
    a_old = ThreadSummary("200", "A-old", "https://example.com/thread/200", 0, 1, "t")
    b_only = ThreadSummary("300", "B-old", "https://example.com/thread/300", 0, 1, "t")

    context_a = BoardContext(
        board=board_a,
        first_page=BoardPageData(board=board_a, threads=[a_new, a_old], current_page=1, last_page=1),
        last_page=1,
    )
    context_b = BoardContext(
        board=board_b,
        first_page=BoardPageData(board=board_b, threads=[b_only], current_page=1, last_page=1),
        last_page=1,
    )

    processed = []

    def fake_get_board_page(context, page, fetcher, base_url, cache):
        return context.first_page

    def fake_process_thread(board, summary, start_page, fetcher, checkpoint, writer, counters, full_verify):
        processed.append(summary.thread_id)
        return True

    monkeypatch.setattr("oatcake_scraper.cli._get_board_page", fake_get_board_page)
    monkeypatch.setattr("oatcake_scraper.cli._process_thread", fake_process_thread)

    writer = BoardWriter(tmp_path)
    checkpoint = CheckpointManager(tmp_path / "checkpoint.json")
    counters = RunCounters()

    consumed = 0
    consumed = _run_global_backfill_phase(
        [context_a, context_b],
        fetcher=None,
        checkpoint=checkpoint,
        writer=writer,
        counters=counters,
        base_url="https://example.com",
        backfill_threads_per_run=1,
        tail_overlap_pages=1,
        max_threads=0,
        consumed=consumed,
    )
    checkpoint.save()

    consumed = _run_global_backfill_phase(
        [context_a, context_b],
        fetcher=None,
        checkpoint=checkpoint,
        writer=writer,
        counters=counters,
        base_url="https://example.com",
        backfill_threads_per_run=1,
        tail_overlap_pages=1,
        max_threads=0,
        consumed=consumed,
    )
    checkpoint.save()

    consumed = _run_global_backfill_phase(
        [context_a, context_b],
        fetcher=None,
        checkpoint=checkpoint,
        writer=writer,
        counters=counters,
        base_url="https://example.com",
        backfill_threads_per_run=1,
        tail_overlap_pages=1,
        max_threads=0,
        consumed=consumed,
    )

    assert processed == ["200", "201", "300"]
    assert consumed == 3


def test_verification_cursor_round_robin(monkeypatch, tmp_path):
    board_a = BoardInfo("2", "A", "https://example.com/board/2")
    board_b = BoardInfo("3", "B", "https://example.com/board/3")
    context_a = BoardContext(
        board=board_a,
        first_page=BoardPageData(board=board_a, threads=[], current_page=1, last_page=1),
        last_page=1,
    )
    context_b = BoardContext(
        board=board_b,
        first_page=BoardPageData(board=board_b, threads=[], current_page=1, last_page=1),
        last_page=1,
    )

    old = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
    fresh = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    refs = [
        {
            "board_id": "2",
            "thread_id": "10",
            "url": "https://example.com/thread/10",
            "title": "old-a",
            "replies": 1,
            "views": 1,
            "last_post_time": "t",
            "last_verified_full_at": old,
        },
        {
            "board_id": "2",
            "thread_id": "11",
            "url": "https://example.com/thread/11",
            "title": "fresh-a",
            "replies": 1,
            "views": 1,
            "last_post_time": "t",
            "last_verified_full_at": fresh,
        },
        {
            "board_id": "3",
            "thread_id": "20",
            "url": "https://example.com/thread/20",
            "title": "old-b",
            "replies": 1,
            "views": 1,
            "last_post_time": "t",
            "last_verified_full_at": old,
        },
    ]

    class DummyWriter:
        def list_thread_refs(self, board_id):
            return [ref for ref in refs if ref["board_id"] == board_id]

    processed = []

    def fake_process_thread(board, summary, start_page, fetcher, checkpoint, writer, counters, full_verify):
        processed.append(summary.thread_id)
        return True

    monkeypatch.setattr("oatcake_scraper.cli._process_thread", fake_process_thread)

    checkpoint = CheckpointManager(tmp_path / "checkpoint.json")
    counters = RunCounters()
    writer = DummyWriter()

    consumed = _run_full_verification_phase(
        [context_a, context_b],
        fetcher=None,
        checkpoint=checkpoint,
        writer=writer,
        counters=counters,
        verify_threads_per_run=1,
        full_verify_days=30,
        max_threads=0,
        consumed=0,
    )

    consumed = _run_full_verification_phase(
        [context_a, context_b],
        fetcher=None,
        checkpoint=checkpoint,
        writer=writer,
        counters=counters,
        verify_threads_per_run=1,
        full_verify_days=30,
        max_threads=0,
        consumed=consumed,
    )

    assert processed == ["10", "20"]
    assert consumed == 2
