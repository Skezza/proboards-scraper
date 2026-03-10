# proboards-scraper

Archive ProBoards forums into structured data for preservation and analysis.

## Status

Early-stage prototype.

## Current capabilities

- Fetch a single board page
- Extract thread links from that board
- Visit each thread and iterate through pages
- Extract basic post data (author, date, content)
- Export to a JSON file

## Planned capabilities

- Board pagination (crawl all pages of a board)
- Robust thread pagination detection
- Resume/checkpoint support for long archives
- SQLite and JSONL output backends
- Attachment and media archiving
- Authenticated scraping for private forums
- CLI arguments and configuration file support
- Tests with HTML fixtures

## Goals

- Produce reliable archival output that preserves forum data
- Preserve correctness over crawl speed
- Make scraper behavior explicit and testable
- Be easy for humans and coding agents to extend safely

## Non-goals

- High-speed crawling or concurrency by default
- Bypassing permissions or access controls
- Supporting every forum theme without fixtures

## Quick start

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the scraper against a board URL:

```bash
python scraper.py https://exampleforum.proboards.com/board/1/general-discussion
```

This will produce a JSON file with threads and posts.

## Project structure

```
scraper.py            # simple scraper script
requirements.txt      # Python dependencies
README.md             # project overview
CODEX.md              # agent instruction manual
docs/
  PROJECT_PLAN.md     # phased roadmap and backlog
  TECHNICAL_CONSIDERATIONS.md  # design notes and guidelines
```

## Roadmap

For detailed plans and milestones, see [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md).
