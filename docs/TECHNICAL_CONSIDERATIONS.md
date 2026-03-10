# Technical Considerations

This document captures engineering notes and constraints for developing the proboards-scraper.

## HTML variability

ProBoards forums can be customized with themes and plugins. CSS classes and HTML structures may change across forums. Avoid hardcoding selectors without verification. Collect sample pages and create fixtures for parser tests.

## Pagination

Board pages and thread pages paginate separately. Do not assume numeric suffixes in URLs; always parse navigation links or presence of "page/x" segments. Ensure the crawler stops when no new posts are found.

## Stable identifiers

Prefer stable identifiers for deduplication and linking:

- The board URL itself
- The thread URL (without trailing page or query parameters)
- Post permalinks or post IDs when available

If the forum theme does not expose a post ID, consider hashing content with timestamp as a fallback.

## Content preservation

When extracting post data, preserve:

- Normalized text (stripped of markup)
- Raw HTML (for future re-parsing or styling)
- Source timestamps in raw string form and as parsed datetime values
- Author names as displayed

Do not discard raw data prematurely; normalization logic may change.

## Resumability

Long-running crawls should be restartable and idempotent. Use checkpoint files or a persistent database to record:

- Completed boards and threads
- Last successfully scraped page in each thread
- Errors encountered

Upon restart, the crawler should skip already processed items and resume from the last checkpoint.

## Politeness and rate limiting

Respect the target forum by:

- Using a descriptive User-Agent header that identifies this as an archive tool
- Adding configurable delays between requests
- Implementing exponential backoff on HTTP errors and respect 429/503 responses
- Allowing the user to configure maximum concurrent requests (default to 1)

## Testing and fixtures

Parser and crawler logic should be tested against static HTML fixtures saved from real ProBoards pages. Avoid relying solely on live forums for tests. Consider adding integration tests that run against a local mirror of sample forums.

## Storage formats

Support multiple output formats:

- JSONL for simple streaming exports
- SQLite for structured relational data and resume support
- Raw HTML snapshots for archival and debugging

Choose the format based on the archive size and downstream usage.

## Authentication and private forums

Eventually support logging in to private forums. Implement session management and cookie handling, but require explicit credentials and avoid storing them in code. Always respect terms of service.

## Attachments and media

Posts may contain embedded images or file attachments. Provide optional downloading of these assets, respecting file sizes and rate limits.
