# Project Plan

This document outlines the phased roadmap and backlog for the proboards-scraper project.

## Phase 0 — Prototype

- [x] Basic single-board scraper
- [x] JSON export

## Phase 1 — Crawl robustness

- [ ] Implement board pagination to crawl all pages of a board
- [ ] Improve thread pagination detection
- [ ] Add retry and backoff strategy for network errors
- [ ] Add better logging and progress output

## Phase 2 — Data model

- [ ] Define a canonical schema for boards, threads and posts
- [ ] Add stable identifiers (permalinks or post IDs)
- [ ] Preserve raw HTML along with normalized text for each post

## Phase 3 — Persistence

- [ ] Support JSONL output
- [ ] Implement a SQLite storage backend
- [ ] Add checkpoint/resume support for long-running crawls

## Phase 4 — Usability

- [ ] Add command-line arguments for board URLs and output options
- [ ] Support configuration files
- [ ] Improve documentation and examples

## Phase 5 — Testing

- [ ] Collect HTML fixtures for parser tests
- [ ] Write parser unit tests against fixtures
- [ ] Add end-to-end smoke tests

## Phase 6 — Advanced support

- [ ] Download attachments and media linked in posts
- [ ] Support login/session handling for private forums
- [ ] Implement incremental recrawl capabilities to fetch only new posts
