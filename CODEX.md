# CODEX.md

## Mission

This repository exists to archive ProBoards forum content in a polite, resumable and verifiable way.

## Priorities

1. Correctness of extracted data.
2. Resumability and recoverability.
3. Test coverage with saved fixtures.
4. Clear CLI and configuration.
5. Performance only after correctness is established.

## Guardrails

- Do not remove crawl delays without replacing them with configurable rate limiting.
- Do not add concurrency by default.
- Do not hardcode assumptions about HTML selectors without tests or fixtures.
- Do not discard raw post HTML unless there is an explicit reason.
- Do not change the output schema casually; document any schema changes.

## Preferred architecture

- Separate fetching logic from parsing logic.
- Separate parsing logic from persistence/storage.
- Isolate persistence from the command-line interface.

## Before submitting changes

Run:

- Unit tests.
- Parser fixture tests.
- Lint/format/type checks, if configured.

## Good next tasks

- Implement board pagination support.
- Improve thread pagination detection.
- Add stable post identifiers.
- Add checkpoint/resume file support.
- Implement a SQLite storage backend.
- Add fixture-based parser tests.
- Add CLI argument parsing and configuration file support.
