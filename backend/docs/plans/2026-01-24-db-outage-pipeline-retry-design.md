# DB Outage Pipeline Retry Design

## Context
- DB outages cause per-app OperationalError logging and skipped writes.
- During outages, collectors keep fetching external data that cannot be persisted.
- Goal: stop the pipeline on DB unavailability, then retry in the next cycle.

## Goals
- Detect DB unavailability centrally.
- Retry 3 times with backoff (5s, 10s, 20s).
- Abort the current cycle on failure and wait 60s before the next cycle.
- Avoid per-app skip for DB outages.

## Non-goals
- Change app-level error handling for HTTP/parsing errors.
- Re-architect review collection scheduling.

## Proposed Design
### DB Layer
- Introduce DatabaseUnavailableError.
- Wrap DB calls in retry logic for OperationalError/AdminShutdown/connection refused/closed unexpectedly.
- Reset global connection on retry and propagate error after 3 failures.

### Collector Layer
- Do not swallow DatabaseUnavailableError.
- In parallel mode, if one child fails, terminate the other and exit non-zero.

### Pipeline Layer
- On DatabaseUnavailableError, mark the cycle as failed.
- Cooldown 60s (or max of 60s and configured interval), then start next cycle.

### Logging
- Single wide event per failure with session_id, attempt, backoff_s, error_type, host.
- Suppress per-app errors during outage once DB is marked unavailable.

## Testing
- Unit test: stub DB to raise OperationalError 3 times -> DatabaseUnavailableError.
- Integration test (manual): stop Postgres, observe retries and cycle abort, resume and confirm next cycle runs.
