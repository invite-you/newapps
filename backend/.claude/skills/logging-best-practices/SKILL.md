---
name: logging-best-practices
description: Structured logging patterns for data collection pipelines.
---

# Logging Best Practices Skill

Use when implementing logging, debugging issues, or reviewing log output.

Based on boristane/agent-skills logging-best-practices (MIT License).

## Core Principle: Wide Events

**Emit one context-rich event per operation** rather than scattered log statements.

```python
# BAD: Scattered logs
logging.info("Starting collection")
logging.info(f"App ID: {app_id}")
logging.info(f"Platform: {platform}")
logging.info("Collection complete")

# GOOD: Single wide event
logging.info("Collection completed", extra={
    "app_id": app_id,
    "platform": platform,
    "duration_ms": duration,
    "status": "success",
    "reviews_collected": count
})
```

## Key Principles

### High Cardinality Fields (CRITICAL)

Include fields with many unique values for debugging:
- `app_id` - Millions of unique values
- `request_id` - Unique per request
- `session_id` - Unique per collection run
- `language` - Locale being collected

### Business Context (CRITICAL)

Log meaningful business data, not just technical details:
```python
logging.info("Review collection completed", extra={
    "app_id": app_id,
    "platform": platform,
    "reviews_collected": len(reviews),
    "date_range": f"{start_date} to {end_date}",
    "collection_policy": policy_name,
    "retry_attempt": attempt
})
```

### Environment Context (CRITICAL)

Include deployment info in every log:
```python
import os

BASE_CONTEXT = {
    "service": "app-collector",
    "version": os.getenv("APP_VERSION", "unknown"),
    "hostname": socket.gethostname(),
    "pid": os.getpid()
}
```

## Single Logger Pattern

Use one logger instance throughout:
```python
# utils/logger.py
import logging
import json

class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if hasattr(record, "extra"):
            log_data.update(record.extra)
        return json.dumps(log_data)

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
```

## Two Log Levels Only

- **INFO**: Normal operations, metrics, business events
- **ERROR**: Failures requiring attention

Avoid DEBUG in production (too verbose) and WARNING (unclear action).

## Logging Patterns for Data Collection

### Collection Start/End
```python
def collect_apps(platform: str, limit: int):
    session_id = str(uuid.uuid4())
    start_time = time.time()

    logger.info("Collection started", extra={
        "session_id": session_id,
        "platform": platform,
        "limit": limit
    })

    try:
        result = do_collection()
        logger.info("Collection completed", extra={
            "session_id": session_id,
            "platform": platform,
            "apps_collected": result.count,
            "duration_s": time.time() - start_time,
            "status": "success"
        })
    except Exception as e:
        logger.error("Collection failed", extra={
            "session_id": session_id,
            "platform": platform,
            "error_type": type(e).__name__,
            "error_message": str(e),
            "duration_s": time.time() - start_time
        })
        raise
```

### API Request/Response
```python
def fetch_with_logging(url: str, params: dict):
    request_id = str(uuid.uuid4())

    try:
        response = requests.get(url, params=params, timeout=30)

        logger.info("API request completed", extra={
            "request_id": request_id,
            "url": url,
            "status_code": response.status_code,
            "response_size": len(response.content),
            "duration_ms": response.elapsed.total_seconds() * 1000
        })

        return response

    except requests.RequestException as e:
        logger.error("API request failed", extra={
            "request_id": request_id,
            "url": url,
            "error_type": type(e).__name__,
            "error_message": str(e)
        })
        raise
```

### Database Operations
```python
def save_batch(conn, items: list):
    start = time.time()

    try:
        with conn.cursor() as cur:
            cur.executemany(query, items)
        conn.commit()

        logger.info("Batch saved", extra={
            "table": "apps",
            "row_count": len(items),
            "duration_ms": (time.time() - start) * 1000
        })

    except Exception as e:
        conn.rollback()
        logger.error("Batch save failed", extra={
            "table": "apps",
            "row_count": len(items),
            "error_type": type(e).__name__,
            "error_message": str(e)
        })
        raise
```

## Anti-Patterns to Avoid

### Don't scatter logs
```python
# BAD
logger.info("Starting...")
logger.info(f"Processing {item}")
logger.info("Step 1 done")
logger.info("Step 2 done")
logger.info("Finished")

# GOOD
logger.info("Operation completed", extra={
    "item": item,
    "steps_completed": 2,
    "status": "success"
})
```

### Don't log sensitive data
```python
# BAD
logger.info(f"API key: {api_key}")

# GOOD
logger.info("Using API key", extra={
    "key_prefix": api_key[:4] + "****"
})
```

### Don't use print()
```python
# BAD
print(f"Debug: {value}")

# GOOD
logger.info("Debug value", extra={"value": value})
```

## Queryable Log Patterns

Structure logs for easy searching:
```bash
# Find all failed collections for iOS
jq 'select(.platform == "ios" and .status == "failed")'

# Find slow API requests (>5s)
jq 'select(.duration_ms > 5000)'

# Find errors by app
jq 'select(.level == "ERROR" and .app_id == "123456")'
```
