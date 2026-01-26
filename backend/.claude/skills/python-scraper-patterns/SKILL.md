---
name: python-scraper-patterns
description: Best practices for web scraping and data collection pipelines in Python.
---

# Python Scraper Patterns Skill

Use when building scrapers, collectors, or data pipelines.

## Core Principles

1. **Respect rate limits** - Don't overwhelm target servers
2. **Handle failures gracefully** - Network is unreliable
3. **Idempotent operations** - Safe to re-run
4. **Incremental collection** - Track progress, resume from failures

## Request Patterns

### Use session for connection reuse
```python
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session() -> requests.Session:
    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session
```

### Set appropriate timeouts
```python
# Always set timeouts - never wait forever
response = session.get(url, timeout=(5, 30))  # (connect, read)
```

### Handle rate limiting
```python
import time

def fetch_with_rate_limit(session, url, delay=1.0):
    response = session.get(url, timeout=30)

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        time.sleep(retry_after)
        return fetch_with_rate_limit(session, url, delay)

    time.sleep(delay)  # Polite delay between requests
    return response
```

### Rotate user agents if needed
```python
USER_AGENTS = [
    "Mozilla/5.0 (compatible; AppCollector/1.0)",
    # Add more if needed
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9"
    }
```

## Error Handling Patterns

### Classify errors for retry decisions
```python
from enum import Enum

class ErrorType(Enum):
    TRANSIENT = "transient"      # Retry
    PERMANENT = "permanent"      # Don't retry
    RATE_LIMIT = "rate_limit"    # Wait and retry

def classify_error(e: Exception) -> ErrorType:
    if isinstance(e, requests.exceptions.Timeout):
        return ErrorType.TRANSIENT
    if isinstance(e, requests.exceptions.ConnectionError):
        return ErrorType.TRANSIENT
    if hasattr(e, 'response') and e.response.status_code == 429:
        return ErrorType.RATE_LIMIT
    if hasattr(e, 'response') and e.response.status_code == 404:
        return ErrorType.PERMANENT
    return ErrorType.TRANSIENT  # Default to retry
```

### Exponential backoff for retries
```python
import time
import random

def retry_with_backoff(func, max_retries=5, base_delay=1.0):
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise

            error_type = classify_error(e)
            if error_type == ErrorType.PERMANENT:
                raise

            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            logging.warning(f"Retry {attempt + 1}/{max_retries} after {delay:.1f}s")
            time.sleep(delay)
```

### Track failed items for later retry
```python
def collect_with_failure_tracking(items: list, collector_func):
    succeeded = []
    failed = []

    for item in items:
        try:
            result = collector_func(item)
            succeeded.append(result)
        except Exception as e:
            failed.append({
                "item": item,
                "error": str(e),
                "error_type": type(e).__name__,
                "timestamp": datetime.now().isoformat()
            })

    return succeeded, failed
```

## Data Parsing Patterns

### Defensive parsing with defaults
```python
def safe_get(data: dict, *keys, default=None):
    """Safely navigate nested dictionaries."""
    result = data
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key)
        else:
            return default
        if result is None:
            return default
    return result

# Usage
rating = safe_get(response, "results", 0, "averageUserRating", default=0.0)
```

### Parse dates with multiple format support
```python
from datetime import datetime
import re

DATE_PATTERNS = [
    (r"(\d{4})-(\d{2})-(\d{2})", "%Y-%m-%d"),
    (r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일", None),  # Korean
    (r"(\d{1,2})/(\d{1,2})/(\d{4})", "%m/%d/%Y"),
]

def parse_date(date_str: str) -> datetime | None:
    if not date_str:
        return None

    date_str = date_str.strip()

    for pattern, fmt in DATE_PATTERNS:
        match = re.search(pattern, date_str)
        if match:
            if fmt:
                return datetime.strptime(match.group(), fmt)
            else:
                # Handle special formats
                groups = match.groups()
                return datetime(int(groups[0]), int(groups[1]), int(groups[2]))

    return None
```

### Normalize text data
```python
import unicodedata
import re

def normalize_text(text: str) -> str:
    if not text:
        return ""

    # Normalize unicode
    text = unicodedata.normalize("NFKC", text)

    # Remove control characters
    text = "".join(c for c in text if unicodedata.category(c) != "Cc")

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text
```

## Batch Processing Patterns

### Process in chunks to limit memory
```python
def chunked(iterable, size):
    """Yield successive chunks of given size."""
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, size))
        if not chunk:
            break
        yield chunk

# Usage
for batch in chunked(app_ids, 100):
    results = collect_batch(batch)
    save_batch(results)
```

### Use generators for memory efficiency
```python
def stream_from_db(query: str, batch_size=1000):
    """Stream results instead of loading all into memory."""
    with get_connection() as conn:
        with conn.cursor(name="stream_cursor") as cur:
            cur.itersize = batch_size
            cur.execute(query)

            for row in cur:
                yield row
```

### Progress tracking
```python
from tqdm import tqdm

def collect_with_progress(items: list, collector_func):
    results = []

    for item in tqdm(items, desc="Collecting"):
        try:
            result = collector_func(item)
            results.append(result)
        except Exception as e:
            logging.error(f"Failed: {item} - {e}")

    return results
```

## Idempotency Patterns

### Use upsert for saves
```python
def upsert_app(conn, app: dict):
    """Insert or update - safe to call multiple times."""
    query = """
        INSERT INTO apps (app_id, name, rating, updated_at)
        VALUES (%(app_id)s, %(name)s, %(rating)s, NOW())
        ON CONFLICT (app_id) DO UPDATE SET
            name = EXCLUDED.name,
            rating = EXCLUDED.rating,
            updated_at = NOW()
        WHERE apps.rating IS DISTINCT FROM EXCLUDED.rating
    """
    with conn.cursor() as cur:
        cur.execute(query, app)
    conn.commit()
```

### Track collection state
```python
def get_last_collection_point(conn, collection_type: str) -> str | None:
    """Get last successfully processed item."""
    query = """
        SELECT last_item_id FROM collection_state
        WHERE collection_type = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (collection_type,))
        row = cur.fetchone()
        return row[0] if row else None

def update_collection_point(conn, collection_type: str, item_id: str):
    """Update progress checkpoint."""
    query = """
        INSERT INTO collection_state (collection_type, last_item_id, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (collection_type) DO UPDATE SET
            last_item_id = EXCLUDED.last_item_id,
            updated_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(query, (collection_type, item_id))
    conn.commit()
```

## Anti-Patterns to Avoid

- Making requests without timeouts
- Loading entire datasets into memory
- Ignoring rate limits
- No retry logic for transient failures
- Losing progress on errors (no checkpointing)
- Hardcoded delays instead of dynamic rate limiting
- Storing unparsed/unnormalized data
