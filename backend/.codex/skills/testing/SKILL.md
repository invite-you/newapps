---
name: testing
description: Testing
---

# Testing

## Requirements
- pytest for unit/integration tests
- pytest-cov for coverage
- Playwright (Python) only when E2E is needed

## Commands
```bash
python -m pytest
python -m pytest --cov=. --cov-report=term-missing
```

## Coverage
- Minimum 80% for new code

## Test types
- Unit tests for pure functions
- Integration tests for FastAPI routes using httpx.AsyncClient

Example:
```python
import pytest
from httpx import AsyncClient
from app.main import app

@pytest.mark.asyncio
async def test_health():
    async with AsyncClient(app=app, base_url="http://test") as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
```
