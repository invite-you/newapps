# Security

## Secrets
- No hardcoded secrets
- Use env vars and fail fast if missing

```python
import os

api_key = os.getenv("API_KEY")
if not api_key:
    raise RuntimeError("API_KEY missing")
```

## Input validation
- Validate all input with Pydantic

## SQL
- Use parameterized queries only

## HTML
- Keep template autoescape on
- Sanitize any user-provided HTML
