---
name: coding-style
description: Coding Style (Python)
---

# Coding Style (Python)

## Principles
- Prefer clarity over cleverness
- Keep functions small (<= 50 lines)
- Favor immutable updates when practical
- Use type hints for public functions and module APIs

## Naming
- snake_case for variables/functions
- PascalCase for classes
- UPPER_SNAKE_CASE for constants

## Error handling
```python
import logging

def load_data(path: str) -> dict:
    try:
        return read_json(path)
    except Exception as exc:
        logging.exception("Failed to load data")
        raise RuntimeError("Failed to load data") from exc
```

## Input validation
```python
from pydantic import BaseModel, Field

class CreateItem(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    quantity: int = Field(ge=0)
```

## Logging
- Do not use print for production code
- Use logging with structured context

## File size
- Prefer 200-400 lines per file, max 800
