---
name: fastapi-backend
description: FastAPI backend patterns for routing, validation, services, and tests.
---

# FastAPI Backend Skill

Use when adding endpoints, schemas, dependencies, or middleware.

## Conventions
- Pydantic v2 for request/response models
- APIRouter per domain
- Validate input at the boundary
- Return JSON with a consistent envelope when useful

Example response model:
```python
from pydantic import BaseModel
from typing import Generic, Optional, TypeVar

T = TypeVar("T")

class ApiResponse(BaseModel, Generic[T]):
    success: bool
    data: Optional[T] = None
    error: Optional[str] = None
```

## Preferred structure (when ready)
- app/main.py
- app/api/routes/
- app/schemas/
- app/services/
- app/db/
- tests/

## Error handling
- Use HTTPException for expected errors
- Add a global exception handler for unexpected errors

## Testing
- Use pytest + httpx.AsyncClient for route tests
- Keep tests close to domain modules
