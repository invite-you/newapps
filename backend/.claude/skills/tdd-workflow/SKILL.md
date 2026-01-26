---
name: tdd-workflow
description: Test-driven development workflow for Python with pytest.
---

# TDD Workflow Skill

Use when implementing features, fixing bugs, or refactoring code.

Based on obra/superpowers test-driven-development (MIT License).

## Core Mandate

**NO PRODUCTION CODE WITHOUT A FAILING TEST FIRST.**

This is absolute. No exceptions without explicit approval.

## The Red-Green-Refactor Cycle

### RED Phase: Write a Failing Test

1. Write ONE minimal test showing desired behavior
2. Run the test - it MUST fail
3. Verify it fails for the RIGHT reason (not typos)

```python
# test_date_parser.py
def test_parse_korean_date():
    """Parse Korean date format like '2024년 1월 15일'"""
    result = parse_date("2024년 1월 15일")
    assert result == datetime(2024, 1, 15)
```

### GREEN Phase: Make It Pass

1. Write the SIMPLEST code to pass the test
2. Don't add extra features or handle edge cases yet
3. Run the test - it MUST pass

```python
# date_parser.py
def parse_date(date_str: str) -> datetime:
    match = re.match(r"(\d+)년\s*(\d+)월\s*(\d+)일", date_str)
    if match:
        return datetime(int(match[1]), int(match[2]), int(match[3]))
    raise ValueError(f"Cannot parse: {date_str}")
```

### REFACTOR Phase: Clean Up

1. Remove duplication
2. Improve names
3. Extract helpers
4. Run ALL tests - they MUST still pass

## Verification Checklist

Before moving on:
- [ ] Test failed first for the right reason
- [ ] Implementation is minimal
- [ ] All tests pass
- [ ] Code is clean and readable

## Test Categories for This Project

### Unit Tests
Test pure functions in isolation:
```python
def test_normalize_rating():
    assert normalize_rating("4.5") == 4.5
    assert normalize_rating(None) is None
    assert normalize_rating("N/A") is None
```

### Integration Tests
Test database operations:
```python
@pytest.fixture
def db_connection():
    conn = get_test_connection()
    yield conn
    conn.rollback()
    conn.close()

def test_save_app_details(db_connection):
    app = {"app_id": "test123", "name": "Test App"}
    save_app_details(db_connection, app)

    result = get_app_by_id(db_connection, "test123")
    assert result["name"] == "Test App"
```

### Collector Tests
Test API interactions with mocks:
```python
@pytest.fixture
def mock_response():
    return {"results": [{"trackId": 123, "trackName": "App"}]}

def test_fetch_app_store_details(mock_response, mocker):
    mocker.patch("requests.get", return_value=Mock(
        status_code=200,
        json=lambda: mock_response
    ))

    result = fetch_app_details("123", "us")
    assert result["name"] == "App"
```

## Common Rationalizations (Don't Do These)

### "I'll write tests after"
Tests written after pass immediately, proving nothing.

### "I already tested it manually"
Manual testing is not reproducible or systematic.

### "It's too simple to need a test"
Simple code still needs verification. Tests also document behavior.

### "I'll just keep this code as reference"
Code without tests has unknown behavior. Delete and start fresh.

## Red Flags - Restart Required

If you find yourself:
- Writing code before tests
- Having tests pass on first run
- Modifying existing code to make new tests pass
- Writing tests that don't test the actual behavior

**Stop, delete the code, start over with TDD.**

## Coverage Requirements

- Minimum 80% for new code
- Focus on behavior, not line coverage
- Every public function should have at least one test
- Edge cases and error conditions need tests

## Running Tests

```bash
# Run all tests
python -m pytest

# Run with coverage
python -m pytest --cov=. --cov-report=term-missing

# Run specific test file
python -m pytest tests/test_date_parser.py

# Run tests matching pattern
python -m pytest -k "test_parse"
```
