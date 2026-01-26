---
name: systematic-debugging
description: Root cause analysis methodology for debugging data collection and scraping issues.
---

# Systematic Debugging Skill

Use when debugging errors, investigating failures, or troubleshooting data collection issues.

Based on obra/superpowers systematic-debugging (MIT License).

## Core Principle

**NO FIXES WITHOUT ROOT CAUSE INVESTIGATION FIRST.**

Symptom-level patches create technical debt and mask underlying problems.

## Four Mandatory Phases

### Phase 1: Root Cause Investigation

1. **Read error messages thoroughly**
   - Full stack traces, not just the last line
   - Error codes and their documentation
   - Timestamps and request context

2. **Reproduce consistently**
   - Document exact reproduction steps
   - Identify conditions (specific app IDs, languages, etc.)
   - Check if issue is intermittent or consistent

3. **Check recent changes**
   - Git log for recent commits
   - Configuration changes
   - External API changes (rate limits, response format)

4. **Gather evidence at component boundaries**
   ```python
   # Add diagnostic logging at boundaries
   logging.debug(f"API request: {url}, headers: {headers}")
   logging.debug(f"API response: status={resp.status_code}, body={resp.text[:500]}")
   ```

5. **Trace data flow backward**
   - Start from the error location
   - Follow the call stack up
   - Identify where data becomes invalid

### Phase 2: Pattern Analysis

1. **Find working examples**
   - Compare failing vs successful requests
   - Identify what's different (app ID, locale, timing)

2. **Compare against references**
   - Check API documentation
   - Compare with known-good implementations
   - Review similar code in the codebase

3. **Identify all differences**
   - Headers, parameters, encoding
   - Timing, ordering, state

4. **Understand dependencies**
   - External services state
   - Database state
   - Network conditions

### Phase 3: Hypothesis Testing

1. **Form a single clear hypothesis**
   - "The error occurs because X"
   - Be specific, not vague

2. **Test with minimal changes**
   - Change ONE variable at a time
   - Log before and after

3. **Verify results**
   - Did the change fix the issue?
   - Did it introduce new issues?
   - Is it reproducible?

### Phase 4: Implementation

1. **Create a failing test case first**
   ```python
   def test_handles_malformed_date():
       # Reproduces the bug
       result = parse_date("Invalid Date Format")
       assert result is None  # Should not crash
   ```

2. **Implement single fix addressing root cause**
   - Not a workaround
   - Not multiple changes at once

3. **Verify the fix**
   - Original test passes
   - No regression in other tests
   - Manual verification if needed

## Critical Guardrails

### Red Flags - Stop and Restart

- Proposing solutions before investigation
- Attempting multiple simultaneous fixes
- Making changes without understanding why
- Ignoring error messages or logs

### Three-Fix Rule

If three or more fix attempts fail:

**STOP. This is NOT a failed hypothesis - this is wrong architecture.**

- Step back and reassess the problem
- Consider if the approach is fundamentally flawed
- Discuss with team before continuing

## Debugging Data Collection Issues

### Common Root Causes

1. **Rate limiting**
   - Check response headers for rate limit info
   - Review request timing and patterns

2. **Response format changes**
   - API may have changed without notice
   - Compare current vs expected response structure

3. **Network issues**
   - Timeouts, connection resets
   - DNS resolution failures
   - Check network binding configuration

4. **Data parsing errors**
   - Unexpected null values
   - Encoding issues (UTF-8, special characters)
   - Date format variations by locale

5. **State management**
   - Database connection pool exhaustion
   - Cursor position issues
   - Transaction isolation problems

### Diagnostic Checklist

```python
# 1. Log the full request
logging.debug(f"Request: {method} {url}")
logging.debug(f"Headers: {headers}")
logging.debug(f"Body: {body}")

# 2. Log the full response
logging.debug(f"Response status: {resp.status_code}")
logging.debug(f"Response headers: {resp.headers}")
logging.debug(f"Response body: {resp.text[:1000]}")

# 3. Check database state
logging.debug(f"DB pool: active={pool.get_stats()}")

# 4. Track timing
start = time.time()
# ... operation ...
logging.debug(f"Operation took {time.time() - start:.2f}s")
```

## Time Investment

Systematic debugging: 15-30 minutes, 95% first-time success
Guess-and-check: 2-3 hours, 40% success rate

**The systematic approach is faster even under time pressure.**
