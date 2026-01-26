---
name: postgres-best-practices
description: PostgreSQL performance optimization patterns for psycopg and batch data operations.
---

# PostgreSQL Best Practices Skill

Use when writing queries, designing schemas, managing connections, or optimizing database performance.

Based on Supabase's postgres-best-practices (MIT License).

## Priority Categories

1. **Query Performance** (CRITICAL) - Indexing, query optimization
2. **Connection Management** (CRITICAL) - Pooling, timeouts
3. **Schema Design** (HIGH) - Table structure, partitioning
4. **Concurrency & Locking** (MEDIUM) - Transaction isolation
5. **Data Access Patterns** (MEDIUM) - Batch operations, pagination

## Query Performance Rules

### Use appropriate indexes for WHERE clauses
```sql
-- BAD: No index on frequently queried column
SELECT * FROM apps WHERE platform = 'ios' AND collected_at > '2024-01-01';

-- GOOD: Composite index for query pattern
CREATE INDEX idx_apps_platform_collected ON apps(platform, collected_at);
```

### Use covering indexes to avoid table lookups
```sql
-- Include frequently selected columns
CREATE INDEX idx_apps_covering ON apps(app_id, platform) INCLUDE (name, rating);
```

### Use partial indexes for filtered queries
```sql
-- Index only active/relevant rows
CREATE INDEX idx_failed_apps_pending ON failed_apps(app_id)
WHERE retry_count < 5;
```

## Connection Management Rules

### Always use connection pooling
```python
# BAD: Creating new connections per operation
conn = psycopg.connect(dsn)

# GOOD: Use connection pool
from psycopg_pool import ConnectionPool
pool = ConnectionPool(dsn, min_size=2, max_size=10)
with pool.connection() as conn:
    ...
```

### Set appropriate timeouts
```python
conn = psycopg.connect(
    dsn,
    connect_timeout=10,
    options="-c statement_timeout=30000"  # 30 seconds
)
```

### Use context managers for automatic cleanup
```python
with pool.connection() as conn:
    with conn.cursor() as cur:
        cur.execute(query)
        # Auto-commit/rollback on exit
```

## Schema Design Rules

### Use table partitioning for large datasets
```sql
-- Partition reviews by month
CREATE TABLE app_reviews (
    id BIGSERIAL,
    app_id TEXT NOT NULL,
    review_date DATE NOT NULL,
    content TEXT
) PARTITION BY RANGE (review_date);

CREATE TABLE app_reviews_2024_01 PARTITION OF app_reviews
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

### Choose appropriate data types
```sql
-- Use TEXT for variable strings, avoid VARCHAR limits
app_id TEXT NOT NULL,

-- Use TIMESTAMPTZ for timestamps
collected_at TIMESTAMPTZ DEFAULT NOW(),

-- Use appropriate numeric types
rating NUMERIC(2,1),  -- For 4.5 ratings
download_count BIGINT
```

## Data Access Patterns

### Use batch operations for bulk inserts
```python
# BAD: Individual inserts
for app in apps:
    cur.execute("INSERT INTO apps VALUES (%s, %s)", (app.id, app.name))

# GOOD: executemany or COPY
cur.executemany(
    "INSERT INTO apps (id, name) VALUES (%s, %s)",
    [(a.id, a.name) for a in apps]
)

# BEST: COPY for large datasets
with cur.copy("COPY apps (id, name) FROM STDIN") as copy:
    for app in apps:
        copy.write_row((app.id, app.name))
```

### Use cursor-based pagination, not OFFSET
```python
# BAD: OFFSET becomes slow on large tables
SELECT * FROM apps ORDER BY id LIMIT 100 OFFSET 10000;

# GOOD: Cursor-based pagination
SELECT * FROM apps WHERE id > %s ORDER BY id LIMIT 100;
```

### Use UPSERT for insert-or-update operations
```sql
INSERT INTO apps (app_id, name, rating)
VALUES (%s, %s, %s)
ON CONFLICT (app_id)
DO UPDATE SET name = EXCLUDED.name, rating = EXCLUDED.rating
WHERE apps.rating IS DISTINCT FROM EXCLUDED.rating;
```

## Monitoring & Diagnostics

### Enable pg_stat_statements
```sql
-- Identify slow queries
SELECT query, calls, mean_exec_time, total_exec_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;
```

### Use EXPLAIN ANALYZE for query optimization
```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM apps WHERE platform = 'ios';
```

### Monitor table bloat and run maintenance
```sql
-- Check for bloat
SELECT relname, n_dead_tup, last_vacuum, last_autovacuum
FROM pg_stat_user_tables
WHERE n_dead_tup > 1000;

-- Manual vacuum if needed
VACUUM ANALYZE apps;
```

## Anti-Patterns to Avoid

- SELECT * when only specific columns needed
- Missing indexes on foreign keys
- N+1 queries in loops (use JOINs or batch loading)
- Long-running transactions holding locks
- Using OFFSET for pagination on large tables
- Storing JSON when relational structure is better
