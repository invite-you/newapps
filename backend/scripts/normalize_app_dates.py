#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Play Store 앱의 비표준 날짜 문자열을 ISO 형식으로 정규화합니다.
"""
import argparse
import os
import re
import sys
from typing import List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.app_details_db import get_connection, release_connection, normalize_date_format

ISO_PREFIX = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}"


def _is_iso(value: str) -> bool:
    return bool(re.match(ISO_PREFIX, value or ""))


def fetch_rows(cursor, last_id: int, batch_size: int, platform: str) -> List[dict]:
    cursor.execute(
        """
        SELECT app_id, id, updated_date, release_date
        FROM apps
        WHERE platform = %s
          AND id > %s
          AND (
              (updated_date IS NOT NULL AND updated_date != '' AND updated_date !~ %s)
              OR (release_date IS NOT NULL AND release_date != '' AND release_date !~ %s)
          )
        ORDER BY id
        LIMIT %s
        """,
        (platform, last_id, ISO_PREFIX, ISO_PREFIX, batch_size),
    )
    return cursor.fetchall()


def build_updates(rows: List[dict]) -> Tuple[List[tuple], dict, List[dict]]:
    updates: List[tuple] = []
    stats = {
        "updated_date_fixed": 0,
        "release_date_fixed": 0,
        "updated_date_failed": 0,
        "release_date_failed": 0,
        "rows_with_changes": 0,
    }
    failed_samples: List[dict] = []

    for row in rows:
        updated_raw = row.get("updated_date")
        release_raw = row.get("release_date")

        new_updated = None
        new_release = None

        if updated_raw and not _is_iso(updated_raw):
            new_updated = normalize_date_format(updated_raw)
            if new_updated and new_updated != updated_raw:
                stats["updated_date_fixed"] += 1
            elif not new_updated:
                stats["updated_date_failed"] += 1
                failed_samples.append(
                    {"app_id": row["app_id"], "id": row["id"], "field": "updated_date", "value": updated_raw}
                )

        if release_raw and not _is_iso(release_raw):
            new_release = normalize_date_format(release_raw)
            if new_release and new_release != release_raw:
                stats["release_date_fixed"] += 1
            elif not new_release:
                stats["release_date_failed"] += 1
                failed_samples.append(
                    {"app_id": row["app_id"], "id": row["id"], "field": "release_date", "value": release_raw}
                )

        if new_updated or new_release:
            stats["rows_with_changes"] += 1
            updates.append((new_updated, new_release, row["app_id"], row["id"]))

    return updates, stats, failed_samples


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize non-ISO app dates for Play Store records.")
    parser.add_argument("--apply", action="store_true", help="Apply updates to the database.")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per batch.")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to process (0 = unlimited).")
    parser.add_argument("--platform", default="play_store", help="Target platform (default: play_store).")
    parser.add_argument("--max-samples", type=int, default=20, help="Max failed samples to show.")
    args = parser.parse_args()

    total_rows = 0
    total_updates = 0
    aggregate = {
        "updated_date_fixed": 0,
        "release_date_fixed": 0,
        "updated_date_failed": 0,
        "release_date_failed": 0,
        "rows_with_changes": 0,
    }
    failed_samples: List[dict] = []

    conn = get_connection()
    last_id = 0
    try:
        while True:
            with conn.cursor() as cursor:
                rows = fetch_rows(cursor, last_id, args.batch_size, args.platform)

            if not rows:
                break

            total_rows += len(rows)
            last_id = max(row["id"] for row in rows)

            updates, stats, failures = build_updates(rows)
            for key in aggregate:
                aggregate[key] += stats[key]
            failed_samples.extend(failures)

            if updates and args.apply:
                with conn.cursor() as cursor:
                    cursor.executemany(
                        """
                        UPDATE apps
                        SET updated_date = COALESCE(%s, updated_date),
                            release_date = COALESCE(%s, release_date)
                        WHERE app_id = %s AND id = %s
                        """,
                        updates,
                    )
                conn.commit()
                total_updates += len(updates)

            if args.limit and total_rows >= args.limit:
                break

        if not args.apply:
            conn.rollback()
    finally:
        release_connection(conn)

    print("SUMMARY")
    print(f"platform={args.platform}")
    print(f"rows_scanned={total_rows}")
    print(f"rows_updated={total_updates}" if args.apply else "rows_updated=0 (dry-run)")
    for key, value in aggregate.items():
        print(f"{key}={value}")

    if failed_samples:
        print("FAILED_SAMPLES")
        for sample in failed_samples[: args.max_samples]:
            print(sample)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
