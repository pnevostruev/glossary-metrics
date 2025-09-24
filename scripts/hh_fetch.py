#!/usr/bin/env python3

"""
HeadHunter (hh.ru) vacancies fetcher via public API.

Usage examples:
  python3 scripts/hh_fetch.py --text "Product Manager" --areas 1,2 --per-page 100 --delay 0.5
  python3 scripts/hh_fetch.py --text "Data Scientist" --areas 1 --date-from 2025-09-01 --date-to 2025-09-24

Defaults:
  - Saves CSV into ./output/hh_vacancies_<timestamp>.csv
  - Respects API pagination; backs off on HTTP 429/5xx

Notes:
  - Provide a meaningful User-Agent with a contact for good citizenship.
  - If you need more fields, consider fetching /vacancies/{id} details per item (slower).
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import datetime as dt
from typing import Dict, Any, Iterable, List, Optional

try:
    import requests  # type: ignore
except Exception as exc:
    sys.stderr.write(
        "requests is required. Install with: pip install requests\n"
    )
    raise


API_URL = "https://api.hh.ru/vacancies"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch vacancies from hh.ru API and save to CSV")
    parser.add_argument("--text", required=True, help="Search text (query)")
    parser.add_argument(
        "--areas",
        default="1",
        help="Comma-separated area IDs (e.g., '1,2' for Moscow, SPB). Default: 1",
    )
    parser.add_argument("--per-page", type=int, default=100, help="Items per page (max 100)")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Hard cap for pages to fetch (0-based pages; None uses API 'pages')",
    )
    parser.add_argument("--date-from", dest="date_from", default=None, help="YYYY-MM-DD filter")
    parser.add_argument("--date-to", dest="date_to", default=None, help="YYYY-MM-DD filter")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests in seconds")
    parser.add_argument(
        "--out",
        default=None,
        help="Output CSV path. Default: ./output/hh_vacancies_<timestamp>.csv",
    )
    parser.add_argument(
        "--user-agent",
        default="GlossaryMetricsVacFetcher/1.0 (+contact: your-email@example.com)",
        help="HTTP User-Agent to send in requests",
    )
    return parser.parse_args()


def ensure_output_path(out_path: Optional[str]) -> str:
    if out_path:
        directory = os.path.dirname(out_path) or "."
        os.makedirs(directory, exist_ok=True)
        return out_path
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join("output")
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, f"hh_vacancies_{timestamp}.csv")


def request_with_backoff(url: str, params: Dict[str, Any], headers: Dict[str, str], max_retries: int = 5) -> requests.Response:
    backoff = 1.0
    for attempt in range(max_retries):
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
            continue
        resp.raise_for_status()
    # last try
    resp.raise_for_status()
    return resp  # for type checker


def iter_vacancies(
    query_text: str,
    area_ids: List[str],
    per_page: int,
    user_agent: str,
    date_from: Optional[str],
    date_to: Optional[str],
    delay: float,
    max_pages: Optional[int],
) -> Iterable[Dict[str, Any]]:
    headers = {"User-Agent": user_agent}
    for area in area_ids:
        page = 0
        total_pages_for_area: Optional[int] = None
        while True:
            if max_pages is not None and page > max_pages:
                break
            params: Dict[str, Any] = {
                "text": query_text,
                "area": area,
                "per_page": per_page,
                "page": page,
            }
            if date_from:
                params["date_from"] = date_from
            if date_to:
                params["date_to"] = date_to

            resp = request_with_backoff(API_URL, params=params, headers=headers)
            data = resp.json()

            if total_pages_for_area is None:
                try:
                    total_pages_for_area = int(data.get("pages", 0))
                except Exception:
                    total_pages_for_area = 0

            for item in data.get("items", []):
                yield item

            page += 1
            if total_pages_for_area is not None and page >= total_pages_for_area:
                break
            time.sleep(delay)


def flatten_item(item: Dict[str, Any]) -> Dict[str, Any]:
    salary = item.get("salary") or {}
    employer = item.get("employer") or {}
    area = item.get("area") or {}
    snippet = item.get("snippet") or {}

    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "alternate_url": item.get("alternate_url"),
        "employer_id": employer.get("id"),
        "employer_name": employer.get("name"),
        "area_id": area.get("id"),
        "area_name": area.get("name"),
        "salary_from": salary.get("from"),
        "salary_to": salary.get("to"),
        "salary_currency": salary.get("currency"),
        "salary_gross": salary.get("gross"),
        "published_at": item.get("published_at"),
        "schedule": (item.get("schedule") or {}).get("name"),
        "employment": (item.get("employment") or {}).get("name"),
        "requirement": snippet.get("requirement"),
        "responsibility": snippet.get("responsibility"),
    }


def main() -> None:
    args = parse_args()
    out_path = ensure_output_path(args.out)

    area_ids = [a.strip() for a in args.areas.split(",") if a.strip()]
    if not area_ids:
        sys.stderr.write("No valid areas provided.\n")
        sys.exit(2)

    fieldnames = [
        "id",
        "name",
        "alternate_url",
        "employer_id",
        "employer_name",
        "area_id",
        "area_name",
        "salary_from",
        "salary_to",
        "salary_currency",
        "salary_gross",
        "published_at",
        "schedule",
        "employment",
        "requirement",
        "responsibility",
    ]

    total = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in iter_vacancies(
            query_text=args.text,
            area_ids=area_ids,
            per_page=args.per_page,
            user_agent=args.user_agent,
            date_from=args.date_from,
            date_to=args.date_to,
            delay=args.delay,
            max_pages=args.max_pages,
        ):
            writer.writerow(flatten_item(item))
            total += 1

    print(f"Saved {total} rows to {out_path}")


if __name__ == "__main__":
    main()


