#!/usr/bin/env python3

"""
HeadHunter (hh.ru) vacancies fetcher via public API.

Usage examples:
  python3 scripts/hh_fetch.py --text "Product Manager" --areas 1,2 --per-page 100 --delay 0.5
  python3 scripts/hh_fetch.py --text "Data Scientist" --areas 1 --date-from 2025-09-01 --date-to 2025-09-24
  # Save Parquet and enrich with details
  python3 scripts/hh_fetch.py --text "Analyst" --areas 1 --parquet --details
  # Last 14 days, daily windows
  python3 scripts/hh_fetch.py --areas 1,2 --last-days 14 --window-days 1

Defaults:
  - Saves CSV into ./output/hh_vacancies_<timestamp>.csv
  - Optional Parquet output with --parquet (requires pandas+pyarrow)
  - Optional per-vacancy details enrichment with --details
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
    parser.add_argument("--text", required=False, default="", help="Search text (query). Optional")
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
    parser.add_argument("--last-days", dest="last_days", type=int, default=None, help="Fetch last N days (mutually exclusive with date-from/to)")
    parser.add_argument("--window-days", dest="window_days", type=int, default=1, help="Split period into N-day windows (default: 1)")
    parser.add_argument(
        "--employment",
        default=None,
        help="Employment filter(s), comma-separated. Examples: part,full,project,volunteer,probation",
    )
    parser.add_argument(
        "--schedule",
        default=None,
        help="Schedule filter(s), comma-separated. Examples: fullDay,shift,flexible,remote,flyInFlyOut",
    )
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests in seconds")
    parser.add_argument(
        "--out",
        default=None,
        help="Output CSV path. Default: ./output/hh_vacancies_<timestamp>.csv",
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        help="Also save results to Parquet (requires pandas & pyarrow)",
    )
    parser.add_argument(
        "--parquet-out",
        default=None,
        help="Parquet output path. Default mirrors CSV name with .parquet",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Fetch /vacancies/{id} for richer fields (slower)",
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


def get_detail_with_backoff(vacancy_id: str, headers: Dict[str, str], max_retries: int = 5) -> Optional[Dict[str, Any]]:
    url = f"https://api.hh.ru/vacancies/{vacancy_id}"
    backoff = 1.0
    for _ in range(max_retries):
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return None
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
            continue
        # other errors: give up for this vacancy only
        return None
    return None


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
    headers = {
        "User-Agent": user_agent,
        "HH-User-Agent": user_agent,
        "Accept": "application/json",
    }
    for area in area_ids:
        page = 0
        total_pages_for_area: Optional[int] = None
        while True:
            if max_pages is not None and page > max_pages:
                break
            params: Dict[str, Any] = {
                "area": area,
                "per_page": per_page,
                "page": page,
            }
            if query_text.strip():
                params["text"] = query_text.strip()
            if date_from:
                params["date_from"] = date_from
            if date_to:
                params["date_to"] = date_to
            # filters
            if args_employment:
                # repeated keys supported by requests when value is list
                params["employment"] = args_employment
            if args_schedule:
                params["schedule"] = args_schedule

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


def flatten_item(item: Dict[str, Any], detail: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    salary = item.get("salary") or {}
    employer = item.get("employer") or {}
    area = item.get("area") or {}
    snippet = item.get("snippet") or {}

    detail_desc_html: Optional[str] = None
    detail_key_skills: Optional[str] = None
    detail_prof_roles: Optional[str] = None
    if detail:
        detail_desc_html = detail.get("description")
        ks = detail.get("key_skills") or []
        if isinstance(ks, list):
            detail_key_skills = ", ".join([str(x.get("name")) for x in ks if isinstance(x, dict)])
        pr = detail.get("professional_roles") or []
        if isinstance(pr, list):
            detail_prof_roles = ", ".join([str(x.get("name")) for x in pr if isinstance(x, dict)])

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
        # details
        "detail_description_html": detail_desc_html,
        "detail_key_skills": detail_key_skills,
        "detail_professional_roles": detail_prof_roles,
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
        "detail_description_html",
        "detail_key_skills",
        "detail_professional_roles",
    ]

    total = 0
    rows: List[Dict[str, Any]] = []
    headers = {
        "User-Agent": args.user_agent,
        "HH-User-Agent": args.user_agent,
        "Accept": "application/json",
    }
    # prepare filters lists
    args_employment: Optional[List[str]] = None
    if args.employment:
        args_employment = [x.strip() for x in args.employment.split(",") if x.strip()]
    args_schedule: Optional[List[str]] = None
    if args.schedule:
        args_schedule = [x.strip() for x in args.schedule.split(",") if x.strip()]
    # Determine date windows
    if args.last_days is not None:
        if args.date_from or args.date_to:
            sys.stderr.write("Provide either --last-days or --date-from/--date-to, not both.\n")
            sys.exit(2)
        end_date = dt.date.today()
        start_date = end_date - dt.timedelta(days=max(args.last_days, 0))
    else:
        start_date = dt.date.fromisoformat(args.date_from) if args.date_from else None
        end_date = dt.date.fromisoformat(args.date_to) if args.date_to else None

    def date_str(d: Optional[dt.date]) -> Optional[str]:
        return d.isoformat() if d else None

    # Build list of (from,to) windows
    windows: List[tuple[Optional[str], Optional[str]]] = []
    if start_date and end_date:
        wd = max(1, int(args.window_days))
        cur = start_date
        while cur <= end_date:
            w_end = min(cur + dt.timedelta(days=wd - 1), end_date)
            windows.append((date_str(cur), date_str(w_end)))
            cur = w_end + dt.timedelta(days=1)
    else:
        windows.append((date_str(start_date), date_str(end_date)))

    for (w_from, w_to) in windows:
        for item in iter_vacancies(
            query_text=args.text,
            area_ids=area_ids,
            per_page=args.per_page,
            user_agent=args.user_agent,
            date_from=w_from,
            date_to=w_to,
            delay=args.delay,
            max_pages=args.max_pages,
        ):
            detail_obj: Optional[Dict[str, Any]] = None
            if args.details and item.get("id"):
                detail_obj = get_detail_with_backoff(str(item.get("id")), headers=headers)
                time.sleep(max(args.delay, 0.25))
            rows.append(flatten_item(item, detail=detail_obj))
            total += 1

    # CSV output
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {total} rows to {out_path}")

    # Optional Parquet output
    if args.parquet:
        parquet_path = args.parquet_out
        if not parquet_path:
            parquet_path = os.path.splitext(out_path)[0] + ".parquet"
        try:
            import pandas as pd  # type: ignore
        except Exception:
            sys.stderr.write(
                "pandas is required for --parquet. Install with: pip install pandas pyarrow\n"
            )
            return
        df = pd.DataFrame(rows)
        try:
            df.to_parquet(parquet_path, index=False)
        except Exception as exc:
            sys.stderr.write(
                f"Failed to write Parquet at {parquet_path}: {exc}\n"
            )
            return
        print(f"Saved Parquet to {parquet_path}")


if __name__ == "__main__":
    main()


