# Glossary of Metrics

This repo contains the Product Heroes glossary and related assets.

- Main glossary: `Глоссарий Product Heroes/Глоссарий основных метрик и терминов 148669696d3544b182cb59e5b16892d7.md`
- Key concepts: AMPU (C1 × AMPPU), C1, CAC, AMPPU, Gross/Profit

## HH vacancies fetcher

Script: `scripts/hh_fetch.py`

- Output: CSV saved to `./output/hh_vacancies_<timestamp>.csv` by default
- Example:
  - `python3 scripts/hh_fetch.py --text "Product Manager" --areas 1,2 --per-page 100 --delay 0.5`
  - `python3 scripts/hh_fetch.py --text "Data Scientist" --areas 1 --date-from 2025-09-01 --date-to 2025-09-24`
- Common flags:
  - `--text` search query (required)
  - `--areas` comma-separated area IDs (default `1`)
  - `--date-from`, `--date-to` in `YYYY-MM-DD`
  - `--per-page` (max 100), `--max-pages` cap
  - `--delay` seconds between requests (default 0.5)
  - `--out` custom CSV path
  - `--user-agent` custom UA string

Notes:
- Please use the official API respectfully; handle rate limits (the script has backoff).
- Salary fields may be missing; normalize as needed downstream.

