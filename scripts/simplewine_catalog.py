#!/usr/bin/env python3
"""Parse the public SimpleWine wine catalog HTML (Moscow) and dump Vivino 4.2+ cards.

This is a personal scout prototype. It reads ordinary public catalog HTML only:
  https://simplewine.ru/catalog/vino/

It does not invent shop APIs, log in, solve captchas, or collect reviewer PII.

Live simplewine.ru sits behind ServicePipe (header x-sp-crid). From a
datacenter IP the response is either HTTP 403 Forbidden or a JS challenge
page (~1 KB, js-challenge-loader, servicepipe.tech). This script records
that and, unless --no-wayback, falls back to a Wayback Machine *memento of
the same public catalog URL* (not a hidden endpoint).

Usage:
  python3 scripts/simplewine_catalog.py
  python3 scripts/simplewine_catalog.py --html /path/to/saved.html
  python3 scripts/simplewine_catalog.py --source wayback --max-pages 1
  python3 scripts/simplewine_catalog.py --vivino-min 4.2 --out output/sample.json
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

try:
    import requests
except Exception:
    requests = None  # type: ignore

LIVE_CATALOG = "https://simplewine.ru/catalog/vino/"
CONTACT_UA = (
    "Mozilla/5.0 (compatible; WineScoutPersonal/0.1; +mailto:i@pnevostruev.ru)"
)
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
)
WAYBACK_CDX = "https://web.archive.org/cdx/search/cdx"
# Public catalog HTML memento verified 2026-10-20: Moscow, 32 cards, cityName=Москва.
WAYBACK_MOSCOW_PAGE1 = (
    "https://web.archive.org/web/20250206224230id_/"
    "https://www.simplewine.ru/catalog/vino/"
)
DEFAULT_DELAY = 2.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Parse public SimpleWine catalog HTML and dump Vivino 4.2+ cards"
    )
    p.add_argument(
        "--source",
        choices=("live", "wayback", "auto"),
        default="auto",
        help="live only, Wayback memento only, or live then Wayback if blocked (default)",
    )
    p.add_argument("--html", help="Parse a saved catalog HTML file instead of fetching")
    p.add_argument(
        "--vivino-min",
        type=float,
        default=4.2,
        help="Minimum VIVINO score printed on the card (default 4.2)",
    )
    p.add_argument("--max-pages", type=int, default=1, help="Catalog pages to fetch (default 1)")
    p.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Seconds between page fetches")
    p.add_argument("--out", help="JSON output path (default output/simplewine_4_2_<source>.json)")
    p.add_argument("--csv", help="Optional CSV output path")
    p.add_argument("--no-wayback", action="store_true", help="Do not fall back to Wayback")
    p.add_argument("--user-agent", default=BROWSER_UA, help="HTTP User-Agent")
    return p.parse_args()


def _norm_space(text: str) -> str:
    return re.sub(r"[\s\xa0]+", " ", text or "").strip()


def _clean_wayback_url(href: str) -> str:
    if not href:
        return ""
    href = re.sub(r"^https://web\.archive\.org/web/\d+[a-z_]*/", "", href)
    href = re.sub(r"^/web/\d+[a-z_]*/https://(www\.)?simplewine\.ru", "", href)
    return href


def _abs_product_url(href: str) -> str:
    href = _clean_wayback_url(href)
    if href.startswith("http"):
        return href.replace("https://www.simplewine.ru", "https://simplewine.ru")
    if href.startswith("/"):
        return urljoin("https://simplewine.ru/", href)
    return href


def _to_float(text: str) -> Optional[float]:
    m = re.search(r"(\d+(?:[.,]\d+)?)", text or "")
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return None


def _price_rub(text: str) -> Optional[int]:
    digits = re.sub(r"[^\d]", "", text or "")
    return int(digits) if digits else None


class CatalogCardParser(HTMLParser):
    """Pull name / producer / vintage / volume / price / availability / printed ratings."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.cards: List[Dict[str, Any]] = []
        self.city: Optional[str] = None
        self.catalog_total: Optional[int] = None
        self._card: Optional[Dict[str, Any]] = None
        self._capture: Optional[str] = None
        self._buf: List[str] = []
        self._in_brand = False
        self._in_total_count = False
        self._script_buf: List[str] = []
        self._in_script = False

    def handle_starttag(self, tag: str, attrs: List[tuple]) -> None:
        d = dict(attrs)
        cls = d.get("class", "")
        target = d.get("data-autotest-target", "")
        itemprop = d.get("itemprop", "")

        if tag == "script":
            self._in_script = True
            self._script_buf = []
            return

        if (
            tag == "span"
            and "js-page-meta-amout" in cls
            and d.get("data-autotest-target-id") == "catalog-info-count-value"
        ):
            self._in_total_count = True
            self._buf = []
            return

        if tag == "article" and "snippet" in cls and target == "product-item":
            self._card = {
                "sku": None,
                "name": None,
                "producer": None,
                "vintage": None,
                "volume": None,
                "price": None,
                "price_rub": None,
                "availability": None,
                "availability_text": None,
                "url": None,
                "vivino": None,
                "rp": None,
                "js": None,
                "shop_rating": None,
                "shop_reviews": None,
                "in_stock": None,
                "country": None,
            }
            return

        if not self._card:
            return

        if tag == "meta" and itemprop == "sku":
            self._card["sku"] = d.get("content")
        if tag == "span" and itemprop == "brand":
            self._in_brand = True
        if self._in_brand and tag == "meta" and itemprop == "name" and not self._card["producer"]:
            self._card["producer"] = d.get("content")
        if tag == "a" and target == "product-item-name":
            href = d.get("href", "")
            if href:
                self._card["url"] = _abs_product_url(href)
            self._start_capture("name")
        if tag == "div" and target == "product-item-final-price":
            content = d.get("content")
            if content:
                self._card["price"] = _norm_space(content)
                self._card["price_rub"] = _price_rub(content)
            self._start_capture("price")
        if tag == "div" and "snippet-rating" in cls.split():
            self._start_capture("crit_rating")
        if tag == "span" and "snippet-star__value" in cls and self._card["shop_rating"] is None:
            self._start_capture("shop_rating")
        if tag == "span" and "snippet-star__reviews" in cls and self._card["shop_reviews"] is None:
            self._start_capture("shop_reviews")
        if tag == "link" and itemprop == "availability":
            href = d.get("href", "")
            self._card["availability"] = href.rsplit("/", 1)[-1] if href else None
        if tag == "div" and "snippet-status" in cls:
            self._start_capture("status")
        if tag == "div" and d.get("id") == "snippet-buy-block":
            year = (d.get("data-product-year") or "").strip()
            self._card["vintage"] = year or None
            cap = _norm_space(d.get("data-product-capacity") or "")
            self._card["volume"] = cap or None
            self._card["in_stock"] = d.get("data-product-instock")
            self._card["country"] = d.get("data-product-country")
            href = d.get("data-product-href")
            if href and not self._card["url"]:
                self._card["url"] = _abs_product_url(href)

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._in_script:
            self._in_script = False
            blob = "".join(self._script_buf)
            if self.city is None:
                m = re.search(r'"cityName"\s*:\s*"([^"]+)"', blob)
                if m:
                    self.city = m.group(1)
            self._script_buf = []
            return

        if self._in_total_count and tag == "span":
            text = _norm_space("".join(self._buf))
            n = _price_rub(text)
            if n:
                self.catalog_total = n
            self._in_total_count = False
            self._buf = []

        if not self._card:
            return

        if self._capture and tag in ("a", "div", "span"):
            text = _norm_space("".join(self._buf))
            if self._capture == "name":
                self._card["name"] = text or self._card["name"]
            elif self._capture == "price" and text:
                self._card["price"] = self._card["price"] or text
                self._card["price_rub"] = self._card["price_rub"] or _price_rub(text)
            elif self._capture == "crit_rating" and text:
                m = re.search(r"(VIVINO|RP|JS)\s*([0-9.]+)", text, re.I)
                if m:
                    kind, val = m.group(1).upper(), float(m.group(2))
                    if kind == "VIVINO":
                        self._card["vivino"] = val
                    elif kind == "RP":
                        self._card["rp"] = val
                    elif kind == "JS":
                        self._card["js"] = val
            elif self._capture == "shop_rating" and text:
                self._card["shop_rating"] = _to_float(text)
            elif self._capture == "shop_reviews" and text:
                n = _price_rub(text)
                if n:
                    self._card["shop_reviews"] = n
            elif self._capture == "status" and text:
                self._card["availability_text"] = text
            self._capture = None
            self._buf = []

        if tag == "span" and self._in_brand:
            self._in_brand = False
        if tag == "article" and self._card:
            if not self._card.get("vintage"):
                self._card["vintage"] = "NV"
            self.cards.append(self._card)
            self._card = None

    def handle_data(self, data: str) -> None:
        if self._in_script:
            self._script_buf.append(data)
            return
        if self._capture or self._in_total_count:
            self._buf.append(data)

    def _start_capture(self, name: str) -> None:
        self._capture = name
        self._buf = []


def classify_body(status: int, body: str, headers: Dict[str, str]) -> str:
    text = body or ""
    head = text[:4000].lower()
    sp = headers.get("x-sp-crid") or headers.get("X-Sp-Crid")
    if status == 403 or "<h1>forbidden</h1>" in head:
        return "forbidden"
    if "js-challenge-loader" in head or "servicepipe.tech" in head or "id_captcha_frame_div" in head:
        return "js_challenge"
    if 'data-autotest-target="product-item"' in text and "snippet-name" in text:
        return "catalog_html"
    if sp and len(text) < 4000:
        return "js_challenge"
    return "unknown"


def _need_requests() -> None:
    if requests is None:
        sys.stderr.write("requests is required for fetching. Install with: pip install requests\n")
        raise SystemExit(1)


def fetch_url(session: Any, url: str, ua: str, timeout: int = 30) -> Any:
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
    }
    return session.get(url, headers=headers, timeout=timeout, allow_redirects=True)


def live_page_url(page: int) -> str:
    if page <= 1:
        return LIVE_CATALOG
    return f"https://simplewine.ru/catalog/vino/page{page}/"


def wayback_memento_url(page: int) -> Optional[str]:
    """Public HTML memento of the same catalog path.

    CDX without a from-date returns 2013–2015 markup first. We look for a
    2024+ capture of the live URL; page 1 falls back to the Moscow snapshot
    verified on 2026-10-20.
    """
    if page == 1:
        return WAYBACK_MOSCOW_PAGE1
    original = live_page_url(page)
    _need_requests()
    try:
        r = requests.get(
            WAYBACK_CDX,
            params={
                "url": original.replace("https://simplewine.ru", "https://www.simplewine.ru"),
                "output": "json",
                "fl": "timestamp,original,statuscode,mimetype,length",
                "filter": "statuscode:200",
                "from": "20240101",
                "limit": 5,
            },
            timeout=30,
            headers={"User-Agent": CONTACT_UA, "Accept": "application/json"},
        )
        rows = r.json() if r.ok else []
    except Exception:
        rows = []
    records = rows[1:] if rows and isinstance(rows[0], list) else rows
    usable = []
    for rec in records:
        if not isinstance(rec, list) or len(rec) < 5:
            continue
        ts, orig, status, _mime, length = rec[0], rec[1], rec[2], rec[3], rec[4]
        try:
            nbytes = int(length)
        except (TypeError, ValueError):
            nbytes = 0
        if status == "200" and nbytes >= 20000:
            usable.append((ts, orig, nbytes))
    if not usable:
        return None
    usable.sort(key=lambda x: x[0], reverse=True)
    ts, orig, _ = usable[0]
    return f"https://web.archive.org/web/{ts}id_/{orig}"


def parse_html(html: str) -> CatalogCardParser:
    parser = CatalogCardParser()
    parser.feed(html)
    parser.close()
    return parser


def filter_vivino(cards: List[Dict[str, Any]], minimum: float) -> List[Dict[str, Any]]:
    out = []
    for card in cards:
        v = card.get("vivino")
        if v is not None and v >= minimum:
            out.append(card)
    return out


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "sku",
        "name",
        "producer",
        "vintage",
        "volume",
        "price",
        "price_rub",
        "availability",
        "availability_text",
        "vivino",
        "rp",
        "js",
        "shop_rating",
        "shop_reviews",
        "country",
        "url",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def fetch_live(session: Any, page: int, ua: str) -> Dict[str, Any]:
    url = live_page_url(page)
    try:
        resp = fetch_url(session, url, ua)
    except Exception as exc:
        return {
            "url": url,
            "ok": False,
            "kind": "network_error",
            "error": str(exc),
            "status": None,
            "bytes": 0,
            "html": "",
            "headers": {},
        }
    html = resp.text or ""
    headers = {k.lower(): v for k, v in resp.headers.items()}
    kind = classify_body(resp.status_code, html, headers)
    return {
        "url": url,
        "ok": kind == "catalog_html",
        "kind": kind,
        "status": resp.status_code,
        "bytes": len(html),
        "html": html,
        "headers": {
            "content-type": headers.get("content-type"),
            "x-sp-crid": headers.get("x-sp-crid"),
            "server": headers.get("server"),
        },
        "excerpt": re.sub(r"\s+", " ", html[:400]),
    }


def fetch_wayback(session: Any, page: int, ua: str) -> Dict[str, Any]:
    memento = wayback_memento_url(page)
    if not memento:
        return {
            "url": None,
            "ok": False,
            "kind": "no_memento",
            "status": None,
            "bytes": 0,
            "html": "",
            "headers": {},
        }
    try:
        resp = fetch_url(session, memento, ua, timeout=60)
    except Exception as exc:
        return {
            "url": memento,
            "ok": False,
            "kind": "network_error",
            "error": str(exc),
            "status": None,
            "bytes": 0,
            "html": "",
            "headers": {},
        }
    html = resp.text or ""
    headers = {k.lower(): v for k, v in resp.headers.items()}
    kind = classify_body(resp.status_code, html, headers)
    return {
        "url": memento,
        "final_url": getattr(resp, "url", memento),
        "ok": kind == "catalog_html",
        "kind": kind,
        "status": resp.status_code,
        "bytes": len(html),
        "html": html,
        "headers": {
            "content-type": headers.get("content-type"),
            "memento-datetime": headers.get("memento-datetime"),
            "x-archive-orig-server": headers.get("x-archive-orig-server"),
        },
        "excerpt": re.sub(r"\s+", " ", html[:400]),
    }


def main() -> int:
    args = parse_args()
    attempts: List[Dict[str, Any]] = []
    html_pages: List[str] = []
    used_source = None
    used_urls: List[str] = []

    if args.html:
        path = Path(args.html)
        html = path.read_text(encoding="utf-8", errors="replace")
        html_pages.append(html)
        used_source = f"local_html:{path}"
        used_urls.append(str(path))
        attempts.append(
            {
                "url": str(path),
                "ok": True,
                "kind": classify_body(200, html, {}),
                "status": None,
                "bytes": len(html),
            }
        )
    else:
        _need_requests()
        session = requests.Session()
        max_pages = max(1, args.max_pages)
        live_blocked = False
        if args.source in ("live", "auto"):
            for page in range(1, max_pages + 1):
                if page > 1:
                    time.sleep(args.delay)
                result = fetch_live(session, page, args.user_agent)
                slim = {k: v for k, v in result.items() if k not in ("html",)}
                attempts.append(slim)
                if result["ok"]:
                    html_pages.append(result["html"])
                    used_urls.append(result["url"])
                    used_source = used_source or "live"
                else:
                    live_blocked = True
                    if page == 1:
                        break
        if not html_pages and args.source == "wayback":
            live_blocked = True
        if not html_pages and live_blocked and not args.no_wayback and args.source != "live":
            sys.stderr.write(
                "Live simplewine.ru did not return catalog HTML; "
                "trying a Wayback memento of the same public URL.\n"
            )
            for page in range(1, max_pages + 1):
                if page > 1:
                    time.sleep(args.delay)
                result = fetch_wayback(session, page, CONTACT_UA)
                slim = {k: v for k, v in result.items() if k not in ("html",)}
                attempts.append(slim)
                if result["ok"]:
                    html_pages.append(result["html"])
                    used_urls.append(result.get("final_url") or result["url"])
                    used_source = used_source or "wayback"
                elif page == 1:
                    break

    if not html_pages:
        payload = {
            "ok": False,
            "reason": "no_catalog_html",
            "attempts": attempts,
            "cards": 0,
            "matches": [],
        }
        out = Path(args.out) if args.out else Path("output/simplewine_4_2_blocked.json")
        write_json(out, payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 2

    all_cards: List[Dict[str, Any]] = []
    city = None
    catalog_total = None
    for html in html_pages:
        parsed = parse_html(html)
        city = city or parsed.city
        catalog_total = catalog_total or parsed.catalog_total
        all_cards.extend(parsed.cards)

    matches = filter_vivino(all_cards, args.vivino_min)
    payload = {
        "ok": True,
        "source": used_source,
        "urls": used_urls,
        "city": city,
        "catalog_total_on_page": catalog_total,
        "cards": len(all_cards),
        "vivino_min": args.vivino_min,
        "match_count": len(matches),
        "matches": matches,
        "attempts": attempts,
        "note": (
            "Ratings are those already printed on the shop card (VIVINO / RP / JS). "
            "Reviewer names are not collected. Live catalog from this host is typically "
            "blocked by ServicePipe; Wayback HTML is a public snapshot, not today's shelf."
        ),
    }
    out = Path(args.out) if args.out else Path(f"output/simplewine_4_2_{used_source.split(':')[0]}.json")
    write_json(out, payload)
    if args.csv:
        write_csv(Path(args.csv), matches)
    print(
        f"source={used_source} city={city} cards={len(all_cards)} "
        f"vivino>={args.vivino_min}: {len(matches)} -> {out}"
    )
    for card in matches:
        print(
            f"  {card.get('vivino')}  {card.get('name')}  {card.get('producer')}  "
            f"{card.get('vintage')}  {card.get('volume')}  {card.get('price')}  "
            f"{card.get('availability_text') or card.get('availability')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
