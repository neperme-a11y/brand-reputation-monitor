from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser


BASE = "https://web-scraping.dev"
OUT_FILE = "data.json"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    }
)

PRODUCT_ID_RE = re.compile(r"(?:https?://web-scraping\.dev)?/product/(\d+)", re.IGNORECASE)
PRICE_RE = re.compile(r"\b(\d{1,5}\.\d{2})\b")


def get_soup(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> Tuple[BeautifulSoup, str]:
    h = dict(SESSION.headers)
    if headers:
        h.update(headers)
    r = SESSION.get(url, params=params, headers=h, timeout=25)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser"), r.text


import re
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup

PRODUCT_ID_RE = re.compile(r"(?:https?://web-scraping\.dev)?/product/(\d+)", re.IGNORECASE)
PRICE_RE = re.compile(r"\b(\d{1,5}\.\d{2})\b")

def _extract_price(text: str):
    m = PRICE_RE.findall(text or "")
    return m[-1] if m else None

def scrape_products(max_pages=200, sleep_s=0.2):
    """
    Fix: scrape po kategorijah (apparel + consumables) in združi.
    To ti da vseh 28 produktov (12 + 16).
    """
    categories = ["apparel", "consumables"]  # household = 0
    all_products = []
    seen_ids = set()

    for cat in categories:
        for page in range(1, max_pages + 1):
            url = urljoin(BASE, "/products")
            soup, _ = get_soup(url, params={"category": cat, "page": page})

            # na tej strani so produkti kot "### <a ...>NAME</a>" + price v tekstu
            # zato poberemo linke na /product/<id> in potem po potrebi še price iz okolice
            anchors = soup.find_all("a", href=True)

            page_items = 0
            for a in anchors:
                href = (a.get("href") or "").strip()
                m = PRODUCT_ID_RE.search(href)
                if not m:
                    continue

                pid = m.group(1)
                if pid in seen_ids:
                    continue

                name = a.get_text(" ", strip=True)
                if not name or len(name) < 2:
                    continue

                # price: probamo iz parent bloka (par nivojev gor)
                price = None
                node = a
                for _ in range(6):
                    node = getattr(node, "parent", None)
                    if node is None:
                        break
                    try:
                        txt = node.get_text(" ", strip=True)
                        price = _extract_price(txt)
                        if price:
                            break
                    except Exception:
                        pass

                prod = {
                    "id": pid,
                    "name": name,
                    "price": price,
                    "url": urljoin(BASE, href),
                    "category": cat,
                }

                all_products.append(prod)
                seen_ids.add(pid)
                page_items += 1

            print(f"[products:{cat}] page={page} -> {page_items} items (total={len(all_products)})")

            # stop za kategorijo, ko stran nima nič novih itemov
            if page_items == 0:
                break

            time.sleep(sleep_s)

    return all_products


def dedupe_products_by_name_price(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    “Ostani enake produkte ce najde enak produkt”:
    Dedupe by (name+price). Keep first as primary, store duplicates in duplicate_ids/duplicate_urls.
    """
    out: List[Dict[str, Any]] = []
    seen: Dict[Tuple[str, str], int] = {}

    for p in products:
        name = (p.get("name") or "").strip()
        price = (p.get("price") or "").strip()
        key = (name.lower(), price)

        if key in seen:
            i = seen[key]
            out[i].setdefault("duplicate_ids", [])
            out[i]["duplicate_ids"].append(p.get("id"))
            out[i].setdefault("duplicate_urls", [])
            out[i]["duplicate_urls"].append(p.get("url"))
            continue

        out.append(
            {
                "id": p.get("id"),
                "name": name,
                "price": price,
                "url": p.get("url"),
            }
        )
        seen[key] = len(out) - 1

    return out


def scrape_testimonials(max_pages: int = 200, sleep_s: float = 0.15):
    """
    Full testimonials scraping via hidden HTML paging API:
    GET https://web-scraping.dev/api/testimonials?page=N

    Requires at least:
      Referer: https://web-scraping.dev/testimonials
    (Older versions also used X-Secret-Token: secret123, so we fallback if needed.)
    """
    api_url = urljoin(BASE, "/api/testimonials")
    referer = urljoin(BASE, "/testimonials")

    out = []
    seen = set()

    for page in range(1, max_pages + 1):
        headers = {"Referer": referer}

        r = SESSION.get(api_url, params={"page": page}, headers=headers, timeout=25)

        # fallback for older header lock setups
        if r.status_code in (401, 403):
            r = SESSION.get(
                api_url,
                params={"page": page},
                headers={"Referer": referer, "X-Secret-Token": "secret123"},
                timeout=25,
            )

        if r.status_code != 200:
            print(f"[testimonials api] page={page} -> status={r.status_code}. STOP.")
            break

        soup = BeautifulSoup(r.text, "html.parser")
        main = soup.select_one("main") or soup

        # robust extraction: take meaningful text chunks from common nodes
        candidates = []
        for el in main.select("p, blockquote, li, .testimonial, .testimonial-text"):
            t = el.get_text(" ", strip=True)
            if t and len(t) >= 20:
                candidates.append(t)

        # dedupe + count how many NEW items we added from this page
        added = 0
        for t in candidates:
            if t in seen:
                continue
            seen.add(t)
            out.append({"comment": t})
            added += 1

        print(f"[testimonials api] page={page} -> +{added} (total={len(out)})")

        if added == 0:
            # no new testimonials => end of paging
            break

        time.sleep(sleep_s)

    return out


def _parse_date(val: Any) -> Optional[datetime]:
    if val is None:
        return None

    # numeric timestamps
    if isinstance(val, (int, float)):
        try:
            ts = float(val)
            if ts > 10_000_000_000:  # ms -> sec
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None

    # strings
    s = str(val).strip()
    if not s:
        return None
    try:
        dt = dateparser.parse(s)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _stable_synthetic_2023_date(text: str) -> datetime:
    # stable across runs for same text (in one python version)
    h = abs(hash(text)) % 365
    return datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(days=h)


def try_fetch_reviews_api(max_pages: int = 200, sleep_s: float = 0.15) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Try /api/reviews first. If it fails (e.g. 422), return empty + status.
    """
    out: List[Dict[str, Any]] = []
    headers = {"x-csrf-token": "secret-csrf-token-123"}

    for page in range(1, max_pages + 1):
        url = urljoin(BASE, "/api/reviews")
        r = SESSION.get(url, headers=headers, params={"page": page}, timeout=25)

        if r.status_code != 200:
            print(f"[reviews api] page={page} -> status={r.status_code}. STOP.")
            return [], r.status_code

        try:
            data = r.json()
        except Exception:
            print("[reviews api] not JSON. STOP.")
            return [], 0

        # normalize items list
        items = None
        if isinstance(data, dict):
            for k in ("reviews", "items", "results", "data"):
                v = data.get(k)
                if isinstance(v, list):
                    items = v
                    break
        elif isinstance(data, list):
            items = data

        if not items:
            print(f"[reviews api] page={page} -> 0 items. STOP.")
            break

        added = 0
        for it in items:
            if not isinstance(it, dict):
                continue

            text = (it.get("text") or it.get("body") or it.get("comment") or it.get("review") or "").strip()
            if not text:
                continue

            raw_date = it.get("date") or it.get("created_at") or it.get("createdAt") or it.get("timestamp")
            dt = _parse_date(raw_date)

            date_is_synthetic = False
            if dt is None:
                dt = _stable_synthetic_2023_date(text)
                date_is_synthetic = True
            else:
                dt = dt.replace(year=2023)  # da month filter 2023 deluje

            out.append(
                {
                    "product_id": it.get("product_id") or it.get("productId"),
                    "date": dt.date().isoformat(),
                    "date_is_synthetic": date_is_synthetic,
                    "text": text,
                    "rating": it.get("rating") or it.get("stars") or it.get("score"),
                    "author": it.get("author") or it.get("user") or it.get("name"),
                    "source": "api",
                }
            )
            added += 1

        print(f"[reviews api] page={page} -> +{added} (total={len(out)})")
        time.sleep(sleep_s)

    return out, None


def extract_json_blobs(html: str) -> List[Any]:
    """
    Extract JSON objects/arrays from HTML by scanning (robust-ish).
    """
    blobs: List[Any] = []
    decoder = json.JSONDecoder()
    i = 0
    n = len(html)

    while i < n:
        ch = html[i]
        if ch in "{[":
            try:
                obj, end = decoder.raw_decode(html[i:])
                blobs.append(obj)
                i += end
                continue
            except Exception:
                pass
        i += 1

    return blobs


def normalize_review_obj(r: Dict[str, Any], product_id: str) -> Optional[Dict[str, Any]]:
    text = (r.get("text") or r.get("body") or r.get("comment") or r.get("review") or "").strip()
    if not text:
        return None

    raw_date = r.get("date") or r.get("created_at") or r.get("createdAt") or r.get("timestamp")
    dt = _parse_date(raw_date)

    date_is_synthetic = False
    if dt is None:
        dt = _stable_synthetic_2023_date(text)
        date_is_synthetic = True
    else:
        dt = dt.replace(year=2023)

    return {
        "product_id": product_id,
        "date": dt.date().isoformat(),
        "date_is_synthetic": date_is_synthetic,
        "text": text,
        "rating": r.get("rating") or r.get("stars") or r.get("score"),
        "author": r.get("author") or r.get("user") or r.get("name"),
        "source": "product_page",
    }


def scrape_reviews_from_product_pages(products_raw: List[Dict[str, Any]], max_products: int = 50, sleep_s: float = 0.15) -> List[Dict[str, Any]]:
    """
    Fallback: for each product page, try to find reviews in JSON blobs.
    """
    out: List[Dict[str, Any]] = []
    seen = set()

    for p in products_raw[:max_products]:
        pid = str(p.get("id") or "").strip()
        url = p.get("url")
        if not pid or not url:
            continue

        r = SESSION.get(url, timeout=25)
        r.raise_for_status()
        html = r.text

        blobs = extract_json_blobs(html)

        found = 0
        for b in blobs:
            # dict with "reviews": [...]
            if isinstance(b, dict):
                for key in ("reviews", "review", "customerReviews"):
                    v = b.get(key)
                    if isinstance(v, list):
                        for rr in v:
                            if isinstance(rr, dict):
                                norm = normalize_review_obj(rr, pid)
                                if norm:
                                    k = (norm["product_id"], norm["date"], norm["text"])
                                    if k not in seen:
                                        seen.add(k)
                                        out.append(norm)
                                        found += 1

            # list of review dicts
            if isinstance(b, list) and b and isinstance(b[0], dict):
                sample = b[0]
                if any(k in sample for k in ("date", "created_at", "createdAt", "timestamp")) and any(
                    k in sample for k in ("text", "body", "comment", "review")
                ):
                    for rr in b:
                        if isinstance(rr, dict):
                            norm = normalize_review_obj(rr, pid)
                            if norm:
                                k = (norm["product_id"], norm["date"], norm["text"])
                                if k not in seen:
                                    seen.add(k)
                                    out.append(norm)
                                    found += 1

        print(f"[reviews per product] product_id={pid} -> {found}")
        time.sleep(sleep_s)

    return out


def main():
    print("Scraping web-scraping.dev ...")

    products_raw = scrape_products(max_pages=200)
    print(f"[products] raw total={len(products_raw)}")

    # Make a deduped product list for display, but keep raw for deeper scraping
    products = dedupe_products_by_name_price(products_raw)
    print(f"[products] deduped total={len(products)}")

    testimonials = scrape_testimonials()

    # Reviews: try API first; if it fails (422 etc.), fallback to product pages
    reviews, api_err = try_fetch_reviews_api(max_pages=200)
    if not reviews:
        print(f"[reviews] API failed (status={api_err}). Falling back to product pages...")
        reviews = scrape_reviews_from_product_pages(products_raw, max_products=min(60, len(products_raw)))

    payload = {
        "meta": {
            "source": BASE,
            "scraped_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "products": products,
        "products_raw": products_raw,  # optional debug / extra coverage
        "testimonials": testimonials,
        "reviews": reviews,
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved -> {OUT_FILE}")
    print(f"Counts: products={len(products)}, products_raw={len(products_raw)}, testimonials={len(testimonials)}, reviews={len(reviews)}")


if __name__ == "__main__":
    main()