#!/usr/bin/env python3
"""
beIN TV guide scraper

Fetches beIN's public EPG-like JSON from:
  https://www.beinsports.com/api/opta/tv-event

Writes a raw snapshot JSON suitable for later ingestion.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import requests

LOG = logging.getLogger("bein_scrape")

DEFAULT_URL = "https://www.beinsports.com/api/opta/tv-event"


@dataclass(frozen=True)
class ScrapeResult:
    url: str
    fetched_utc: str
    count: Optional[int]
    rows: list


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def fetch_json(url: str, timeout_s: int, accept_language: str) -> Dict[str, Any]:
    headers = {
        "Accept": "application/json",
        "Accept-Language": accept_language,
        # Keep UA generic; avoid anything that looks like a bot but don't overdo it.
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    # Standing preference: never use proxies (and don’t inherit env proxies).
    session = requests.Session()
    session.trust_env = False

    LOG.info("GET %s", url)
    r = session.get(url, headers=headers, timeout=timeout_s)
    LOG.info("HTTP %s %s", r.status_code, r.reason)
    r.raise_for_status()

    # requests will decode based on headers; fallback handled by json()
    return r.json()


def write_json(path: Path, payload: Dict[str, Any], pretty: bool) -> None:
    ensure_parent_dir(path)
    tmp = path.with_suffix(path.suffix + ".tmp")

    with tmp.open("w", encoding="utf-8") as f:
        if pretty:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        else:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    tmp.replace(path)


def build_snapshot(raw: Dict[str, Any], url: str) -> ScrapeResult:
    rows = raw.get("rows") or []
    count = raw.get("count")
    return ScrapeResult(
        url=url,
        fetched_utc=utc_now_iso(),
        count=count if isinstance(count, int) else None,
        rows=rows if isinstance(rows, list) else [],
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape beIN tv-event feed to a JSON snapshot")
    ap.add_argument("--url", default=DEFAULT_URL, help="Endpoint URL (default: tv-event)")
    ap.add_argument("--out", required=True, help="Output JSON path")
    ap.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    ap.add_argument("--accept-language", default="en-US,en;q=0.9", help="Accept-Language header")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON (bigger files)")
    ap.add_argument("--rotate", action="store_true",
                    help="Also write a timestamped copy next to --out (audit trail)")
    args = ap.parse_args()

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    out_path = Path(args.out)

    raw = fetch_json(args.url, timeout_s=args.timeout, accept_language=args.accept_language)
    snap = build_snapshot(raw, args.url)

    payload = {
        "source": "bein",
        "endpoint": snap.url,
        "fetched_utc": snap.fetched_utc,
        "count": snap.count if snap.count is not None else len(snap.rows),
        "rows": snap.rows,
    }

    write_json(out_path, payload, pretty=args.pretty)
    LOG.info("Wrote snapshot: %s (rows=%d)", out_path, len(snap.rows))

    if args.rotate:
        ts = snap.fetched_utc.replace(":", "").replace("-", "").replace("Z", "Z")
        rotated = out_path.with_name(out_path.stem + f"_{ts}" + out_path.suffix)
        write_json(rotated, payload, pretty=args.pretty)
        LOG.info("Wrote rotated snapshot: %s", rotated)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

