#!/usr/bin/env python3
"""
amazon2.py (Playwright-only, headless)

Purpose:
- Extract Amazon GTIs from fruit_events.db
- Visit each GTI page with Playwright (headless) to determine required channel/subscription
- Upsert results into amazon_channels table
- Emit a detailed debug CSV next to the DB file

Notes:
- No emojis in logs.
- Designed to be run inside the container.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as _dt
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

LOG = logging.getLogger("amazon2")

GTI_RE = re.compile(r"(amzn1\.dv\.gti\.[0-9a-fA-F-]{36})")
BENEFIT_RE_LIST = [
    re.compile(r"benefitId=([A-Za-z0-9_.\-]+)"),
    re.compile(r'"benefitId"\s*:\s*"([A-Za-z0-9_.\-]+)"'),
]

# BenefitId -> display name + canonical logical service id (matches your amazon_services style)
BENEFIT_MAP: Dict[str, Tuple[str, str]] = {
    "prime_included": ("Prime Exclusive", "aiv_prime"),
    "daznus": ("DAZN", "aiv_dazn"),
    "peacockus": ("Peacock", "aiv_peacock"),
    "maxliveeventsus": ("Max", "aiv_max"),
    "vixplusus": ("ViX Premium", "aiv_vix_premium"),
    "FSNOHIFSOH3": ("FanDuel Sports Network", "aiv_fanduel"),
}

# Simple inference from entitlement/page text -> (display name, logical_service)
TEXT_INFER: List[Tuple[re.Pattern, Tuple[str, str]]] = [
    (re.compile(r"\bNBA League Pass\b", re.I), ("NBA League Pass", "aiv_nba_league_pass")),
    (re.compile(r"\bWNBA League Pass\b", re.I), ("WNBA League Pass", "aiv_wnba_league_pass")),
    (re.compile(r"\bFOX One\b", re.I), ("FOX One", "aiv_fox_one")),
    (re.compile(r"\bPeacock\b", re.I), ("Peacock", "aiv_peacock")),
    (re.compile(r"\bMax\b", re.I), ("Max", "aiv_max")),
    (re.compile(r"\bDAZN\b", re.I), ("DAZN", "aiv_dazn")),
    (re.compile(r"\bFanDuel Sports Network\b", re.I), ("FanDuel Sports Network", "aiv_fanduel")),
    (re.compile(r"\bViX Premium\b", re.I), ("ViX Premium", "aiv_vix_premium")),
    (re.compile(r"\bViX\b", re.I), ("ViX", "aiv_vix")),
    (re.compile(r"\bParamount\+\b", re.I), ("Paramount+", "aiv_paramount_plus")),
    (re.compile(r"\bWillow\b", re.I), ("Willow TV", "aiv_willow")),
    (re.compile(r"\bSquashTV\b", re.I), ("SquashTV", "aiv_squash")),
    (re.compile(r"\bPrime\b", re.I), ("Prime Exclusive", "aiv_prime")),
]

@dataclass
class ScrapeResult:
    gti: str
    url: str
    status: str  # SUCCESS / TIMEOUT / ERROR / STALE
    channel_id: str
    channel_name: str
    benefit_id: str
    entitlement_text: str
    failure_reason: str
    elapsed_ms: int

def _utcnow_iso() -> str:
    return _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"



def count_processes(patterns: List[str]) -> int:
    """Return a best-effort count of running processes whose command line contains any pattern."""
    try:
        import subprocess
        out = subprocess.check_output(["/bin/sh", "-lc", "ps ax -o pid=,command="], text=True, stderr=subprocess.DEVNULL)
        cnt = 0
        for line in out.splitlines():
            cmd = line.strip()
            if not cmd:
                continue
            low = cmd.lower()
            if any(p.lower() in low for p in patterns):
                cnt += 1
        return cnt
    except Exception:
        return 0


def _preflight_cleanup() -> None:
    """Deprecated: kept for compatibility; no longer kills processes."""
    try:
        leftovers = count_processes(["playwright", "chromium", "chrome"])
        if leftovers:
            LOG.warning("Preflight: detected %d existing chromium/playwright processes (not killed).", leftovers)
    except Exception:
        LOG.debug("Preflight: skipped", exc_info=True)

def _ensure_tables(conn: sqlite3.Connection) -> None:
    """
    Ensure amazon_channels exists AND has expected columns.

    We do *not* assume a fresh DB: older installs may already have amazon_channels without
    newer columns. SQLite supports ADD COLUMN, so we migrate in-place.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS amazon_channels (
            gti TEXT PRIMARY KEY,
            channel_id TEXT,
            channel_name TEXT
        )
    """)

    # Migrate schema forward if needed
    cur = conn.execute("PRAGMA table_info(amazon_channels)")
    existing = {r[1] for r in cur.fetchall()}

    # Add columns if missing (safe on existing data)
    if "channel_id" not in existing:
        conn.execute("ALTER TABLE amazon_channels ADD COLUMN channel_id TEXT")
    if "channel_name" not in existing:
        conn.execute("ALTER TABLE amazon_channels ADD COLUMN channel_name TEXT")
    if "last_updated_utc" not in existing:
        conn.execute("ALTER TABLE amazon_channels ADD COLUMN last_updated_utc TEXT")
    if "is_stale" not in existing:
        conn.execute("ALTER TABLE amazon_channels ADD COLUMN is_stale INTEGER DEFAULT 0")

    conn.commit()

def _detect_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]

def _extract_gtis_from_row(values: Iterable[Optional[str]]) -> List[str]:
    out: List[str] = []
    for v in values:
        if not v:
            continue
        for m in GTI_RE.findall(v):
            out.append(m)
    return out


def purge_malformed_amazon_channels(conn: sqlite3.Connection) -> int:
    """
    Delete rows from amazon_channels whose GTI looks malformed/truncated.

    This protects against older runs that inserted partial GTIs (e.g. due to SUBSTR(...,38))
    which will always 404 (Amazon "dog" page) and can interfere with joins/migrations.
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='amazon_channels'")
        if not cur.fetchone():
            return 0

        cur.execute("SELECT gti FROM amazon_channels")
        rows = cur.fetchall()

        good_re = re.compile(r"^amzn1\.dv\.gti\.[0-9a-fA-F-]{36}$")
        bad = []
        for (gti,) in rows:
            if not gti or not isinstance(gti, str):
                bad.append(gti)
                continue
            g = gti.strip()
            if g != gti:
                # whitespace -> treat as bad (we'll reinsert clean)
                bad.append(gti)
                continue
            if not good_re.match(g):
                bad.append(gti)

        if not bad:
            return 0

        # Delete in reasonably sized batches
        deleted = 0
        for i in range(0, len(bad), 500):
            chunk = bad[i:i+500]
            qmarks = ",".join(["?"] * len(chunk))
            cur.execute(f"DELETE FROM amazon_channels WHERE gti IN ({qmarks})", chunk)
            deleted += cur.rowcount if cur.rowcount is not None else 0

        conn.commit()
        return deleted
    except Exception:
        # Never fail the scrape due to preflight cleanup
        return 0

def extract_gtis(
    db_path: str,
    limit: int,
    horizon_hours: int = 72,
    past_hours: int = 6,
    rescrape_hours: int = 48,
) -> List[str]:
    """
    Extract Amazon GTIs to scrape.

    Strategy:
      1) Prefer an event-horizon selection (JOIN playables->events) so we only scrape what matters soon.
      2) Apply a cache-skip using amazon_channels.last_updated_utc (skip recently-successful GTIs).
      3) Fall back to legacy behavior if required columns/tables are missing.

    Notes:
      - Horizon filter can be disabled by setting horizon_hours=0.
      - Cache-skip can be disabled by setting rescrape_hours=0.
      - 'limit' is an additional hard safety cap applied at the end.
    """
    conn = sqlite3.connect(db_path)
    purged = purge_malformed_amazon_channels(conn)
    if purged:
        LOG.info("Preflight: purged malformed amazon_channels rows count=%d", purged)

    try:
        play_cols = set(_detect_columns(conn, "playables"))
        has_event_id = "event_id" in play_cols
        has_events_table = False
        try:
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
            has_events_table = bool(cur.fetchone())
        except Exception:
            has_events_table = False

        candidates = [c for c in [
            "playable_url", "deeplink_play", "deeplink_open", "http_deeplink_url",
            "playable_id", "content_id", "title"
        ] if c in play_cols]
        if not candidates:
            raise RuntimeError("No usable columns found in playables table for GTI extraction")

        gtis: List[str] = []

        # Preferred: horizon-based selection
        if has_event_id and has_events_table and horizon_hours and horizon_hours > 0:
            ev_cols = set(_detect_columns(conn, "events"))
            if ("start_utc" in ev_cols) or ("end_utc" in ev_cols):
                future_mod = f"+{int(horizon_hours)} hours"
                past_mod = f"-{int(past_hours)} hours"
                sql = f"""
                    SELECT {', '.join('p.' + c for c in candidates)}
                    FROM playables p
                    JOIN events e ON e.id = p.event_id
                    WHERE p.provider='aiv'
                      AND (e.start_utc IS NULL OR e.start_utc <= datetime('now', ?))
                      AND (e.end_utc IS NULL OR e.end_utc >= datetime('now', ?))
                """
                cur = conn.execute(sql, (future_mod, past_mod))
                for row in cur.fetchall():
                    gtis.extend(_extract_gtis_from_row(row))

        # Fallback: legacy (all AIV playables)
        if not gtis:
            sql = f"SELECT {', '.join(candidates)} FROM playables WHERE provider='aiv'"
            cur = conn.execute(sql)
            for row in cur.fetchall():
                gtis.extend(_extract_gtis_from_row(row))

        # De-dupe while preserving order
        seen = set()
        uniq: List[str] = []
        for g in gtis:
            if g not in seen:
                seen.add(g)
                uniq.append(g)

        # Cache-skip: drop GTIs scraped recently (but only if they're not stale)
        if rescrape_hours and rescrape_hours > 0:
            try:
                _ensure_tables(conn)
                cutoff_mod = f"-{int(rescrape_hours)} hours"
                cur = conn.execute(
                    "SELECT gti FROM amazon_channels "
                    "WHERE last_updated_utc IS NOT NULL "
                    "AND datetime(last_updated_utc) >= datetime('now', ?) "
                    "AND (is_stale IS NULL OR is_stale = 0)",
                    (cutoff_mod,),
                )
                recent = {r[0] for r in cur.fetchall() if r and r[0]}
                if recent:
                    before = len(uniq)
                    uniq = [g for g in uniq if g not in recent]
                    LOG.info(
                        "Cache-skip: filtered %d recently-scraped GTIs (rescrape_hours=%d). Remaining=%d",
                        before - len(uniq),
                        int(rescrape_hours),
                        len(uniq),
                    )
            except Exception:
                LOG.debug("Cache-skip: failed; continuing without cache filtering", exc_info=True)

        # Apply hard cap last
        if limit and limit > 0:
            uniq = uniq[:limit]
        return uniq
    finally:
        conn.close()

def gti_to_url(gti: str) -> str:
    # Amazon Video detail page by GTI
    return f"https://www.amazon.com/gp/video/detail/{gti}"

def _parse_benefit_id(html: str) -> str:
    for rx in BENEFIT_RE_LIST:
        for m in rx.findall(html):
            # Filter common false positives
            if not m:
                continue
            if m.lower() == "amzn1":
                continue
            return m
    return ""

def _normalize(benefit_id: str, entitlement: str, page_text: str) -> Tuple[str, str, str]:
    """
    Returns: (channel_name, channel_id, failure_reason_if_any_for_unknown)
    channel_id is your canonical service id (aiv_*) when possible.
    """
    if benefit_id in BENEFIT_MAP:
        name, sid = BENEFIT_MAP[benefit_id]
        return name, sid, ""

    # Infer from entitlement first, then page text
    for rx, (name, sid) in TEXT_INFER:
        if entitlement and rx.search(entitlement):
            return name, sid, f"UNKNOWN_BENEFIT_ID benefit_id={benefit_id} inferred_from=entitlement"
    for rx, (name, sid) in TEXT_INFER:
        if page_text and rx.search(page_text):
            return name, sid, f"UNKNOWN_BENEFIT_ID benefit_id={benefit_id} inferred_from=page_text"

    # Fallback: use entitlement text as name; make a stable-ish id
    safe_name = entitlement.strip() or "Amazon Error"
    slug = re.sub(r"[^a-z0-9]+", "_", safe_name.lower()).strip("_")
    sid = f"aiv_{slug}" if slug else "aiv_aggregator"
    reason = f"UNKNOWN_BENEFIT_ID benefit_id={benefit_id} fallback_to_entitlement"
    return safe_name, sid, reason

async def _extract_entitlement_text(page) -> str:
    # Try a couple likely selectors; keep it fast.
    selectors = [
        '[data-automation-id="entitlement-message"]',
        '[data-testid="entitlement-message"]',
        '#entitlement-message',
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                txt = (await loc.first.inner_text()).strip()
                if txt:
                    return txt
        except Exception:
            continue
    return ""

async def scrape_one(playwright, browser, gti: str, timeout_ms: int, retries: int,
                    unknown_seen: set, progress_idx: int, total: int) -> ScrapeResult:
    url = gti_to_url(gti)
    start = time.time()
    last_err = ""
    benefit_id = ""
    entitlement = ""
    channel_name = ""
    channel_id = ""
    failure_reason = ""
    status = "ERROR"

    for attempt in range(retries + 1):
        ctx = None
        page = None
        try:
            LOG.info("[SCRAPE] %d/%d GTI=%s URL=%s attempt=%d", progress_idx, total, gti, url, attempt + 1)
            ctx = await browser.new_context()
            page = await ctx.new_page()

            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            # light wait for client-rendered data without requiring full load
            try:
                await page.wait_for_timeout(250)
            except Exception:
                pass

            html = await page.content()
            benefit_id = _parse_benefit_id(html)

            entitlement = await _extract_entitlement_text(page)

            # page text for inference fallback (bounded)
            page_text = ""
            try:
                page_text = (await page.inner_text("body"))[:20000]
            except Exception:
                page_text = ""

            channel_name, channel_id, unknown_reason = _normalize(benefit_id, entitlement, page_text)
            if unknown_reason and benefit_id and benefit_id not in unknown_seen:
                unknown_seen.add(benefit_id)
                LOG.warning("%s entitlement=%r", unknown_reason, entitlement)

            # classify common stale/404-ish cases from HTML
            if "Sorry, we couldn't find" in html or "Page Not Found" in html:
                status = "STALE"
                failure_reason = "STALE_GTI_404"
                channel_name = ""
                channel_id = ""
            else:
                status = "SUCCESS"
                failure_reason = ""

            elapsed = int((time.time() - start) * 1000)
            LOG.info("[RESULT] %d/%d GTI=%s status=%s channel_id=%s channel_name=%s benefit_id=%s elapsed_ms=%d",
                     progress_idx, total, gti, status, channel_id, channel_name, benefit_id, elapsed)

            return ScrapeResult(
                gti=gti, url=url, status=status,
                channel_id=channel_id, channel_name=channel_name,
                benefit_id=benefit_id, entitlement_text=entitlement,
                failure_reason=failure_reason, elapsed_ms=elapsed
            )

        except PlaywrightTimeoutError:
            last_err = "TIMEOUT"
            status = "TIMEOUT"
            failure_reason = "TIMEOUT"
            LOG.warning("[RESULT] %d/%d GTI=%s status=TIMEOUT attempt=%d", progress_idx, total, gti, attempt + 1)
            # retry with fresh context
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            status = "ERROR"
            failure_reason = last_err
            LOG.warning("[RESULT] %d/%d GTI=%s status=ERROR attempt=%d error=%s",
                        progress_idx, total, gti, attempt + 1, last_err)
        finally:
            try:
                if page:
                    await page.close()
            except Exception:
                pass
            try:
                if ctx:
                    await ctx.close()
            except Exception:
                pass

    elapsed = int((time.time() - start) * 1000)
    return ScrapeResult(
        gti=gti, url=url, status=status,
        channel_id=channel_id, channel_name=channel_name,
        benefit_id=benefit_id, entitlement_text=entitlement,
        failure_reason=(failure_reason or last_err),
        elapsed_ms=elapsed
    )

def _debug_csv_path(db_path: str) -> str:
    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    p = Path(db_path).resolve()
    return str(p.parent / f"amazon_scrape_{ts}.csv")

def write_debug_csv(path: str, results: Sequence[ScrapeResult]) -> None:
    fields = [
        "gti", "url", "status",
        "channel_id", "channel_name",
        "benefit_id", "entitlement_text",
        "failure_reason", "elapsed_ms",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            w.writerow({
                "gti": r.gti,
                "url": r.url,
                "status": r.status,
                "channel_id": r.channel_id,
                "channel_name": r.channel_name,
                "benefit_id": r.benefit_id,
                "entitlement_text": r.entitlement_text,
                "failure_reason": r.failure_reason,
                "elapsed_ms": r.elapsed_ms,
            })


def prune_old_debug_csvs(db_path: str, keep: int = 3) -> int:
    """Delete old amazon_scrape_*.csv files next to the DB, keeping newest N.
    Returns number of files deleted. keep<=0 means keep all.
    """
    if keep is None or keep <= 0:
        return 0
    p = Path(db_path).resolve()
    folder = p.parent
    files = sorted(folder.glob("amazon_scrape_*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
    if len(files) <= keep:
        return 0
    deleted = 0
    for f in files[keep:]:
        try:
            f.unlink()
            deleted += 1
        except Exception:
            pass
    return deleted

def upsert_results(db_path: str, results: Sequence[ScrapeResult]) -> int:
    conn = sqlite3.connect(db_path)
    try:
        _ensure_tables(conn)
        n = 0
        now = _utcnow_iso()
        for r in results:
            # Skip ERROR and TIMEOUT - only write SUCCESS and STALE
            if r.status not in ("SUCCESS", "STALE"):
                continue
            
            # Build an UPSERT that matches whatever schema exists
            cols_cur = conn.execute("PRAGMA table_info(amazon_channels)")
            cols = {row[1] for row in cols_cur.fetchall()}
            has_lu = "last_updated_utc" in cols
            has_stale = "is_stale" in cols
            
            # Determine is_stale value
            is_stale_val = 1 if r.status == "STALE" else 0

            if has_lu and has_stale:
                conn.execute(
                    "INSERT INTO amazon_channels(gti, channel_id, channel_name, last_updated_utc, is_stale) "
                    "VALUES(?,?,?,?,?) "
                    "ON CONFLICT(gti) DO UPDATE SET "
                    "channel_id=excluded.channel_id, "
                    "channel_name=excluded.channel_name, "
                    "last_updated_utc=excluded.last_updated_utc, "
                    "is_stale=excluded.is_stale",
                    (r.gti, r.channel_id, r.channel_name, now, is_stale_val),
                )
            elif has_lu:
                conn.execute(
                    "INSERT INTO amazon_channels(gti, channel_id, channel_name, last_updated_utc) "
                    "VALUES(?,?,?,?) "
                    "ON CONFLICT(gti) DO UPDATE SET "
                    "channel_id=excluded.channel_id, "
                    "channel_name=excluded.channel_name, "
                    "last_updated_utc=excluded.last_updated_utc",
                    (r.gti, r.channel_id, r.channel_name, now),
                )
            elif has_stale:
                conn.execute(
                    "INSERT INTO amazon_channels(gti, channel_id, channel_name, is_stale) "
                    "VALUES(?,?,?,?) "
                    "ON CONFLICT(gti) DO UPDATE SET "
                    "channel_id=excluded.channel_id, "
                    "channel_name=excluded.channel_name, "
                    "is_stale=excluded.is_stale",
                    (r.gti, r.channel_id, r.channel_name, is_stale_val),
                )
            else:
                conn.execute(
                    "INSERT INTO amazon_channels(gti, channel_id, channel_name) "
                    "VALUES(?,?,?) "
                    "ON CONFLICT(gti) DO UPDATE SET "
                    "channel_id=excluded.channel_id, "
                    "channel_name=excluded.channel_name",
                    (r.gti, r.channel_id, r.channel_name),
                )
            n += 1
        conn.commit()
        return n
    finally:
        conn.close()


async def run(
    db: str,
    max_n: int,
    workers: int,
    timeout_ms: int,
    retries: int,
    horizon_hours: int = 72,
    past_hours: int = 6,
    rescrape_hours: int = 48,
    log_every: int = 10,
    keep_debug: int = 3,
) -> int:
    gtis = extract_gtis(db, max_n, horizon_hours=horizon_hours, past_hours=past_hours, rescrape_hours=rescrape_hours)
    total_gtis = len(gtis)
    LOG.info("Extracted %d GTIs from DB", total_gtis)
    if not gtis:
        LOG.info("No GTIs found; exiting.")
        return 0

    unknown_seen: set = set()

    # Preflight: detect (but do NOT kill) leftover chromium/playwright processes.
    # Killing processes from inside a container can race Playwright's driver and cause:
    #   BrowserType.launch: Connection closed while reading from the driver
    try:
        leftovers = count_processes(["playwright", "chromium", "chrome"])
        if leftovers:
            LOG.warning(
                "Preflight: detected %d existing chromium/playwright processes. "
                "If you see launch failures, restart the container to clear them.",
                leftovers,
            )
    except Exception:
        LOG.debug("Preflight: process scan failed", exc_info=True)

    async with async_playwright() as p:
        LOG.info("Launching Playwright Chromium (headless=True)")

        browser = None
        last_launch_err: Exception | None = None
        for launch_attempt in range(3):
            try:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-dev-shm-usage",
                        "--no-sandbox",
                        "--disable-blink-features=AutomationControlled",
                    ],
                )
                break
            except Exception as e:
                last_launch_err = e
                LOG.warning(
                    "Browser launch attempt %d/3 failed: %s: %s",
                    launch_attempt + 1,
                    type(e).__name__,
                    e,
                )
                await asyncio.sleep(0.5 * (launch_attempt + 1))

        if browser is None:
            raise RuntimeError(
                "Failed to launch browser after 3 attempts: "
                f"{type(last_launch_err).__name__}: {last_launch_err}"
            )

        results: List[ScrapeResult] = []
        sem = asyncio.Semaphore(max(1, workers))

        done = 0
        cnt_success = 0
        cnt_timeout = 0
        cnt_stale = 0
        cnt_error = 0
        lock = asyncio.Lock()

        async def _note_result(r: ScrapeResult) -> None:
            nonlocal done, cnt_success, cnt_timeout, cnt_stale, cnt_error
            async with lock:
                done += 1
                if r.status == "SUCCESS":
                    cnt_success += 1
                elif r.status == "TIMEOUT":
                    cnt_timeout += 1
                elif r.status == "STALE":
                    cnt_stale += 1
                else:
                    cnt_error += 1

                if log_every and (done % log_every == 0 or done == total_gtis):
                    LOG.info(
                        "[PROGRESS] %d/%d success=%d timeout=%d stale=%d error=%d",
                        done,
                        total_gtis,
                        cnt_success,
                        cnt_timeout,
                        cnt_stale,
                        cnt_error,
                    )

            # Per-item logging: keep it quieter when stable.
            # - Success: DEBUG
            # - Failures: INFO (so you can spot GTIs to human-review)
            if r.status == "SUCCESS":
                LOG.debug(
                    "[RESULT] %d/%d GTI=%s status=%s channel_id=%s channel_name=%s benefit_id=%s elapsed_ms=%s",
                    done,
                    total_gtis,
                    r.gti,
                    r.status,
                    r.channel_id,
                    r.channel_name,
                    r.benefit_id,
                    r.elapsed_ms,
                )
            else:
                LOG.info(
                    "[FAIL] %d/%d GTI=%s status=%s reason=%s channel_id=%s channel_name=%s benefit_id=%s elapsed_ms=%s",
                    done,
                    total_gtis,
                    r.gti,
                    r.status,
                    r.failure_reason,
                    r.channel_id,
                    r.channel_name,
                    r.benefit_id,
                    r.elapsed_ms,
                )

        async def _task(i: int, gti: str) -> None:
            async with sem:
                r = await scrape_one(
                    p,
                    browser,
                    gti,
                    timeout_ms,
                    retries,
                    unknown_seen,
                    i + 1,
                    total_gtis,
                )
                async with lock:
                    results.append(r)
                await _note_result(r)

        try:
            tasks = [asyncio.create_task(_task(i, g)) for i, g in enumerate(gtis)]
            await asyncio.gather(*tasks)
        finally:
            try:
                await browser.close()
            except Exception:
                LOG.debug("Browser close failed", exc_info=True)

    # Sort results by original GTI order for stable CSV
    order = {g: i for i, g in enumerate(gtis)}
    results.sort(key=lambda r: order.get(r.gti, 10**9))

    csv_path = _debug_csv_path(db)
    write_debug_csv(csv_path, results)
    LOG.info("Debug report exported: %s", csv_path)

    # Prune old debug CSVs (keep last N)
    try:
        if keep_debug and keep_debug > 0:
            pruned = prune_old_debug_csvs(db, keep_debug)
            if pruned:
                LOG.info("Pruned %d old debug CSV(s)", pruned)
    except Exception:
        LOG.debug("Debug CSV pruning failed", exc_info=True)

    upserts = upsert_results(db, results)
    LOG.info("Database updated (%d successful upserts).", upserts)

    # Summary
    total = len(results)
    ok = sum(1 for r in results if r.status == "SUCCESS")
    timeouts = sum(1 for r in results if r.status == "TIMEOUT")
    stale = sum(1 for r in results if r.status == "STALE")
    err = sum(1 for r in results if r.status == "ERROR")
    LOG.info("Summary: total=%d success=%d timeout=%d stale=%d error=%d", total, ok, timeouts, stale, err)

    return 0

def main(
argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to fruit_events.db")
    ap.add_argument("--max", type=int, default=0, help="Max GTIs to scrape (0 = all found)")
    ap.add_argument("--horizon-hours", type=int, default=72, help="Only consider events starting within the next N hours (default 72). Set 0 to disable horizon filter.")
    ap.add_argument("--past-hours", type=int, default=6, help="Include events that ended within the last N hours (default 6).")
    ap.add_argument("--rescrape-hours", type=int, default=48, help="Skip GTIs successfully scraped within the last N hours (default 48). Set 0 to disable cache-skip.")
    ap.add_argument("--workers", type=int, default=3, help="Concurrent workers (contexts)")
    ap.add_argument("--timeout-ms", type=int, default=30000, help="Navigation timeout per GTI")
    ap.add_argument("--retries", type=int, default=1, help="Retries per GTI on timeout/error")
    ap.add_argument("--log-level", default="INFO", help="INFO, DEBUG, WARNING")
    ap.add_argument("--log-every", type=int, default=10, help="Progress log cadence in completed items (0 disables)")
    ap.add_argument("--keep-debug", type=int, default=3, help="Keep last N debug CSVs next to DB (0 keeps all)")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    return asyncio.run(run(
        args.db,
        args.max,
        args.workers,
        args.timeout_ms,
        args.retries,
        horizon_hours=args.horizon_hours,
        past_hours=args.past_hours,
        rescrape_hours=args.rescrape_hours,
        log_every=args.log_every,
        keep_debug=args.keep_debug,
    ))

if __name__ == "__main__":
    raise SystemExit(main())
