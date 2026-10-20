#!/usr/bin/env python3
"""
routes/api/search_terms.py - Apple TV search term config + discovery stats

Routes:
  GET  /api/search-terms                 terms, defaults, per-term history + upcoming counts
  POST /api/search-terms                 {"terms": [...]} to save, {"reset": true} for defaults
  GET  /api/search-terms/events?term=X   upcoming events found by a term (latest run)
"""

import json
import sqlite3
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from core.search_terms import (
    ALWAYS_TERM, DEFAULT_TERMS, PREF_KEY, load_terms, normalize_terms,
)
from db.connection import get_conn, get_conn_or_none, resolve_db_path
from server.logging_setup import log

bp = Blueprint("search_terms", __name__)

HISTORY_RUNS = 30


def _apple_db_path():
    return resolve_db_path().parent / "apple_events.db"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _has_stats(conn: sqlite3.Connection, schema: str = "main") -> bool:
    row = conn.execute(
        f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name='search_term_stats'"
    ).fetchone()
    return row is not None


def _load_history(apple_path):
    """Return (runs, stats_by_term) for the last HISTORY_RUNS runs."""
    runs, by_term = [], {}
    if not apple_path.exists():
        return runs, by_term
    with get_conn(apple_path) as conn:
        if not _has_stats(conn):
            return runs, by_term
        runs = [dict(r) for r in conn.execute(
            "SELECT run_id, started_at, finished_at, term_count, total_found "
            "FROM search_term_runs ORDER BY run_id DESC LIMIT ?", (HISTORY_RUNS,)
        )]
        runs.reverse()
        if not runs:
            return runs, by_term
        for r in conn.execute(
            "SELECT * FROM search_term_stats WHERE run_id >= ? ORDER BY run_id",
            (runs[0]["run_id"],),
        ):
            by_term.setdefault(r["term"], []).append(dict(r))
    return runs, by_term


def _upcoming_counts(apple_path):
    """Per-term counts of events from the latest run that are still upcoming/live in the guide DB."""
    if not apple_path.exists():
        return {}
    with get_conn_or_none() as conn:
        if conn is None:
            return {}
        conn.execute("ATTACH DATABASE ? AS apple", (str(apple_path),))
        if not _has_stats(conn, "apple"):
            return {}
        rows = conn.execute(
            """
            WITH hits AS (
                SELECT h.term, h.event_id FROM apple.search_term_hits h
                JOIN events e ON e.id = 'appletv-' || h.event_id
                WHERE e.end_utc IS NULL OR e.end_utc >= ?
            ),
            n AS (SELECT event_id, COUNT(*) AS c FROM hits GROUP BY event_id)
            SELECT hits.term,
                   COUNT(*) AS upcoming,
                   SUM(CASE WHEN n.c = 1 THEN 1 ELSE 0 END) AS upcoming_unique
            FROM hits JOIN n USING (event_id)
            GROUP BY hits.term
            """,
            (_now_iso(),),
        ).fetchall()
        return {r["term"]: {"upcoming": r["upcoming"], "upcoming_unique": r["upcoming_unique"]} for r in rows}


@bp.route("/api/search-terms", methods=["GET"])
def api_search_terms_get():
    with get_conn_or_none() as conn:
        terms = load_terms(conn)
    apple_path = _apple_db_path()
    try:
        runs, by_term = _load_history(apple_path)
        upcoming = _upcoming_counts(apple_path)
    except Exception as e:
        log(f"Search term stats unavailable: {e}", "WARNING")
        runs, by_term, upcoming = [], {}, {}

    # Terms with history that are no longer configured (removed by the user) are
    # still listed so their last numbers stay visible until they age out.
    configured = {t.lower() for t in terms} | {ALWAYS_TERM}
    removed = sorted(t for t in by_term if t.lower() not in configured)

    return jsonify({
        "status": "success",
        "always_term": ALWAYS_TERM,
        "terms": terms,
        "defaults": DEFAULT_TERMS,
        "customized": terms != DEFAULT_TERMS,
        "removed_with_history": removed,
        "runs": runs,
        "history": by_term,
        "upcoming": upcoming,
    })


@bp.route("/api/search-terms", methods=["POST"])
def api_search_terms_post():
    body = request.json or {}
    if not isinstance(body, dict):
        return jsonify({"status": "error", "message": "Expected JSON object"}), 400

    path = resolve_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS user_preferences (key TEXT PRIMARY KEY, value TEXT, updated_utc TEXT)"
        )
        if body.get("reset"):
            conn.execute("DELETE FROM user_preferences WHERE key = ?", (PREF_KEY,))
            conn.commit()
            log("Apple search terms reset to defaults", "INFO")
            return jsonify({"status": "success", "terms": DEFAULT_TERMS})

        raw = body.get("terms")
        if not isinstance(raw, list):
            return jsonify({"status": "error", "message": "'terms' must be a list"}), 400
        terms = normalize_terms(raw)
        if not terms:
            return jsonify({"status": "error", "message": "At least one search term is required"}), 400

        conn.execute(
            "INSERT OR REPLACE INTO user_preferences (key, value, updated_utc) VALUES (?, ?, ?)",
            (PREF_KEY, json.dumps(terms), datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        log(f"Apple search terms updated ({len(terms)} terms)", "INFO")
        return jsonify({"status": "success", "terms": terms})
    finally:
        conn.close()


@bp.route("/api/search-terms/events", methods=["GET"])
def api_search_term_events():
    term = (request.args.get("term") or "").strip()
    if not term:
        return jsonify({"status": "error", "message": "term is required"}), 400
    apple_path = _apple_db_path()
    if not apple_path.exists():
        return jsonify({"status": "success", "term": term, "events": []})

    with get_conn_or_none() as conn:
        if conn is None:
            return jsonify({"status": "success", "term": term, "events": []})
        conn.execute("ATTACH DATABASE ? AS apple", (str(apple_path),))
        if not _has_stats(conn, "apple"):
            return jsonify({"status": "success", "term": term, "events": []})
        rows = conn.execute(
            """
            SELECT e.id, e.title, e.classification_json, e.start_utc, e.channel_name,
                   (SELECT COUNT(*) FROM apple.search_term_hits o WHERE o.event_id = h.event_id) AS term_hits
            FROM apple.search_term_hits h
            JOIN events e ON e.id = 'appletv-' || h.event_id
            WHERE h.term = ? AND (e.end_utc IS NULL OR e.end_utc >= ?)
            ORDER BY term_hits ASC, e.start_utc ASC
            LIMIT 300
            """,
            (term, _now_iso()),
        ).fetchall()
    events = []
    for r in rows:
        ev = dict(r)
        cls = {}
        try:
            for c in json.loads(ev.pop("classification_json") or "[]"):
                cls.setdefault(c.get("type"), c.get("value"))
        except Exception:
            pass
        ev["league"] = cls.get("league") or cls.get("sport") or ""
        ev["unique"] = r["term_hits"] == 1
        events.append(ev)
    return jsonify({"status": "success", "term": term, "events": events})
