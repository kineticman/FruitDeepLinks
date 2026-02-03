#!/usr/bin/env python3
"""
audit_xml_icons.py

Quick helper to audit <icon src="..."> URLs in an XMLTV file and optionally rewrite
known-bad beIN placeholder URLs to a stable placeholder.

Defaults are tuned for your current beIN issue:
- Unsplash links (can 404 / rotate / rate-limit)
- beinsports.com DAM logo path you reported as 404

Usage:
  python audit_xml_icons.py --xml direct.xml
  python audit_xml_icons.py --xml direct.xml --rewrite-out direct.fixed.xml

Exit code:
  0 = success (even if bad URLs found)
  2 = file parse error
"""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

from lxml import etree


BAD_SUBSTRINGS = [
    "images.unsplash.com",
    "www.beinsports.com/content/dam/",
]

DEFAULT_PLACEHOLDER = "https://placehold.co/800x450/png"


def is_bad(url: str) -> bool:
    u = (url or "").strip()
    if not u:
        return True
    for s in BAD_SUBSTRINGS:
        if s in u:
            return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit (and optionally rewrite) XMLTV icon URLs")
    ap.add_argument("--xml", required=True, help="Input XMLTV path")
    ap.add_argument("--rewrite-out", help="If set, write a rewritten XML to this path")
    ap.add_argument("--placeholder", default=DEFAULT_PLACEHOLDER, help="Replacement URL for bad icons")
    args = ap.parse_args()

    xml_path = Path(args.xml)
    if not xml_path.exists():
        print(f"ERROR: not found: {xml_path}")
        return 2

    try:
        parser = etree.XMLParser(recover=False, huge_tree=True)
        tree = etree.parse(str(xml_path), parser)
    except Exception as e:
        print(f"ERROR: failed to parse XML: {e}")
        return 2

    icons = tree.xpath("//programme/icon/@src")
    total = len(icons)
    uniq = len(set(icons))

    domains = Counter(urlparse(u).netloc for u in icons)
    bad = [u for u in icons if is_bad(u)]
    bad_uniq = sorted(set(bad))

    print(f"Total <icon> URLs: {total}")
    print(f"Unique <icon> URLs: {uniq}")
    print("\nTop domains:")
    for d, n in domains.most_common(15):
        print(f"  {d or '(none)'}: {n}")

    print(f"\nBad URLs (matched {len(bad)} occurrences, {len(bad_uniq)} unique):")
    for u in bad_uniq[:50]:
        print(f"  {u}")
    if len(bad_uniq) > 50:
        print(f"  ... plus {len(bad_uniq) - 50} more")

    if args.rewrite_out:
        out_path = Path(args.rewrite_out)
        replaced = 0

        # Replace in-place
        for icon in tree.xpath("//programme/icon"):
            src = icon.get("src") or ""
            if is_bad(src):
                icon.set("src", args.placeholder)
                replaced += 1

        out_path.parent.mkdir(parents=True, exist_ok=True)
        tree.write(
            str(out_path),
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=True,
        )
        print(f"\nRewrote XML: {out_path} (replaced {replaced} <icon> src values)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
