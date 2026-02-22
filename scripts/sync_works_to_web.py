#!/usr/bin/env python3
"""Sync verified works.csv fields back into app/data artifacts.

Rules:
- update historical backgrounds (ZH/EN + combined) for all rows present in works.csv
- update period/sources only for rows with status=updated
- keep rows 35-60 untouched implicitly (they are excluded from works.csv)
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKS_CSV = ROOT / "works.csv"
ARTWORKS_JSON = ROOT / "app" / "data" / "artworks.json"


def load_works():
    rows = list(csv.DictReader(WORKS_CSV.open(encoding="utf-8-sig", newline="")))
    return {r["id"]: r for r in rows}


def parse_sources_json(raw: str):
    raw = (raw or "").strip()
    if not raw:
        return []
    try:
        arr = json.loads(raw)
    except Exception:
        return []
    out = []
    seen = set()
    for item in arr:
        url = (item or {}).get("url", "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def build_study_description(material: str, period: str, historical_background_zh: str, historical_background_en: str) -> str:
    period_zh = period or "未标注"
    period_en = period or "Not stated in source slide."
    material_zh = material or "未标注"
    material_en = material or "Not stated in source slide."
    bg_zh = historical_background_zh or "未标注"
    bg_en = historical_background_en or "Not stated in source slide."
    return (
        f"材质：{material_zh}。时期：{period_zh}。历史背景：{bg_zh}\n"
        f"Material: {material_en}. Period: {period_en}. Historical background: {bg_en}"
    )


def sync_artworks_json():
    data = json.loads(ARTWORKS_JSON.read_text(encoding="utf-8"))
    works = load_works()

    background_updates = 0
    verified_updates = 0

    for item in data.get("items", []):
        item_id = item.get("id")
        if not item_id or item_id not in works:
            continue

        row = works[item_id]
        meta = item.setdefault("metadata", {})
        bg_zh = (row.get("historical_background_zh") or "").strip()
        bg_en = (row.get("historical_background_en") or "").strip()
        if bg_zh or bg_en:
            meta["historicalBackgroundZh"] = bg_zh
            meta["historicalBackgroundEn"] = bg_en
            meta["historicalBackground"] = f"{bg_zh}\n{bg_en}".strip()
            background_updates += 1

        if row.get("status") == "updated":
            confirmed_period = (row.get("confirmed_year_expr") or "").strip()
            if confirmed_period:
                meta["period"] = confirmed_period
                # Keep only production period on web; clear year field for artworks.
                if meta.get("recordType") == "artwork":
                    meta["year"] = ""
            src_urls = parse_sources_json(row.get("sources", ""))
            if src_urls:
                meta["historicalBackgroundSources"] = src_urls
            verified_updates += 1

        item["studyDescription"] = build_study_description(
            meta.get("material", ""),
            meta.get("period", ""),
            meta.get("historicalBackgroundZh", ""),
            meta.get("historicalBackgroundEn", ""),
        )

    # Refresh generated timestamp
    data["generatedAt"] = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    ARTWORKS_JSON.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return background_updates, verified_updates


def rebuild_comparison_table():
    sys.path.insert(0, str(ROOT / "scripts"))
    import build_dataset  # type: ignore

    data = json.loads(ARTWORKS_JSON.read_text(encoding="utf-8"))
    build_dataset.write_comparison_table(data.get("items", []))


def main():
    if not WORKS_CSV.exists():
        raise SystemExit(f"Missing {WORKS_CSV}")
    if not ARTWORKS_JSON.exists():
        raise SystemExit(f"Missing {ARTWORKS_JSON}")

    bg_count, verified_count = sync_artworks_json()
    rebuild_comparison_table()
    print(f"Background synced for {bg_count} items")
    print(f"Verified period/source synced for {verified_count} items")
    print("Rebuilt app/data/comparison_table.csv")


if __name__ == "__main__":
    main()
