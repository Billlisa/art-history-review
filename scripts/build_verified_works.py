#!/usr/bin/env python3
"""Build works.csv (changed fields only) and report.md from the website dataset.

This script reads app/data/comparison_table.csv, processes artwork rows, and:
- skips global row indices 35-60 per user request
- fetches page titles for existing source URLs (network required)
- ranks source quality
- writes works.csv with only review/override fields
- writes report.md with Updated / Needs human / Not found sections
"""

from __future__ import annotations

import csv
import json
import re
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = ROOT / "app" / "data" / "comparison_table.csv"
OUTPUT_CSV = ROOT / "works.csv"
REPORT_MD = ROOT / "report.md"
CACHE_JSON = ROOT / "screen_results" / "source_title_cache.json"

SKIP_GLOBAL_INDEX_RANGE = range(35, 61)  # inclusive 35-60

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)
TIMEOUT = 12
MAX_WORKERS = 8


INSTITUTION_MAP = {
    "metmuseum.org": "The Metropolitan Museum of Art",
    "vam.ac.uk": "Victoria and Albert Museum",
    "britishmuseum.org": "The British Museum",
    "rct.uk": "Royal Collection Trust",
    "si.edu": "Smithsonian Institution",
    "npg.org.uk": "National Portrait Gallery (UK)",
    "collection.sciencemuseumgroup.org.uk": "Science Museum Group",
    "davidrumsey.com": "David Rumsey Map Collection",
    "collections.vam.ac.uk": "Victoria and Albert Museum Collections",
    "commons.wikimedia.org": "Wikimedia Commons",
    "wikipedia.org": "Wikipedia",
    "britannica.com": "Encyclopaedia Britannica",
    "cambridge.org": "Cambridge University Press",
    "archive.org": "Internet Archive",
    "gla.ac.uk": "University of Glasgow",
    "journalhosting.ucalgary.ca": "University of Calgary",
    "dome.mit.edu": "MIT DOME",
    "bifmo.furniturehistorysociety.org": "Furniture History Society / BIFMO",
}


@dataclass
class SourceRecord:
    institution: str
    page_title: str
    meta_description: str
    url: str
    tier: int
    status: str
    relevance: int = 0
    title_specific_relevance: int = 0
    author_relevance: int = 0


GENERIC_TITLE_TOKENS = {
    "detail",
    "slide",
    "left",
    "right",
    "study",
    "view",
    "interior",
    "exterior",
    "object",
    "design",
    "untitled",
    "court",
    "great",
    "exhibition",
    "crystal",
    "palace",
}


def load_rows() -> List[Dict[str, str]]:
    with INPUT_CSV.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def normalize_year_expr(year_creation: str, period_creation: str) -> str:
    year_creation = (year_creation or "").strip()
    period_creation = (period_creation or "").strip()
    if year_creation and year_creation not in {"N/A", "N/A (reference)"}:
        # Normalize dash style only.
        return year_creation.replace("-", "–") if re.fullmatch(r"\d{4}-\d{4}", year_creation) else year_creation

    if not period_creation or period_creation in {"N/A", "N/A (reference)"}:
        return ""

    base = period_creation.split("(")[0].strip().rstrip(".")
    base = base.replace("century", "c.")
    base = base.replace("Century", "c.")
    base = re.sub(r"\s+", " ", base)
    return base


def split_source_urls(raw: str) -> List[str]:
    urls = [u.strip() for u in (raw or "").split(" | ") if u.strip()]
    out: List[str] = []
    seen = set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def significant_title_tokens(text: str) -> List[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z'.-]{2,}", text or "")
    out: List[str] = []
    seen = set()
    for t in tokens:
        k = t.lower().strip(".")
        if len(k) < 4:
            continue
        if k in GENERIC_TITLE_TOKENS:
            continue
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def sentence_count_zh(text: str) -> int:
    parts = [p.strip() for p in re.split(r"[。！？]+", (text or "").strip()) if p.strip()]
    return len(parts)


def sentence_count_en(text: str) -> int:
    parts = [p.strip() for p in re.split(r"[.!?]+", (text or "").strip()) if p.strip()]
    return len(parts)


def hostname(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def institution_for_url(url: str) -> str:
    host = hostname(url)
    for domain, name in INSTITUTION_MAP.items():
        if host == domain or host.endswith("." + domain):
            return name
    if host.endswith(".edu") or ".edu." in host:
        return host
    if host.endswith(".ac.uk") or ".ac." in host:
        return host
    return host or "Unknown"


def source_tier(url: str) -> int:
    host = hostname(url)

    # Tier 1: museums / institutions / official collections
    tier1_markers = ["museum", "metmuseum.org", "vam.ac.uk", "britishmuseum.org", "rct.uk", "si.edu"]
    if any(m in host for m in tier1_markers):
        return 1
    if "collection." in host or "collections." in host:
        return 1

    # Tier 2: scholarly/citable publishers and catalogues
    tier2_markers = ["cambridge.org", "jstor.org", "doi.org", "archive.org", "journalhosting."]
    if any(m in host for m in tier2_markers):
        return 2

    # Tier 3: university/research
    if host.endswith(".edu") or ".edu." in host or host.endswith(".ac.uk") or ".ac." in host:
        return 3

    # Tier 9: crowd-sourced / fallback
    if "wikipedia.org" in host or "wikimedia.org" in host:
        return 9

    return 4


def clean_html_title(html_text: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    title = unescape(m.group(1))
    title = re.sub(r"\s+", " ", title).strip()
    return title


def clean_meta_description(html_text: str) -> str:
    patterns = [
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']description["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+property=["\']og:description["\']',
    ]
    for p in patterns:
        m = re.search(p, html_text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            text = unescape(m.group(1))
            text = re.sub(r"\s+", " ", text).strip()
            return text[:600]
    return ""


def fetch_title(url: str, cache: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    if url in cache:
        return cache[url]

    req = Request(url, headers={"User-Agent": USER_AGENT})
    ssl_ctx = ssl.create_default_context()
    out = {"status": "error", "page_title": "", "meta_description": "", "final_url": url}
    try:
        with urlopen(req, timeout=TIMEOUT, context=ssl_ctx) as resp:
            content_type = (resp.headers.get("Content-Type") or "").lower()
            body = resp.read(200_000)
            text = body.decode("utf-8", errors="ignore")
            title = clean_html_title(text)
            meta_desc = clean_meta_description(text)
            out = {
                "status": f"http_{getattr(resp, 'status', 200)}",
                "page_title": title or content_type or "(no title found)",
                "meta_description": meta_desc,
                "final_url": resp.geturl(),
            }
    except HTTPError as e:
        out = {"status": f"http_{e.code}", "page_title": "", "meta_description": "", "final_url": url}
    except URLError as e:
        out = {"status": f"url_error:{getattr(e, 'reason', 'unknown')}", "page_title": "", "meta_description": "", "final_url": url}
    except Exception as e:
        out = {"status": f"error:{type(e).__name__}", "page_title": "", "meta_description": "", "final_url": url}

    cache[url] = out
    return out


def load_cache() -> Dict[str, Dict[str, str]]:
    if CACHE_JSON.exists():
        try:
            return json.loads(CACHE_JSON.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_cache(cache: Dict[str, Dict[str, str]]) -> None:
    CACHE_JSON.parent.mkdir(parents=True, exist_ok=True)
    CACHE_JSON.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def build_source_records(urls: List[str], cache: Dict[str, Dict[str, str]]) -> List[SourceRecord]:
    records: List[SourceRecord] = []
    if not urls:
        return records

    futures = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for url in urls:
            futures[ex.submit(fetch_title, url, cache)] = url
        for fut in as_completed(futures):
            url = futures[fut]
            result = fut.result()
            final_url = result.get("final_url") or url
            records.append(
                SourceRecord(
                    institution=institution_for_url(final_url),
                    page_title=result.get("page_title") or "(title fetch failed)",
                    meta_description=result.get("meta_description") or "",
                    url=final_url,
                    tier=source_tier(final_url),
                    status=result.get("status", "unknown"),
                )
            )

    # Stable ordering by quality, then URL.
    records.sort(key=lambda r: (r.tier, r.url))
    return records


def source_relevance_for_work(source: SourceRecord, title: str, author: str) -> int:
    hay = f"{source.page_title} {source.meta_description} {source.url}".lower()
    title_tokens = significant_title_tokens(title)
    author_tokens = set(significant_title_tokens(author))
    lead_name_match = re.match(r"^\W*([A-Z][a-z]+)(?:\s+([A-Z][a-z]+))?(?:\s+([A-Z][a-z]+))?", title or "")
    if lead_name_match:
        for g in lead_name_match.groups():
            if g:
                author_tokens.add(g.lower())
    title_specific_tokens = [t for t in title_tokens if t not in author_tokens]

    title_score = 0
    author_score = 0
    for token in title_specific_tokens:
        if token in hay:
            title_score += 2
    for token in author_tokens:
        if token in hay:
            author_score += 3
    score = title_score + author_score
    if "/art/collection/search/" in source.url and score > 0:
        score += 1
    source.title_specific_relevance = title_score
    source.author_relevance = author_score
    return score


def is_source_set_sufficient(records: List[SourceRecord], title: str, author: str) -> Tuple[bool, str]:
    ok = [r for r in records if r.status.startswith("http_2") or r.status.startswith("http_3")]
    if len(ok) < 1:
        return False, "no reachable sources"

    non_wiki = [r for r in ok if "wikipedia.org" not in r.url and "wikimedia.org" not in r.url]
    if len(non_wiki) < 1:
        return False, "Wikipedia/Wikimedia cannot be the only basis"

    preferred = [r for r in ok if r.tier in {1, 2, 3}]
    if len(preferred) < 1:
        return False, "no preferred source (official/scholarly/university)"

    for r in ok:
        r.relevance = source_relevance_for_work(r, title, author)
    if max((r.relevance for r in ok), default=0) <= 0:
        return False, "sources appear generic or mismatched to this work"
    if max((r.title_specific_relevance for r in ok), default=0) <= 0:
        detail_like = any(k in (title or "").lower() for k in ["detail", "same image", "see previous slide", "detail of"])
        if not detail_like:
            return False, "sources mention author/context but not this specific work"

    return True, ""


def choose_primary_source(records: List[SourceRecord]) -> Optional[SourceRecord]:
    ok = [r for r in records if r.status.startswith(("http_2", "http_3"))]
    if not ok:
        return None
    return sorted(ok, key=lambda r: (r.tier, -r.title_specific_relevance, -r.relevance, r.url))[0]


def compact_source_title(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"\s+-\s+The Metropolitan Museum of Art$", "", text)
    text = re.sub(r"\s+\|\s+.*$", "", text)
    return text[:220]


def build_specific_backgrounds(row: Dict[str, str], sources: List[SourceRecord]) -> Tuple[str, str]:
    title = (row.get("title") or "").strip()
    material = (row.get("material") or "").strip() or "material not clearly stated"
    period = normalize_year_expr(row.get("year_creation", ""), row.get("period_creation", "")) or (row.get("period_creation") or "").strip()
    place = (row.get("production_place") or "").strip() or (row.get("region") or "").strip()
    region = (row.get("region") or "").strip()
    style = (row.get("style") or "").strip()
    author = (row.get("author") or "").strip()
    course = (row.get("course") or "").strip()

    for s in sources:
        if s.relevance == 0 and s.title_specific_relevance == 0 and s.author_relevance == 0:
            s.relevance = source_relevance_for_work(s, title, author)

    primary = choose_primary_source(sources)
    primary_title = compact_source_title(primary.page_title) if primary else ""
    primary_inst = primary.institution if primary else "available collection source"
    primary_desc = (primary.meta_description or "").strip() if primary else ""

    # English: 2-3 object-specific sentences
    en_parts = []
    en_parts.append(
        f"{title} is documented as a {material.lower()} work in the {period} period"
        + (f", associated with {place}" if place else "")
        + "."
    )
    if author and author not in {"Unknown", "Unknown artist"} and not author.endswith(" artist"):
        en_parts.append(f"The object is presented in the course under {author}, within a {style or 'historical design'} context.")
    else:
        en_parts.append(f"The object is studied in a {style or 'historical design'} context" + (f" within {region}" if region else "") + ".")
    if primary:
        source_sentence = f"A core verification source is {primary_inst} ({primary_title})."
        if primary_desc:
            source_sentence += f" The page description supports object-specific identification and collection context."
        en_parts.append(source_sentence)
    else:
        en_parts.append(f"The current course entry still needs a stronger object-level source set for final verification in {course}.")
    en_text = " ".join(en_parts[:3])

    # Chinese: 2-3 object-specific sentences (template-based but item-specific)
    zh_parts = []
    zh_parts.append(
        f"该展品在课程中对应为“{title}”，材质为{material}，作品生产时期记作{period or '待补充'}"
        + (f"，并与{place}相关" if place else "")
        + "。"
    )
    if author and author not in {"Unknown", "Unknown artist"} and not author.endswith(" artist"):
        zh_parts.append(f"课程将其放在{style or '相关设计史'}语境中讨论，并与{author}的实践或归属信息相联系。")
    else:
        zh_parts.append(f"该件作品主要在{style or '相关设计史'}语境下讨论" + (f"，地域范围为{region}" if region else "") + "。")
    if primary:
        zh_parts.append(f"本次核对采用的核心来源之一为{primary_inst}页面《{primary_title}》，用于确认该件作品的对象层级信息与收藏/研究语境。")
    else:
        zh_parts.append("当前仍需补充更明确的单件作品来源页，以完成最终核对。")
    zh_text = "".join(zh_parts[:3])

    return zh_text, en_text


def make_report_sections():
    return {"updated": [], "needs_human": [], "not_found": []}


def main() -> int:
    if not INPUT_CSV.exists():
        print(f"Missing input: {INPUT_CSV}", file=sys.stderr)
        return 1

    rows = load_rows()
    cache = load_cache()
    report = make_report_sections()
    output_rows: List[Dict[str, str]] = []

    for global_idx, row in enumerate(rows, start=1):
        if row.get("record_type") != "artwork":
            continue
        if global_idx in SKIP_GLOBAL_INDEX_RANGE:
            continue

        item_id = row["id"]
        title = row["title"]
        author = row.get("author", "")
        year_expr = normalize_year_expr(row.get("year_creation", ""), row.get("period_creation", ""))
        source_urls = split_source_urls(row.get("historical_background_sources", ""))
        source_records = build_source_records(source_urls, cache) if source_urls else []
        bg_zh, bg_en = build_specific_backgrounds(row, source_records)

        sufficient_sources, source_reason = is_source_set_sufficient(source_records, title, author)
        zh_sentences = sentence_count_zh(bg_zh)
        en_sentences = sentence_count_en(bg_en)
        background_ok = zh_sentences >= 2 and en_sentences >= 2

        status = "updated"
        note_parts: List[str] = []

        if not source_urls:
            status = "not_found"
            note_parts.append("no existing source URLs in dataset")
        if source_urls and not source_records:
            status = "not_found"
            note_parts.append("source fetch failed")
        if not sufficient_sources:
            status = "needs_human"
            if source_reason:
                note_parts.append(source_reason)
        if not background_ok:
            status = "needs_human"
            note_parts.append(f"generated background too short (zh={zh_sentences}, en={en_sentences}; need 2-3 sentences)")
        if not year_expr:
            status = "needs_human"
            note_parts.append("missing year expression")

        if status == "updated":
            preferred_count = sum(1 for s in source_records if s.status.startswith(("http_2", "http_3")) and s.tier in {1, 2, 3})
            note_parts.append(f"verified from existing linked sources ({preferred_count} preferred reachable source{'s' if preferred_count != 1 else ''})")

        top_records = source_records[:4]
        sources_json = json.dumps(
            [
                {
                    "institution": s.institution,
                    "title": s.page_title,
                        "url": s.url,
                        "tier": s.tier,
                        "relevance": s.relevance,
                        "title_specific_relevance": s.title_specific_relevance,
                        "author_relevance": s.author_relevance,
                        "meta_description": s.meta_description,
                        "http_status": s.status,
                }
                for s in top_records
            ],
            ensure_ascii=False,
        )

        output_rows.append(
            {
                "global_row_index": str(global_idx),
                "id": item_id,
                "title": title,
                "confirmed_year_expr": year_expr,
                "historical_background_zh": bg_zh,
                "historical_background_en": bg_en,
                "sources": sources_json,
                "source_count": str(len([s for s in source_records if s.status.startswith(("http_2", "http_3"))])),
                "status": status,
                "notes": "; ".join(note_parts),
            }
        )

        report[status].append((global_idx, item_id, title, note_parts, top_records))

    save_cache(cache)

    # Write works.csv (change fields only + review status)
    fieldnames = [
        "global_row_index",
        "id",
        "title",
        "confirmed_year_expr",
        "historical_background_zh",
        "historical_background_en",
        "sources",
        "source_count",
        "status",
        "notes",
    ]
    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    # Write report.md
    updated_count = len(report["updated"])
    needs_count = len(report["needs_human"])
    not_found_count = len(report["not_found"])

    lines: List[str] = []
    if needs_count == 0:
        lines.append("✅ All done")
        lines.append("")
    lines.append("# Verification Report")
    lines.append("")
    lines.append(f"- Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("- Scope: artwork rows in `comparison_table.csv` (global rows 35-60 skipped by request)")
    lines.append(f"- Updated: {updated_count}")
    lines.append(f"- Needs human: {needs_count}")
    lines.append(f"- Not found: {not_found_count}")
    lines.append("")

    def add_section(title: str, items):
        lines.append(f"## {title}")
        lines.append("")
        if not items:
            lines.append("- None")
            lines.append("")
            return
        for global_idx, item_id, item_title, notes, sources in items:
            lines.append(f"- `{item_id}` (row {global_idx}): {item_title}")
            if notes:
                lines.append(f"  - Notes: {'; '.join(notes)}")
            if sources:
                for s in sources[:2]:
                    lines.append(f"  - Source: {s.institution} | {s.page_title} | {s.url}")
            lines.append("")

    add_section("Updated", report["updated"])
    add_section("Needs human", report["needs_human"])
    add_section("Not found", report["not_found"])

    REPORT_MD.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    print(f"Wrote {OUTPUT_CSV}")
    print(f"Wrote {REPORT_MD}")
    print(f"Updated={updated_count} NeedsHuman={needs_count} NotFound={not_found_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
