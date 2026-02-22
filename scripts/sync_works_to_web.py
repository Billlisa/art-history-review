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
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKS_CSV = ROOT / "works.csv"
ARTWORKS_JSON = ROOT / "app" / "data" / "artworks.json"

GENERIC_BG_ZH = {
    "常与非洲语境中的仪式权力、宫廷文化、社会记忆，以及后期博物馆收藏史相关。",
    "属于19世纪末设计改革思潮的一部分，反对僵化历史主义，并重新思考装饰、工艺与现代生活。",
    "与工业化、大博览会，以及维多利亚时期关于设计质量、装饰与机械化生产的争论相关。",
}
GENERIC_BG_EN = {
    "Often linked to ritual authority, court culture, social memory, and later museum collection histories in African contexts.",
    "Part of late-19th-century reform movements that rejected rigid historicism and rethought ornament, craft, and modern life.",
    "Connected to industrialization, the Great Exhibition, and Victorian debates about design quality, ornament, and mass manufacture.",
}
MISSING_SOURCE_SENTENCE_ZH = "当前仍需补充更明确的单件作品来源页，以完成最终核对。"
MISSING_SOURCE_SENTENCE_EN = "The current course entry still needs a stronger object-level source set for final verification in"


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


def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _is_generic_background(bg_zh: str, bg_en: str) -> bool:
    zh = _norm_text(bg_zh)
    en = _norm_text(bg_en)
    return any(t in zh for t in GENERIC_BG_ZH) or any(t in en for t in GENERIC_BG_EN)


def _fallback_detail_background(item: dict) -> tuple[str, str]:
    title = (item.get("title") or "").strip()
    meta = item.get("metadata", {}) or {}
    course = (meta.get("courseTitle") or item.get("courseTitle") or "").strip()
    record_type = (meta.get("recordType") or "").strip()
    material = (meta.get("material") or "").strip() or "未标注"
    period = (meta.get("period") or "").strip() or "未标注"
    place = (meta.get("productionPlace") or "").strip() or (meta.get("region") or "").strip() or "未标注"
    style = (meta.get("style") or "").strip() or "课程相关风格"
    author = (meta.get("author") or "").strip()
    title_l = title.lower()

    zh_parts = []
    en_parts = []

    zh_parts.append(
        f"该图像在课程中作为{'参考图像' if record_type == 'reference' else '作品图像'}使用，对应条目为“{title}”，当前记录的材质为{material}、作品生产时期为{period}，地域信息指向{place}。"
    )
    en_parts.append(
        f"This image is used in the course as a {'reference image' if record_type == 'reference' else 'work image'} for “{title}”, with current metadata listing material as {material}, production period as {period}, and place/region as {place}."
    )

    if "coldstream stone" in title_l:
        zh_parts.append("该条目涉及南非旧石器时代末期的刻石/颜料痕迹研究语境，常被用来讨论非洲图像制作传统的长时段起点，而不只是近代博物馆分类中的“部落艺术”框架。")
        zh_parts.append("在复习中可将其与后期宫廷器物或仪式性雕塑区分：这里的重点是史前材料痕迹、考古证据与图像行为的早期证据。")
        en_parts.append("This entry points to a South African Later Stone Age context of incised stone and pigment traces, often used to frame a long history of image-making in Africa rather than only later museum categories of 'tribal art.'")
        en_parts.append("For comparison study, it should be separated from later court or ritual objects: the emphasis here is on archaeological evidence, material traces, and early image practices.")
    elif "map of the kingdom of congo" in title_l or ("map" in title_l and "congo" in title_l):
        zh_parts.append("该图像属于早期近代欧洲关于刚果王国的地图/版画知识生产，反映了旅行记录、传教与贸易信息如何被转译为印刷图像并在欧洲传播。")
        zh_parts.append("复习时可把它作为“非洲对象在跨区域知识网络中的再现”案例，对比课堂中的实物器物如何在功能、使用语境与图像再现方式上不同。")
        en_parts.append("This image belongs to early modern European map/print knowledge production about the Kingdom of Kongo, showing how travel, missionary, and trade information was translated into printed visual form for European circulation.")
        en_parts.append("For review, use it as a case of cross-regional representation of Africa, and contrast it with course objects whose function and use-context differ from printed cartographic imagery.")
    elif "just for context" in title_l:
        zh_parts.append("该条目在课件中被明确标注为“仅作背景语境”，作用是补充时间线、地域范围或比较框架，而非作为重点单件作品要求记忆。")
        zh_parts.append("复习时可提取其与前后主图的关系（例如时代跨度、题材转折或展示语境变化），而不必像主图一样逐项背诵材质与作者信息。")
        en_parts.append("The slide explicitly marks this image as contextual rather than a required object, so its role is to support chronology, geography, or comparison framing instead of serving as a core memorization item.")
        en_parts.append("In review, focus on how it relates to the nearby primary images (period shift, thematic transition, or display context) rather than memorizing it as a standalone object entry.")
    elif "slide 2 image 1" in title_l or "met visit" in title_l:
        zh_parts.append("这类早期导入页图像通常用于建立课程的参观/讨论范围（馆藏部门、地域覆盖与观察方法），帮助你把后续个案放回同一策展与教学框架中。")
        zh_parts.append("复习时可将其当作“课程地图”，记录课堂如何从区域概念进入具体对象，而不是把它当作单件作品进行风格断代。")
        en_parts.append("This kind of early introductory slide image usually establishes the museum-visit or course scope (collection area, regional coverage, and viewing method), helping situate later case studies within the same curatorial and teaching frame.")
        en_parts.append("For review, treat it as a course map that frames how the class moves from regional concepts to specific objects, rather than as a single work for stylistic dating.")
    else:
        if "africa" in course.lower() or "african" in style.lower():
            zh_parts.append("在非洲艺术课程语境中，这一条目应结合对象功能、使用场景与采集/收藏路径来理解，而不仅是以形式特征进行分类。")
            zh_parts.append("如标题中包含人物、地图、织物或版画信息，复习时可分别追问其社会用途、知识生产背景与进入博物馆叙事的方式。")
            en_parts.append("Within the African art course context, this entry is best studied through object function, use setting, and collection history, not only through formal style.")
            en_parts.append("If the title points to a person, map, textile, or print, compare how social use, knowledge production, and museum framing differ across those categories.")
        elif "art nouveau" in style.lower():
            zh_parts.append("该条目可放在新艺术运动关于自然形式、材料表现与整体设计（Gesamtkunstwerk）倾向的讨论中理解，并关注其在展览与消费市场中的传播。")
            zh_parts.append("复习比较时可记录它与同周作品在线条节奏、装饰与结构关系、以及工艺/工业生产方式上的差异。")
            en_parts.append("This entry can be read within Art Nouveau debates on natural form, material expressiveness, and total design, including its circulation through exhibitions and consumer markets.")
            en_parts.append("For comparison, note differences from other works in line rhythm, ornament-structure relations, and craft versus industrial production methods.")
        else:
            zh_parts.append(f"该条目与{style}语境下的材料选择、展示场景和历史用途相关，复习时可优先记录其与同组作品在功能与观看方式上的差异。")
            zh_parts.append("即使当前来源信息有限，也应先从标题、材质与课程位置入手建立可比较的描述框架。")
            en_parts.append(f"This entry relates to material choice, display setting, and historical use within a {style} context; for review, prioritize how it differs functionally and visually from works in the same group.")
            en_parts.append("Even when source detail is limited, you can build a comparison-ready note from the title, material, and its position in the course sequence.")

    if author and not author.endswith(" artist") and author.lower() not in {"unknown", "unknown artist"}:
        zh_parts.append(f"当前课程记录还将其与{author}关联，可在比较时同时注意作者归属与对象功能之间是否一致。")
        en_parts.append(f"The course metadata also links this entry to {author}; when comparing, check whether authorship attribution aligns with the object's function and context.")

    return "".join(zh_parts[:3]), " ".join(en_parts[:3])


def _replacement_for_missing_source_sentence(item: dict) -> tuple[str, str]:
    meta = item.get("metadata", {}) or {}
    title = (item.get("title") or "").strip()
    style = (meta.get("style") or "").strip()
    sources = meta.get("historicalBackgroundSources") or []
    title_l = title.lower()
    is_detail_like = any(k in title_l for k in ["detail", "front and back", "two views", "same image"])  # title-based
    likely_secondary = meta.get("recordType") != "artwork" or is_detail_like

    if sources:
        zh = "当前条目已记录可复核来源，但由于标题截断、细节图/多视图关系或页面抓取限制，自动流程未将其作为独立主图完成对象级核对；复习时建议与同页主图一并对照来源信息。"
        en = "This entry already includes a reviewable source, but title truncation, detail/multi-view status, or page-fetch limits prevented the automated process from treating it as a fully verified standalone primary image; review it alongside the main image on the same slide."
    elif likely_secondary:
        zh = "该条目更可能属于同页细节图、双视图或辅助图像，自动流程未单独追踪对象级来源；复习时可重点记录它与主图在局部纹样、结构或观看角度上的区别。"
        en = "This entry is likely a same-slide detail, multi-view, or supporting image, so the automated workflow did not prioritize a standalone object-level source; for review, focus on how it differs from the main image in pattern, structure, or viewpoint."
    elif "africa" in style.lower():
        zh = "当前条目的标题与课程元数据已可支持基本识别，但来源页对象标题抓取仍不稳定；复习时可先依据材质、地域与功能类型建立比较框架，并与同主题已核对作品互证。"
        en = "The title and course metadata are sufficient for basic identification, but object-page title retrieval remains unstable; for review, build a comparison framework from material, region, and function, then cross-check against verified works in the same theme."
    else:
        zh = "当前条目的课程信息已可用于复习比较，但在线来源页在对象标题抓取或匹配上仍有限制；建议先依据材质、时期与风格语境整理差异点，并在后续补充对象级来源。"
        en = "The course metadata is already usable for comparison review, but online source-page title retrieval or matching is still limited; use material, period, and style context to structure differences first, and add an object-level source later if needed."
    return zh, en


def _rewrite_missing_source_placeholder(meta: dict, item: dict) -> None:
    zh = (meta.get("historicalBackgroundZh") or "").strip()
    en = (meta.get("historicalBackgroundEn") or "").strip()
    if MISSING_SOURCE_SENTENCE_ZH not in zh and MISSING_SOURCE_SENTENCE_EN not in en:
        return
    rep_zh, rep_en = _replacement_for_missing_source_sentence(item)
    if MISSING_SOURCE_SENTENCE_ZH in zh:
        zh = zh.replace(MISSING_SOURCE_SENTENCE_ZH, rep_zh)
    if MISSING_SOURCE_SENTENCE_EN in en:
        en = re.sub(r"The current course entry still needs a stronger object-level source set for final verification in .*?\.?$", rep_en, en)
        if en == (meta.get("historicalBackgroundEn") or "").strip():
            en = en.replace(MISSING_SOURCE_SENTENCE_EN, rep_en)
    meta["historicalBackgroundZh"] = zh
    meta["historicalBackgroundEn"] = en
    meta["historicalBackground"] = f"{zh}\n{en}".strip()


def sync_artworks_json():
    data = json.loads(ARTWORKS_JSON.read_text(encoding="utf-8"))
    works = load_works()

    background_updates = 0
    verified_updates = 0

    for item in data.get("items", []):
        item_id = item.get("id")
        meta = item.setdefault("metadata", {})
        if item_id and item_id in works:
            row = works[item_id]
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

        # Replace old course-level defaults with item-level fallback text for web reading.
        if _is_generic_background(meta.get("historicalBackgroundZh", ""), meta.get("historicalBackgroundEn", "")):
            fb_zh, fb_en = _fallback_detail_background(item)
            meta["historicalBackgroundZh"] = fb_zh
            meta["historicalBackgroundEn"] = fb_en
            meta["historicalBackground"] = f"{fb_zh}\n{fb_en}".strip()

        _rewrite_missing_source_placeholder(meta, item)

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
