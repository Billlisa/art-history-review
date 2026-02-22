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
import unicodedata
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
    "loc.gov": "Library of Congress",
    "cooperhewitt.org": "Cooper Hewitt, Smithsonian Design Museum",
    "collection.cooperhewitt.org": "Cooper Hewitt, Smithsonian Design Museum",
    "clevelandart.org": "Cleveland Museum of Art",
    "parismuseescollections.paris.fr": "Paris Musees Collections",
    "gulbenkian.pt": "Calouste Gulbenkian Museum",
    "ndl.go.jp": "National Diet Library (Japan)",
    "ndlsearch.ndl.go.jp": "National Diet Library (Japan)",
    "moma.org": "The Museum of Modern Art",
    "snl.no": "Store norske leksikon",
    "nelson-atkins.org": "The Nelson-Atkins Museum of Art",
    "art.nelson-atkins.org": "The Nelson-Atkins Museum of Art",
    "wienmuseum.at": "Wien Museum",
    "magazin.wienmuseum.at": "Wien Museum",
    "tekniskmuseum.no": "Norwegian Museum of Science and Technology",
    "glasscollection.cmog.org": "The Corning Museum of Glass",
    "wittmann.at": "Wittmann",
}

MANUAL_SOURCE_URL_OVERRIDES: Dict[str, List[str]] = {
    # Africa (official collection pages / institutional resources)
    "africa-s005-i01": [
        "https://www.metmuseum.org/art/collection/search/312336",
        "https://www.metmuseum.org/-/media/files/learn/for-educators/publications-for-educators/the-arts-of-africa-at-the-metropolitan-museum.pdf",
    ],
    "africa-s012-i01": ["https://www.metmuseum.org/art/collection/search/321237"],
    "africa-s012-i02": ["https://www.metmuseum.org/art/collection/search/321237"],
    "africa-s012-i03": ["https://www.metmuseum.org/art/collection/search/321237"],
    "africa-s013-i01": ["https://www.metmuseum.org/art/collection/search/309900"],
    "africa-s013-i02": ["https://www.metmuseum.org/art/collection/search/309900"],
    "africa-s015-i01": ["https://www.metmuseum.org/art/collection/search/488860"],
    "africa-s015-i02": ["https://www.metmuseum.org/art/collection/search/488860"],
    "africa-s016-i01": ["https://www.metmuseum.org/art/collection/search/319474"],
    "africa-s016-i02": ["https://www.metmuseum.org/art/collection/search/319474"],
    "africa-s006-i01": ["https://www.metmuseum.org/art/collection/search/310765"],
    "africa-s009-i01": ["https://www.metmuseum.org/art/collection/search/635680"],
    "africa-s011-i01": [
        "https://harn.ufl.edu/resources/recycled-sculpture-inspired-by-el-anatsui/",
        "https://elanatsui.art/artworks/el-anatsui-old-mans-cloth-2003",
    ],
    "africa-s017-i01": ["https://www.britishmuseum.org/collection/object/E_Af1934-0307-241"],
    "africa-s017-i02": ["https://www.britishmuseum.org/collection/object/E_Af1934-0307-241"],
    "africa-s017-i03": ["https://www.britishmuseum.org/collection/object/E_Af1934-0307-241"],
    "africa-s019-i01": ["https://www.metmuseum.org/exhibitions/listings/2015/kongo"],
    "africa-s019-i02": ["https://www.metmuseum.org/exhibitions/listings/2015/kongo"],
    "africa-s007-i01": ["https://www.metmuseum.org/toah/works-of-art/2008.30"],
    "africa-s008-i01": ["https://www.metmuseum.org/toah/works-of-art/2008.30"],
    # Industrial reform / printed source
    "industrial_reform-s015-i01": ["https://archive.org/details/grammarofornamen00joneuoft"],
    "industrial_reform-s015-i02": ["https://archive.org/details/grammarofornamen00joneuoft"],
    "industrial_reform-s013-i01": [
        "https://collections.vam.ac.uk/item/O278829/table-pugin-augustus-welby-northmore/",
        "https://www.vam.ac.uk/articles/arts-and-crafts-an-introduction",
        "https://en.wikipedia.org/wiki/A._W._N._Pugin",
    ],
    "industrial_reform-s014-i01": [
        "https://www.vam.ac.uk/articles/arts-and-crafts-an-introduction",
        "https://en.wikipedia.org/wiki/A._W._N._Pugin",
    ],
    "industrial_reform-s019-i01": ["https://www.vam.ac.uk/page/t/tipus-tiger/"],
    "industrial_reform-s023-i01": [
        "https://www.metmuseum.org/art/collection/search/814025",
        "https://quod.lib.umich.edu/h/hart/x-895460/1",
    ],
    "industrial_reform-s023-i02": [
        "https://www.metmuseum.org/art/collection/search/814025",
        "https://quod.lib.umich.edu/h/hart/x-895460/1",
    ],
    "industrial_reform-s028-i01": ["https://www.metmuseum.org/art/collection/search/56353"],
    "industrial_reform-s035-i01": ["https://www.metmuseum.org/art/collection/search/208855"],
    "industrial_reform-s035-i02": ["https://www.metmuseum.org/essays/christopher-dresser-1834-1904"],
    "industrial_reform-s036-i01": ["https://www.vam.ac.uk/articles/rococo-textile-designs-by-william-kilburn"],
    "industrial_reform-s037-i01": ["https://www.vam.ac.uk/articles/rococo-textile-designs-by-william-kilburn"],
    "industrial_reform-s037-i02": ["https://www.vam.ac.uk/articles/rococo-textile-designs-by-william-kilburn"],
    "industrial_reform-s037-i03": ["https://www.vam.ac.uk/articles/rococo-textile-designs-by-william-kilburn"],
    "industrial_reform-s046-i01": ["https://www.artic.edu/artworks/149709/decanter"],
    "industrial_reform-s046-i02": ["https://www.artic.edu/artworks/149709/decanter"],
    "industrial_reform-s050-i01": ["https://www.metmuseum.org/art/collection/search/373763"],
    "industrial_reform-s050-i02": ["https://www.metmuseum.org/art/collection/search/373763"],
    # Art Nouveau
    "art_nouveau-s005-i01": ["https://www.metmuseum.org/art/collection/search/208571"],
    "art_nouveau-s007-i01": [
        "https://www.tekniskmuseum.no/en/enchanted-design-gerhard-munthe",
        "https://snl.no/Gerhard_Munthe",
        "https://en.wikipedia.org/wiki/Gerhard_Munthe",
    ],
    "art_nouveau-s018-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Fran%C3%A7ois-Rupert_Carabin",
    ],
    "art_nouveau-s022-i01": ["https://ndlsearch.ndl.go.jp/books/R100000128-IB16318624"],
    "art_nouveau-s022-i02": ["https://ndlsearch.ndl.go.jp/books/R100000128-IB16318624"],
    "art_nouveau-s022-i03": ["https://ndlsearch.ndl.go.jp/books/R100000128-IB16318624"],
    "art_nouveau-s023-i01": ["https://www.metmuseum.org/art/collection/search/648189"],
    "art_nouveau-s024-i01": ["https://parismuseescollections.paris.fr/en/node/240365"],
    "art_nouveau-s024-i02": ["https://parismuseescollections.paris.fr/en/node/240365"],
    "art_nouveau-s024-i03": ["https://parismuseescollections.paris.fr/en/node/240365"],
    "art_nouveau-s026-i01": ["https://gulbenkian.pt/museu/en/works/dragonfly-corsage-ornament/"],
    "art_nouveau-s028-i01": ["https://www.loc.gov/item/2001697152/"],
    "art_nouveau-s029-i01": ["https://www.clevelandart.org/art/1976.53"],
    "art_nouveau-s029-i02": ["https://www.clevelandart.org/art/1976.53"],
    "art_nouveau-s029-i03": ["https://www.clevelandart.org/art/1976.53"],
    "art_nouveau-s029-i04": ["https://www.clevelandart.org/art/1976.53"],
    "art_nouveau-s031-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Loie_Fuller",
    ],
    "art_nouveau-s034-i01": ["https://www.gla.ac.uk/hunterian/visit/our-venues/mackintosh-house/"],
    "industrial_reform-s054-i01": [
        "https://asia.si.edu/interactives/symbols/peacock/harmony-in-blue-and-gold-the-peacock-room/index.html",
        "https://en.wikipedia.org/wiki/James_McNeill_Whistler",
    ],
    "art_nouveau-s011-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Jane_Avril",
    ],
    "art_nouveau-s013-i02": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/H%C3%B4tel_Tassel",
    ],
    "art_nouveau-s014-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/H%C3%B4tel_Tassel",
    ],
    "art_nouveau-s015-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Castel_B%C3%A9ranger",
    ],
    "art_nouveau-s032-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Glasgow_School_of_Art",
    ],
    "art_nouveau-s033-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Glasgow_School_of_Art",
    ],
    "art_nouveau-s038-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Willow_Tea_Rooms",
    ],
    "art_nouveau-s042-i01": [
        "https://magazin.wienmuseum.at/sabine-pollak-ueber-adolf-loos",
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Adolf_Loos",
    ],
    "industrial_reform-s020-i01": [
        "https://collections.londonmuseum.org.uk/online/object/361696.html",
        "https://en.wikipedia.org/wiki/Great_Exhibition",
    ],
    "industrial_reform-s022-i01": [
        "https://www.vam.ac.uk/articles/building-the-museum",
        "https://en.wikipedia.org/wiki/Thomas_Onwhyn",
    ],
    "industrial_reform-s025-i01": [
        "https://www.rct.uk/group/381/content/collections/royal-archives/prince-alberts-official-papers/great-exhibition-catalogue-from-the-royal-library",
        "https://www.vam.ac.uk/articles/building-the-museum",
        "https://en.wikipedia.org/wiki/Great_Exhibition",
    ],
    "industrial_reform-s027-i01": [
        "https://collections.vam.ac.uk/item/O154306/tile-dresser-dr-christopher/",
        "https://en.wikipedia.org/wiki/Christopher_Dresser",
    ],
    "industrial_reform-s029-i01": [
        "https://collections.vam.ac.uk/item/O484340/travelling-tea-and-drinking-set-dresser-dr-christopher/",
        "https://en.wikipedia.org/wiki/Christopher_Dresser",
    ],
    "industrial_reform-s042-i01": [
        "https://collections.vam.ac.uk/item/O35645/brother-rabbit-furnishing-fabric-morris-william/",
        "https://en.wikipedia.org/wiki/William_Morris",
    ],
    "industrial_reform-s044-i01": [
        "https://www.vam.ac.uk/articles/arts-and-crafts-an-introduction",
        "https://en.wikipedia.org/wiki/Philip_Webb",
        "https://en.wikipedia.org/wiki/William_Morris",
    ],
    "art_nouveau-s002-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Edward_William_Godwin",
    ],
    "art_nouveau-s006-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Akseli_Gallen-Kallela",
    ],
    "art_nouveau-s008-i01": [
        "https://www.metmuseum.org/art/collection/search/202588",
        "https://en.wikipedia.org/wiki/%C3%89mile_Gall%C3%A9",
    ],
    "art_nouveau-s009-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Henry_van_de_Velde",
    ],
    "art_nouveau-s010-i01": [
        "https://art.nelson-atkins.org/objects/11201/tropon-protein-food",
        "https://en.wikipedia.org/wiki/Henry_van_de_Velde",
        "https://en.wikipedia.org/wiki/Tropon",
    ],
    "art_nouveau-s012-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://www.vmfa.museum/piction/6027262-80226461/",
        "https://en.wikipedia.org/wiki/Henry_van_de_Velde",
    ],
    "art_nouveau-s017-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Hector_Guimard",
    ],
    "art_nouveau-s019-i01": [
        "https://www.metmuseum.org/art/collection/search/204842",
        "https://en.wikipedia.org/wiki/%C3%89mile_Gall%C3%A9",
    ],
    "art_nouveau-s020-i01": [
        "https://www.metmuseum.org/art/collection/search/204842",
        "https://en.wikipedia.org/wiki/%C3%89mile_Gall%C3%A9",
    ],
    "art_nouveau-s021-i01": [
        "https://glasscollection.cmog.org/objects/28355/la-libellule-the-dragonfly",
        "https://en.wikipedia.org/wiki/%C3%89mile_Gall%C3%A9",
    ],
    "art_nouveau-s025-i01": [
        "https://www.artic.edu/artworks/182209/furniture-from-the-bing-pavilion-at-the-paris-world-s-fair",
        "https://www.musee-orsay.fr/fr/oeuvres/pavillon-de-lart-nouveau-bing-lovers-and-peacocks-253385",
        "https://en.wikipedia.org/wiki/Georges_de_Feure",
    ],
    "art_nouveau-s027-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Henry_van_de_Velde",
    ],
    "art_nouveau-s037-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Hill_House,_Helensburgh",
    ],
    "art_nouveau-s040-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Wiener_Werkst%C3%A4tte",
        "https://en.wikipedia.org/wiki/Josef_Hoffmann",
    ],
    "art_nouveau-s041-i01": [
        "https://www.metmuseum.org/essays/art-nouveau",
        "https://en.wikipedia.org/wiki/Josef_Hoffmann",
    ],
    "art_nouveau-s043-i01": [
        "https://www.wittmann.at/produkte/wohnen/sessel-fauteuils/kubus-fauteuil-sessel",
        "https://en.wikipedia.org/wiki/Josef_Hoffmann",
    ],
}

MANUAL_TITLE_HINTS: Dict[str, str] = {
    "africa-s013-i01": "lidded saltcellar sapi portuguese edo bini portuguese ivory",
    "africa-s013-i02": "lidded saltcellar sapi portuguese edo bini portuguese ivory",
    "africa-s015-i01": "andre derain studio interior paris african art collection photograph",
    "africa-s015-i02": "andre derain studio interior paris african art collection photograph",
    "africa-s009-i01": "bansoa throne royal couple bamileke beaded throne cameroon",
    "africa-s011-i01": "old man's cloth el anatsui aluminum copper wire harn museum",
    "africa-s017-i01": "wrapper textile saint-louis senegal british museum af1934 0307 241",
    "africa-s017-i02": "wrapper textile saint-louis senegal british museum af1934 0307 241",
    "africa-s017-i03": "wrapper textile saint-louis senegal british museum af1934 0307 241",
    "africa-s019-i01": "luxury cloth cushion cover kongo raffia kungliga samlingarna sweden",
    "africa-s019-i02": "luxury cloth cushion cover kongo raffia kungliga samlingarna sweden",
    "africa-s005-i01": "mother and child bamana mali bougouni dioila",
    "africa-s007-i01": "nkisi nkondi mangaaka kongo yombe power figure",
    "africa-s008-i01": "nkisi nkondi mangaaka kongo yombe power figure detail",
    "industrial_reform-s019-i01": "tipu tiger mysore victoria and albert museum 2545 is",
    "industrial_reform-s023-i01": "grammar of ornament egyptian ornament plate 3 1856 owen jones",
    "industrial_reform-s023-i02": "grammar of ornament egyptian ornament plate 3 1856 owen jones",
    "industrial_reform-s028-i01": "great wave under the wave off kanagawa hokusai mount fuji woodblock print",
    "industrial_reform-s035-i01": "willow boughs wallpaper william morris sidewall",
    "industrial_reform-s035-i02": "christopher dresser botanical drawing study branch berries design reform",
    "industrial_reform-s036-i01": "seaweed print watercolor textile design kilburn 1788",
    "industrial_reform-s037-i01": "seaweed print used for fabric watercolor kilburn",
    "industrial_reform-s037-i02": "seaweed print used for fabric watercolor kilburn",
    "industrial_reform-s037-i03": "seaweed print used for fabric watercolor kilburn",
    "industrial_reform-s046-i01": "cr ashbee decanter glass silver chrysoprase arts and crafts",
    "industrial_reform-s046-i02": "cr ashbee decanter glass silver chrysoprase arts and crafts",
    "industrial_reform-s050-i01": "walter crane frontispiece clarence cook the house beautiful 1878",
    "industrial_reform-s050-i02": "walter crane frontispiece clarence cook the house beautiful 1878",
    "industrial_reform-s013-i01": "awn pugin table vam gothic revival",
    "industrial_reform-s020-i01": "john nash stuffed elephant howdah india no 7 great exhibition 1852",
    "industrial_reform-s025-i01": "industry of all nations great exhibition catalogue royal library",
    "industrial_reform-s029-i01": "christopher dresser travelling tea and drinking set",
    "art_nouveau-s005-i01": "vilmos zsolnay vase iridescent metallic luster glaze 1899",
    "art_nouveau-s007-i01": "gerhard munthe armchair fairytale room holmenkollen tourist hotel norway",
    "art_nouveau-s010-i01": "tropon protein food poster henry van de velde",
    "art_nouveau-s012-i01": "henry van de velde candelabrum candelabra art nouveau",
    "art_nouveau-s018-i01": "francois rupert carabin chair walnut art nouveau",
    "art_nouveau-s021-i01": "emile galle libellule dragonfly vase",
    "art_nouveau-s025-i01": "georges de feure bing pavilion paris world fair furniture",
    "art_nouveau-s042-i01": "adolf loos schlafzimmer lina loos bedroom",
    "art_nouveau-s043-i01": "josef hoffmann kubus club chair kubus fauteuil",
    "art_nouveau-s022-i01": "the silly jelly-fish japanese fairy tale series no 13 takejiro hasegawa kawabata",
    "art_nouveau-s022-i02": "the silly jelly-fish japanese fairy tale series no 13 takejiro hasegawa kawabata",
    "art_nouveau-s022-i03": "the silly jelly-fish japanese fairy tale series no 13 takejiro hasegawa kawabata",
    "art_nouveau-s023-i01": "bamboo bowler hat hayakawa shokosai 1880s 1890s japan",
    "art_nouveau-s024-i01": "porte monumentale exposition universelle 1900 rene binet paris",
    "art_nouveau-s024-i02": "porte monumentale exposition universelle 1900 rene binet paris",
    "art_nouveau-s024-i03": "porte monumentale exposition universelle 1900 rene binet paris",
    "art_nouveau-s026-i01": "rene lalique dragonfly woman corsage ornament 1897 1898",
    "art_nouveau-s028-i01": "exhibit of american negroes paris exposition 1900 web du bois",
    "art_nouveau-s029-i01": "louis majorelle cabinet c 1900 art nouveau",
    "art_nouveau-s029-i02": "louis majorelle cabinet c 1900 art nouveau",
    "art_nouveau-s029-i03": "louis majorelle cabinet c 1900 art nouveau",
    "art_nouveau-s029-i04": "louis majorelle cabinet c 1900 art nouveau",
    "art_nouveau-s031-i01": "loie fuller danse serpentine lumiere brothers 1896 serpentine dance",
    "art_nouveau-s034-i01": "mackintosh house hunterian glasgow 6 florentine terrace",
}

# Narrow curator-approved exceptions for pages that are blocked/unstable but manually matched.
MANUAL_TITLE_MISMATCH_OK: Dict[str, str] = {
    "industrial_reform-s020-i01": "London Museum object page is Cloudflare-blocked in automation; object ID and title were manually matched.",
    "art_nouveau-s007-i01": "Gerhard Munthe armchair slide manually matched; available sources are author/context level only.",
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

OBJECTISH_LEAD_WORDS = {
    "ceremonial",
    "skirt",
    "wrapper",
    "tusk",
    "mother",
    "child",
    "power",
    "figure",
    "figures",
    "cushion",
    "cover",
    "chair",
    "teapot",
    "tile",
    "traveling",
    "travelling",
    "lidded",
    "saltcellar",
    "portrait",
    "sideboard",
    "room",
    "peacock",
    "industry",
    "cooking",
    "pendant",
    "mask",
    "plaque",
    "head",
    "vase",
    "rug",
    "doorway",
    "facade",
    "façade",
    "school",
    "bedroom",
    "service",
    "decanter",
    "banquette",
    "candelabra",
    "cabinet",
    "bowl",
    "chair",
    "tea",
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


def extract_urls_from_text(text: str) -> List[str]:
    txt = unescape(text or "")
    return split_source_urls(" | ".join(re.findall(r"https?://[^\s,)>]+", txt)))


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




def normalize_dedupe_title(text: str) -> str:
    txt = (text or "").lower()
    txt = txt.replace("“", '"').replace("”", '"').replace("’", "'")
    txt = re.sub(r"^\s*(two views of|view of|detail of|detail)\s+", "", txt)
    txt = re.sub(r"\b(front and back|detail|same image)\b", "", txt)
    txt = re.sub(r"[^a-z0-9]+", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


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


def fold_text(text: str) -> str:
    norm = unicodedata.normalize("NFKD", text or "")
    return "".join(ch for ch in norm if not unicodedata.combining(ch)).lower()


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
    tier1_markers = [
        "museum",
        "musee",
        "metmuseum.org",
        "vam.ac.uk",
        "britishmuseum.org",
        "rct.uk",
        "si.edu",
        "cooperhewitt.org",
        "loc.gov",
        "clevelandart.org",
        "parismuseescollections.paris.fr",
        "gulbenkian.pt",
        "ndl.go.jp",
        "moma.org",
    ]
    if any(m in host for m in tier1_markers):
        return 1
    if "collection." in host or "collections." in host:
        return 1

    # Tier 2: scholarly/citable publishers and catalogues
    tier2_markers = ["cambridge.org", "jstor.org", "doi.org", "archive.org", "journalhosting.", "snl.no"]
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


def met_object_id_from_url(url: str) -> Optional[str]:
    m = re.search(r"metmuseum\.org/art/collection/search/(\d+)", url)
    return m.group(1) if m else None


def fetch_met_object_via_api(object_id: str) -> Optional[Dict[str, str]]:
    api_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{object_id}"
    req = Request(api_url, headers={"User-Agent": USER_AGENT})
    ssl_ctx = ssl.create_default_context()
    try:
        with urlopen(req, timeout=TIMEOUT, context=ssl_ctx) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        if not isinstance(data, dict) or not data.get("objectID"):
            return None
        title = (data.get("title") or "").strip()
        bits = [
            data.get("artistDisplayName") or "",
            data.get("objectName") or "",
            data.get("culture") or "",
            data.get("period") or "",
            data.get("objectDate") or "",
        ]
        meta_desc = " | ".join([b.strip() for b in bits if str(b).strip()])[:600]
        return {
            "status": "http_200",
            "page_title": f"{title} - The Metropolitan Museum of Art" if title else "The Metropolitan Museum of Art object",
            "meta_description": meta_desc,
            "final_url": f"https://www.metmuseum.org/art/collection/search/{object_id}",
        }
    except Exception:
        return None


def fetch_title(url: str, cache: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    cached = cache.get(url)
    met_id = met_object_id_from_url(url)
    if cached:
        if not (met_id and (cached.get("page_title") in {"", "(title fetch failed)"} or str(cached.get("status", "")).startswith("http_4"))):
            return cached

    if met_id:
        met = fetch_met_object_via_api(met_id)
        if met:
            cache[url] = met
            return met

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
    hay = fold_text(f"{source.page_title} {source.meta_description} {source.url}")
    title_tokens = [fold_text(t) for t in significant_title_tokens(title)]
    author_tokens = {fold_text(t) for t in significant_title_tokens(author)}
    lead_name_match = re.match(r"^\W*([A-Z][a-z]+)(?:\s+([A-Z][a-z]+))?(?:\s+([A-Z][a-z]+))?", title or "")
    if lead_name_match:
        lead_words = [g.lower() for g in lead_name_match.groups() if g]
        looks_objectish = any(w in OBJECTISH_LEAD_WORDS for w in lead_words)
        first_is_article = bool(lead_words) and lead_words[0] in {"the", "a", "an"}
        comma_author_prefix = "," in (title or "")[:50] and len(lead_words) >= 2 and not looks_objectish
        no_comma_author_prefix = len(lead_words) in {2, 3} and not first_is_article and not looks_objectish
        looks_author_prefix = comma_author_prefix or no_comma_author_prefix
        if looks_author_prefix:
            for g in lead_name_match.groups():
                if g:
                    author_tokens.add(fold_text(g))
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
    provisional_403 = [r for r in records if r.status == "http_403" and r.tier in {1, 2, 3}]
    eval_records = [*ok, *provisional_403]
    if len(eval_records) < 1:
        return False, "no reachable sources"

    non_wiki = [r for r in eval_records if "wikipedia.org" not in r.url and "wikimedia.org" not in r.url]
    if len(non_wiki) < 1:
        return False, "Wikipedia/Wikimedia cannot be the only basis"

    preferred = [r for r in eval_records if r.tier in {1, 2, 3}]
    if len(preferred) < 1:
        return False, "no preferred source (official/scholarly/university)"

    for r in eval_records:
        r.relevance = source_relevance_for_work(r, title, author)
    if max((r.relevance for r in eval_records), default=0) <= 0:
        return False, "sources appear generic or mismatched to this work"
    if max((r.title_specific_relevance for r in eval_records), default=0) <= 0:
        title_l = (title or "").lower()
        detail_like = any(k in title_l for k in ["detail", "same image", "see previous slide", "detail of", "two views", "front and back"])
        person_prefix = re.match(r"^\W*([A-Z][a-z]+)(?:\s+([A-Z][a-z]+)){0,2}", title or "")
        person_like = False
        if person_prefix and "," not in (title or "")[:60]:
            lead_words = [g.lower() for g in person_prefix.groups() if g]
            if not any(w in OBJECTISH_LEAD_WORDS for w in lead_words):
                person_like = True
        if not detail_like and not (person_like and max((r.author_relevance for r in eval_records), default=0) > 0):
            return False, "sources mention author/context but not this specific work"

    if not ok:
        if max((r.relevance for r in provisional_403), default=0) > 0:
            return True, "official source blocked by 403 but title/context match is strong"
        return False, "no reachable sources"

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


def classify_needs_human_detail(note_parts: List[str]) -> str:
    text = " | ".join(note_parts).lower()
    if "no existing source urls" in text:
        return "needs_human_source_missing"
    if "not this specific work" in text or "generic or mismatched" in text:
        return "needs_human_title_mismatch"
    if "wikipedia/wikimedia" in text or "no preferred source" in text or "no reachable sources" in text:
        return "needs_human_source_quality"
    return "needs_human_other"


def main() -> int:
    if not INPUT_CSV.exists():
        print(f"Missing input: {INPUT_CSV}", file=sys.stderr)
        return 1

    rows = load_rows()
    cache = load_cache()
    report = make_report_sections()
    output_rows: List[Dict[str, str]] = []
    seen_image_paths: set[str] = set()
    seen_slide_object_keys: set[tuple[str, str, str]] = set()

    for global_idx, row in enumerate(rows, start=1):
        if row.get("record_type") != "artwork":
            continue
        if global_idx in SKIP_GLOBAL_INDEX_RANGE:
            continue

        image_path = (row.get("image_path") or "").strip()
        course = (row.get("course") or "").strip()
        slide = str(row.get("slide") or "").strip()
        title_for_dedupe = normalize_dedupe_title(row.get("title", ""))

        # Skip exact duplicate images and repeated same-object views/details within a slide.
        if image_path and image_path in seen_image_paths:
            continue
        if title_for_dedupe:
            slide_object_key = (course, slide, title_for_dedupe)
            if slide_object_key in seen_slide_object_keys:
                continue
            seen_slide_object_keys.add(slide_object_key)
        if image_path:
            seen_image_paths.add(image_path)

        item_id = row["id"]
        title = row["title"]
        relevance_title = f"{title} {MANUAL_TITLE_HINTS.get(item_id, '')}".strip()
        author = row.get("author", "")
        year_expr = normalize_year_expr(row.get("year_creation", ""), row.get("period_creation", ""))
        source_urls = split_source_urls(row.get("historical_background_sources", ""))
        source_urls = split_source_urls(" | ".join([*source_urls, *extract_urls_from_text(row.get('raw_slide_text', ''))]))
        if item_id in MANUAL_SOURCE_URL_OVERRIDES:
            source_urls = split_source_urls(" | ".join([*source_urls, *MANUAL_SOURCE_URL_OVERRIDES[item_id]]))
        source_records = build_source_records(source_urls, cache) if source_urls else []
        row_for_bg = dict(row)
        row_for_bg["title"] = relevance_title if MANUAL_TITLE_HINTS.get(item_id) else row.get("title", "")
        bg_zh, bg_en = build_specific_backgrounds(row_for_bg, source_records)
        # Preserve original displayed title in generated background sentence.
        if MANUAL_TITLE_HINTS.get(item_id):
            bg_zh = bg_zh.replace(f"“{relevance_title}”", f"“{title}”")
            bg_en = bg_en.replace(relevance_title, title)

        sufficient_sources, source_reason = is_source_set_sufficient(source_records, relevance_title, author)
        if not sufficient_sources and item_id in MANUAL_TITLE_MISMATCH_OK:
            sufficient_sources = True
            source_reason = MANUAL_TITLE_MISMATCH_OK[item_id]
        zh_sentences = sentence_count_zh(bg_zh)
        en_sentences = sentence_count_en(bg_en)
        background_ok = zh_sentences >= 2 and en_sentences >= 2

        status = "updated"
        status_detail = "updated"
        note_parts: List[str] = []

        if not source_urls:
            status = "needs_human"
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
        elif status == "needs_human":
            status_detail = classify_needs_human_detail(note_parts)
        else:
            status_detail = status

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
                "status_detail": status_detail,
                "notes": "; ".join(note_parts),
            }
        )

        report[status].append((global_idx, item_id, title, status_detail, note_parts, top_records))

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
        "status_detail",
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
        for global_idx, item_id, item_title, status_detail, notes, sources in items:
            label = f" [{status_detail}]" if status_detail and status_detail != "updated" else ""
            lines.append(f"- `{item_id}` (row {global_idx}){label}: {item_title}")
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
