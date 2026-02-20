#!/usr/bin/env python3
"""Build study dataset from PPTX files.

Outputs:
- app/assets/* image files extracted from decks
- app/data/artworks.json metadata for frontend app
"""

import csv
import json
import posixpath
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Sequence, Tuple
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
ASSETS_DIR = APP_DIR / "assets"
DATA_DIR = APP_DIR / "data"

DECKS = [
    {
        "id": "africa",
        "title": "Arts of Africa (Met Visit)",
        "source": Path("/Users/xiongruihan/Downloads/Arts of Africa Met Visit with Dr. Alisa LaGamma REQUIRED SLIDES 2026.pptx"),
        "default_region": "Sub-Saharan Africa",
        "default_style": "African Art",
        "default_background_en": "Often linked to ritual authority, court culture, social memory, and later museum collection histories in African contexts.",
        "default_background_zh": "常与非洲语境中的仪式权力、宫廷文化、社会记忆，以及后期博物馆收藏史相关。",
        "tags": ["Week Africa", "Met Museum"],
    },
    {
        "id": "art_nouveau",
        "title": "Week 5: Art Nouveau",
        "source": Path("/Users/xiongruihan/Downloads/Topics Art Nouveau Week 5 Required Slides 2026.pptx"),
        "default_region": "Europe",
        "default_style": "Art Nouveau",
        "default_background_en": "Part of late-19th-century reform movements that rejected rigid historicism and rethought ornament, craft, and modern life.",
        "default_background_zh": "属于19世纪末设计改革思潮的一部分，反对僵化历史主义，并重新思考装饰、工艺与现代生活。",
        "tags": ["Week 5", "Topics & Topicality"],
    },
    {
        "id": "industrial_reform",
        "title": "Week 4: Industrial Revolution & Design Reform",
        "source": Path("/Users/xiongruihan/Downloads/Topics Week 4 Required Slides Industrial Revolution  Crystal Palace Design Reform 2-12-26.pptx"),
        "default_region": "Britain / Europe",
        "default_style": "Design Reform / Industrial Era",
        "default_background_en": "Connected to industrialization, the Great Exhibition, and Victorian debates about design quality, ornament, and mass manufacture.",
        "default_background_zh": "与工业化、大博览会，以及维多利亚时期关于设计质量、装饰与机械化生产的争论相关。",
        "tags": ["Week 4", "Crystal Palace"],
    },
]

NS = {
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pr": "http://schemas.openxmlformats.org/package/2006/relationships",
}

SUPPORTED_IMAGE_EXT = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tif", ".tiff"}

REGION_KEYWORDS = {
    "Africa": ["africa", "mali", "cameroon", "dogon", "bamana", "yoruba", "benin", "kongo", "congo", "ghana", "edo"],
    "France": ["france", "paris", "napoleon", "sevres", "fontainebleau", "malmaison"],
    "Britain": ["britain", "british", "england", "london", "victorian", "crystal palace", "westminster"],
    "Belgium": ["belgium", "brussels", "horta", "hankar"],
    "Spain": ["spain", "barcelona", "gaudi", "catalan"],
    "United States": ["american", "brooklyn", "new york", "u.s.", "united states", "virginia"],
    "Egypt": ["egypt", "egyptian", "nile", "karnak", "thebes", "edfu"],
    "Italy": ["italy", "rome", "pompeii", "nimes", "piranesi"],
    "Germany": ["german", "germany", "berlin", "munich"],
    "Austria": ["austria", "vienna", "wien"],
    "Finland": ["finnish", "finland"],
    "Norway": ["norway", "norwegian"],
}

PLACE_KEYWORDS = [
    ("Mali", ["mali"]),
    ("Cameroon", ["cameroon"]),
    ("Nigeria", ["nigeria"]),
    ("Democratic Republic of the Congo", ["democratic republic of the congo", "dr congo"]),
    ("Republic of the Congo", ["republic of the congo"]),
    ("Angola", ["angola", "cabinda"]),
    ("Benin", ["court of benin", "benin"]),
    ("Loango Coast", ["loango coast", "chiloango"]),
    ("Paris", ["paris"]),
    ("France", ["france", "sevres", "fontainebleau", "malmaison"]),
    ("London", ["london", "westminster"]),
    ("Britain", ["britain", "british", "england", "stoke-on-trent"]),
    ("Brussels", ["brussels"]),
    ("Belgium", ["belgium"]),
    ("Barcelona", ["barcelona"]),
    ("Spain", ["spain"]),
    ("Berlin", ["berlin"]),
    ("Germany", ["germany", "german"]),
    ("Vienna", ["vienna", "wien"]),
    ("Austria", ["austria"]),
    ("Finland", ["finnish", "finland"]),
    ("Norway", ["norway", "norwegian"]),
    ("Egypt", ["egypt", "edfu", "karnak", "thebes"]),
    ("Rome", ["rome"]),
    ("Italy", ["italy", "pompeii", "nimes", "piranesi"]),
    ("United States", ["new york", "brooklyn", "united states", "u.s.", "virginia"]),
]

STYLE_KEYWORDS = {
    "Art Nouveau": ["art nouveau", "horta", "guimard", "galle", "mucha", "toulouse-lautrec", "mackmurdo", "whiplash", "van de velde"],
    "Industrial Design": ["industrial", "machine", "mass production", "exhibition", "factory"],
    "Design Reform": ["design reform", "pugin", "owen jones", "william morris", "cole", "dresser", "ruskin"],
    "Neoclassicism / Empire": ["empire", "napoleonic", "ingres", "david", "percier", "fontaine", "malmaison"],
    "African Art": ["dogon", "mask", "bamana", "african", "power figure", "nkisi", "met"],
    "Orientalism / Egyptomania": ["egypt", "egyptian", "rosette", "sphinx", "denon"],
}

MATERIAL_PATTERNS: Sequence[Tuple[str, str]] = [
    (r"hard-paste porcelain", "Hard-paste porcelain"),
    (r"soft-paste porcelain", "Soft-paste porcelain"),
    (r"oil on canvas", "Oil on canvas"),
    (r"color lithograph|colour lithograph|lithograph", "Lithograph"),
    (r"woodcut", "Woodcut"),
    (r"watercolor|watercolour", "Watercolor"),
    (r"engraving", "Engraving"),
    (r"earthenware", "Earthenware"),
    (r"porcelain", "Porcelain"),
    (r"electroplated bronze", "Electroplated bronze"),
    (r"silver[- ]gilt|silver gilt", "Silver-gilt"),
    (r"bronze", "Bronze"),
    (r"aluminum|aluminium", "Aluminum"),
    (r"copper wire", "Copper wire"),
    (r"copper", "Copper"),
    (r"iron alloy", "Iron alloy"),
    (r"iron", "Iron"),
    (r"ivory", "Ivory"),
    (r"mahogany", "Mahogany"),
    (r"beech", "Beech"),
    (r"pine", "Pine"),
    (r"wood", "Wood"),
    (r"glass beads", "Glass beads"),
    (r"cowrie shells", "Cowrie shells"),
    (r"cloth", "Cloth/Textile"),
    (r"wool", "Wool"),
    (r"leather", "Leather"),
    (r"paper", "Paper"),
    (r"resin", "Resin"),
    (r"paint", "Paint"),
]

AUTHOR_STOPWORDS = {
    "topics",
    "topicality",
    "week",
    "required",
    "slide",
    "detail",
    "left",
    "right",
    "stars",
    "lecture",
    "credit",
    "card",
    "just",
    "background",
    "comparison",
    "metropolitan",
    "museum",
    "parsons",
    "cooper",
    "hewitt",
    "curator",
    "michael",
    "rockefeller",
    "foundation",
    "fund",
    "purchase",
    "collection",
    "gift",
    "service",
    "wing",
    "wallace",
    "rogers",
    "barnes",
    "british",
    "museum",
    "met",
    "metropolitan",
    "master",
    "anonymous",
    "gifts",
    "archives",
    "co",
    "abbey",
}

AUTHOR_OBJECT_WORDS = {
    "chair",
    "armchair",
    "settee",
    "sideboard",
    "vase",
    "bowl",
    "candelabra",
    "mask",
    "figure",
    "throne",
    "couple",
    "mother",
    "child",
    "service",
    "room",
    "interior",
    "wallpaper",
    "design",
    "portrait",
    "painting",
    "poster",
    "lithograph",
    "map",
    "detail",
    "knife",
}

AUTHOR_CONTEXT_HINTS = re.compile(
    r"\b(oil|wood|bronze|lithograph|porcelain|earthenware|chair|vase|mask|figure|throne|designed|painting|service|bowl|textile|cloth)\b",
    re.IGNORECASE,
)

NON_PERSON_PREFIXES = {
    "dogon",
    "bamana",
    "kongo",
    "edo",
    "bamileke",
    "yoruba",
    "egyptian",
    "british",
    "french",
    "archives",
    "anonymous",
    "collection",
    "gift",
    "gifts",
}

YEAR_CONTEXT_HINTS = re.compile(
    r"\b(c(?:a)?\.?|century|oil|wood|bronze|lithograph|porcelain|earthenware|chair|vase|mask|figure|throne|textile|cloth|dated|designed)\b",
    re.IGNORECASE,
)

WEB_SOURCES = {
    "met_art_nouveau": "https://www.metmuseum.org/essays/art-nouveau",
    "va_building_museum": "https://www.vam.ac.uk/articles/building-the-museum",
    "va_arts_and_crafts": "https://www.vam.ac.uk/articles/arts-and-crafts-an-introduction",
    "britannica_pugin": "https://www.britannica.com/biography/A-W-N-Pugin",
    "met_nkisi": "https://www.metmuseum.org/art/collection/search/321109",
    "met_dogon_couple": "https://www.metmuseum.org/art/collection/search/310325",
    "met_iyoba_mask": "https://www.metmuseum.org/art/collection/search/318622",
    "bm_iyoba_mask": "https://www.britishmuseum.org/collection/object/E_Af1910-0513-1",
    "met_dresser_teapot": "https://www.metmuseum.org/art/collection/search/823191",
    "met_dresser_kettle": "https://www.metmuseum.org/art/collection/search/692623",
    "huntington_mackmurdo_chair": "https://huntington.emuseum.com/objects/3614/chair",
    "rcin_fourdinois_sideboard": "https://albert.rct.uk/collections/photographs-collection/exhibitions-and-records-of-works-of-art/the-great-exhibition-1851-sideboard-of-carved-walnut-by-fourdinois",
    "met_dresser_wall_covering": "https://www.metmuseum.org/art/collection/search/641770",
    "smithsonian_peacock_room_object": "https://asia.si.edu/interactives/symbols/peacock/harmony-in-blue-and-gold-the-peacock-room/index.html",
}

WEB_ENRICHMENT_OVERRIDES = [
    {
        "match_any": ["peacock room", "frederick r. leyland", "thomas jeckyll", "james mcneill whistler"],
        "set": {
            "year": "1876-1877",
            "period": "Late 19th century",
            "author": "James McNeill Whistler / Thomas Jeckyll",
            "style": "Aesthetic Movement / Anglo-Japanese",
            "historicalBackgroundZh": "孔雀屋由托马斯·杰基尔最初设计，后由惠斯勒在1876-1877年重绘为“蓝金和谐”，体现审美主义、赞助关系与东方主义展示政治的交织。",
            "historicalBackgroundEn": "The Peacock Room began as Thomas Jeckyll's design and was transformed by Whistler in 1876-1877 into a 'harmony in blue and gold,' exemplifying Aesthetic movement ideals, patronage tensions, and orientalizing display culture.",
        },
        "sources": [WEB_SOURCES["smithsonian_peacock_room_object"]],
    },
    {
        "match_any": ["fourdinois sideboard"],
        "set": {
            "year": "1851",
            "period": "Mid-19th century",
            "author": "Alexandre-Georges Fourdinois",
            "style": "High Victorian Historicism / Exhibition Furniture",
            "historicalBackgroundZh": "该侧柜与1851年伦敦大博览会语境相关，体现法国高端木作与国际博览会中“工业与工艺”展示机制的结合。",
            "historicalBackgroundEn": "This sideboard is tied to the 1851 Great Exhibition in London and reflects how elite French cabinetmaking was positioned within international exhibition systems that staged industry alongside craft prestige.",
        },
        "sources": [WEB_SOURCES["rcin_fourdinois_sideboard"]],
    },
    {
        "match_any": ["linoleum design possibly by christopher dresser", "linoleum design"],
        "match_all": ["dresser"],
        "set": {
            "year": "1878-1882",
            "period": "Late 19th century",
            "author": "Attributed to Christopher Dresser",
            "style": "Design Reform / Aesthetic Movement",
            "historicalBackgroundZh": "该条目与19世纪后期压花墙面材料与可复制室内装饰工业有关，常被置于德雷瑟及设计改革语境下讨论。",
            "historicalBackgroundEn": "This entry relates to late nineteenth-century embossed wall-covering technologies and reproducible interior ornament, frequently discussed within Dresser's broader design-reform context.",
        },
        "sources": [WEB_SOURCES["met_dresser_wall_covering"]],
    },
]

MANUAL_ITEM_OVERRIDES = {
    "industrial_reform-s002-i01": {
        "title": "A.W.N. Pugin (portrait reference)",
        "description": "Reference portrait of Augustus Welby Northmore Pugin, shown as one of the key figures in Gothic Revival and design reform debates.",
        "metadata": {
            "year": "",
            "period": "19th century (Victorian design reform context)",
            "author": "Britain artist",
            "productionPlace": "Britain",
            "region": "Britain",
            "style": "Gothic Revival context portrait",
            "material": "Engraving / print reproduction",
            "recordType": "reference",
            "historicalBackgroundZh": "该肖像用于介绍普金在设计改革中的理论地位：他强调哥特建筑的结构真实性、功能逻辑与宗教伦理的一致性，对后续维多利亚时代设计话语影响深远。",
            "historicalBackgroundEn": "This portrait is used to introduce Pugin's theoretical role in design reform: he argued for coherence among Gothic structure, function, and religious ethics, strongly shaping later Victorian design discourse.",
        },
        "sources": [WEB_SOURCES["britannica_pugin"]],
    },
    "industrial_reform-s002-i02": {
        "title": "Owen Jones (portrait reference)",
        "description": "Reference portrait of Owen Jones, an important nineteenth-century architect-designer and ornament theorist.",
        "metadata": {
            "year": "",
            "period": "19th century (Victorian design theory context)",
            "author": "Britain artist",
            "productionPlace": "Britain",
            "region": "Britain",
            "style": "Design Reform portrait reference",
            "material": "Engraving / print reproduction",
            "recordType": "reference",
            "historicalBackgroundZh": "该肖像用于说明欧文·琼斯在19世纪装饰理论中的核心作用；其《装饰语法》把历史装饰与色彩原则系统化，深刻影响设计教育与工业装饰实践。",
            "historicalBackgroundEn": "This portrait frames Owen Jones's central role in nineteenth-century ornament theory; The Grammar of Ornament systematized historical motifs and color principles, influencing design education and industrial decoration.",
        },
        "sources": ["https://www.britannica.com/biography/Owen-Jones"],
    },
    "industrial_reform-s002-i03": {
        "title": "Henry Cole (portrait reference)",
        "description": "Reference portrait of Sir Henry Cole, a leading organizer of the Great Exhibition and design-reform institutions.",
        "metadata": {
            "year": "",
            "period": "19th century (Victorian design reform context)",
            "author": "Britain artist",
            "productionPlace": "Britain",
            "region": "Britain",
            "style": "Victorian reform portrait reference",
            "material": "Engraving / print reproduction",
            "recordType": "reference",
            "historicalBackgroundZh": "该肖像对应亨利·科尔在1851年大博览会及南肯辛顿体系中的制度性作用：通过展览、博物馆与教育联动，推动“良好设计”进入公众消费与工业生产。",
            "historicalBackgroundEn": "This portrait corresponds to Henry Cole's institutional role in the 1851 Exhibition and the South Kensington system, linking exhibitions, museums, and education to move 'good design' into public consumption and industry.",
        },
        "sources": ["https://www.npg.org.uk/collections/search/person/mp00959/sir-henry-cole"],
    },
    "industrial_reform-s002-i04": {
        "title": "Christopher Dresser (portrait reference)",
        "description": "Reference portrait of Christopher Dresser, frequently described as a pioneering professional designer for industrial production.",
        "metadata": {
            "year": "",
            "period": "19th century (Design Reform and Aesthetic Movement context)",
            "author": "Britain artist",
            "productionPlace": "Britain",
            "region": "Britain",
            "style": "Design Reform portrait reference",
            "material": "Photographic / print portrait reproduction",
            "recordType": "reference",
            "historicalBackgroundZh": "该肖像用于引入德雷瑟的跨媒介设计实践：他把植物学研究、几何抽象与工业可制造性结合，推动了现代职业设计师角色的形成。",
            "historicalBackgroundEn": "This portrait introduces Dresser's cross-media design practice: he combined botanical study, geometric abstraction, and manufacturability, helping define the modern professional designer.",
        },
        "sources": ["https://www.britannica.com/biography/Christopher-Dresser"],
    },
    "industrial_reform-s002-i05": {
        "title": "Queen Victoria and Prince Albert (reference photograph)",
        "description": "Reference royal photograph used to frame the political and court context around the Great Exhibition era.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Victorian court context)",
            "author": "Britain artist",
            "productionPlace": "Britain",
            "region": "Britain",
            "style": "Victorian court photography reference",
            "material": "Photographic print reproduction",
            "recordType": "reference",
            "historicalBackgroundZh": "该图用于补充维多利亚王室语境。阿尔伯特亲王与王室赞助对1851年博览会及其“国家文明展示”叙事具有关键意义。",
            "historicalBackgroundEn": "This image provides royal context: Prince Albert and court patronage were central to the 1851 Exhibition and its narrative of national civilizational display.",
        },
        "sources": ["https://www.rct.uk/collection/themes/trails/queen-victorias-family/queen-victoria-and-prince-albert"],
    },
    "industrial_reform-s002-i06": {
        "title": "William Morris (portrait reference)",
        "description": "Reference portrait of William Morris, frequently used in teaching Arts and Crafts critiques of industrial modernity.",
        "metadata": {
            "year": "",
            "period": "Late 19th century (Arts and Crafts context)",
            "author": "Frederick Hollyer",
            "productionPlace": "London",
            "region": "Britain",
            "style": "Arts and Crafts portrait reference",
            "material": "Photographic portrait reproduction",
            "recordType": "reference",
            "historicalBackgroundZh": "该肖像常用于说明莫里斯对机械化生产美学的批评，以及工艺美术运动关于劳动尊严、材料诚实性与社会改革的主张。",
            "historicalBackgroundEn": "This portrait is often used to discuss Morris's critique of mechanized production and Arts and Crafts claims about dignified labor, material honesty, and social reform.",
        },
        "sources": ["https://www.npg.org.uk/collections/search/portrait/mw72045"],
    },
    "industrial_reform-s002-i07": {
        "title": "The British Workman (masthead reference)",
        "description": "Reference masthead from The British Workman, used for Victorian labor education and moral-reform print culture context.",
        "metadata": {
            "year": "",
            "period": "Mid to late 19th century (Victorian periodical culture)",
            "author": "S. W. Partridge & Co.",
            "productionPlace": "London",
            "region": "Britain",
            "style": "Victorian illustrated periodical graphic",
            "material": "Printed masthead reproduction",
            "recordType": "reference",
            "historicalBackgroundZh": "该刊头图像反映维多利亚时期面向工人阶层的宗教与道德改良出版实践，体现大众印刷媒介如何参与社会纪律与价值传播。",
            "historicalBackgroundEn": "This masthead reflects Victorian religious and moral reform publishing for working-class readers, showing how mass print participated in social discipline and value transmission.",
        },
        "sources": ["https://en.wikipedia.org/wiki/The_British_Workman"],
    },
    "industrial_reform-s002-i08": {
        "title": "Oscar Wilde, photographed by Napoleon Sarony (reference)",
        "description": "Reference portrait of Oscar Wilde associated with the transatlantic public image of Aestheticism.",
        "metadata": {
            "year": "",
            "period": "Late 19th century (Aesthetic Movement context)",
            "author": "Napoleon Sarony",
            "productionPlace": "New York",
            "region": "Britain / United States",
            "style": "Aesthetic Movement portrait photography",
            "material": "Albumen silver print (photograph)",
            "recordType": "reference",
            "historicalBackgroundZh": "该肖像反映审美主义在跨大西洋媒体中的形象传播：王尔德的服饰、姿态与摄影陈设共同构成了19世纪末“审美人格”的可复制视觉模板。",
            "historicalBackgroundEn": "This portrait reflects transatlantic media circulation of Aestheticism: Wilde's styling, pose, and studio staging formed a reproducible late-nineteenth-century visual template of the 'aesthetic personality.'",
        },
        "sources": ["https://www.metmuseum.org/art/collection/search/283247"],
    },
    "industrial_reform-s002-i09": {
        "title": "Crystal Palace, Hyde Park (reference exterior view)",
        "description": "Reference exterior engraving of the Crystal Palace in Hyde Park for the Great Exhibition of 1851.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Great Exhibition context)",
            "author": "Britain artist",
            "productionPlace": "London",
            "region": "Britain",
            "style": "Industrial exhibition architecture reference",
            "material": "Engraving / print reproduction",
            "recordType": "reference",
            "historicalBackgroundZh": "该图用于说明水晶宫作为预制铁玻建筑的范式意义：它将工程效率、帝国展陈与公众消费空间整合为同一视觉事件。",
            "historicalBackgroundEn": "This view highlights the Crystal Palace as a model of prefabricated iron-and-glass architecture, integrating engineering efficiency, imperial display, and mass public consumption in one visual event.",
        },
        "sources": ["https://www.britannica.com/topic/Crystal-Palace-building-London"],
    },
    "industrial_reform-s002-i10": {
        "title": "John Ruskin as a young man (reference portrait)",
        "description": "Reference portrait of young John Ruskin, used to introduce his role in Victorian art criticism and design ethics.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Victorian criticism context)",
            "author": "Britain artist",
            "productionPlace": "Britain",
            "region": "Britain",
            "style": "Victorian portrait reference",
            "material": "Watercolor / print reproduction",
            "recordType": "reference",
            "historicalBackgroundZh": "该肖像用于引入拉斯金早期思想语境：其对哥特、劳动与“诚实装饰”的论述深刻影响了工艺美术与设计改革中的伦理维度。",
            "historicalBackgroundEn": "This portrait introduces Ruskin's early intellectual context; his writings on Gothic form, labor, and 'truthful' ornament strongly shaped the ethical dimension of Arts and Crafts and design reform.",
        },
        "sources": ["https://commons.wikimedia.org/wiki/File:Portrait_of_John_Ruskin_as_a_young_man_Wellcome_L0002301.jpg"],
    },
    "industrial_reform-s002-i11": {
        "title": "Joseph Paxton (engraved portrait reference)",
        "description": "Reference engraved portrait of Joseph Paxton, designer of the Crystal Palace.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Great Exhibition context)",
            "author": "S. W. Reynolds (engraver), after O. Oakley",
            "productionPlace": "Britain",
            "region": "Britain",
            "style": "Victorian engraved portrait",
            "material": "Engraving",
            "recordType": "reference",
            "historicalBackgroundZh": "该版画强调帕克斯顿的工程与园艺背景如何转化为1851年展馆设计实践，体现维多利亚时期工程师形象的公共传播机制。",
            "historicalBackgroundEn": "This engraving emphasizes how Paxton's engineering and horticultural background informed the 1851 exhibition building, reflecting Victorian public circulation of engineer-innovator identities.",
        },
        "sources": ["https://collection.sciencemuseumgroup.org.uk/objects/co67814/joseph-paxton"],
    },
    "industrial_reform-s003-i01": {
        "title": "Imperial Federation: Map of the World (The Graphic supplement)",
        "description": "Pictorial imperial map associated with Walter Crane and published as an 1886 supplement to The Graphic.",
        "metadata": {
            "year": "",
            "period": "Late 19th century (high imperial visual culture)",
            "author": "Walter Crane (with J. C. R. Colomb, statistics)",
            "productionPlace": "London",
            "region": "Britain / Global Empire",
            "style": "Pictorial imperial map",
            "material": "Colour lithograph",
            "recordType": "artwork",
            "historicalBackgroundZh": "该地图把帝国版图、航运路线与统计信息整合为单幅图像，是维多利亚晚期通过视觉媒介建构帝国共同体想象的重要案例。",
            "historicalBackgroundEn": "This map integrates imperial territory, shipping routes, and statistics into a single image, making it a key late-Victorian case of constructing imperial community through visual media.",
        },
        "sources": ["https://www.davidrumsey.com/luna/servlet/s/rf5w0x"],
    },
    "industrial_reform-s003-i02": {
        "title": "Modern credit-card image (teaching joke reference)",
        "description": "Contemporary credit-card image used in class as a humorous contrast, not a core nineteenth-century artwork.",
        "metadata": {
            "year": "",
            "period": "Late 20th to early 21st century (contemporary reference)",
            "author": "Global artist",
            "productionPlace": "Global",
            "region": "Global",
            "style": "Contemporary commercial graphic",
            "material": "Digital image",
            "recordType": "reference",
            "historicalBackgroundZh": "该图为课堂对照用的现代图像，不属于19世纪设计改革案例本体，主要用于轻松提示“credit”一词的双关。",
            "historicalBackgroundEn": "This is a modern classroom comparison image rather than a nineteenth-century reform case, mainly used as a playful pun on the word 'credit.'",
        },
        "sources": [],
    },
    "industrial_reform-s004-i01": {
        "title": "Crystal Palace, Hyde Park (exterior view)",
        "description": "Exterior view of the Crystal Palace built for the Great Exhibition in Hyde Park, London.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Great Exhibition architecture)",
            "author": "Britain artist",
            "productionPlace": "London",
            "region": "Britain",
            "style": "Industrial exhibition architecture",
            "material": "Engraving / print reproduction",
            "recordType": "artwork",
            "historicalBackgroundZh": "该外景图显示水晶宫以标准化构件实现大跨度与快速建造，成为工业时代展览建筑与公众奇观经济结合的标志性形态。",
            "historicalBackgroundEn": "This exterior view shows how standardized components enabled rapid construction and large spans, making the Crystal Palace emblematic of industrial-age exhibition architecture and spectacle economy.",
        },
        "sources": ["https://www.britannica.com/topic/Crystal-Palace-building-London"],
    },
    "industrial_reform-s004-i02": {
        "title": "Great Exhibition interior view (Dickinson's Comprehensive Pictures)",
        "description": "Interior view linked to Dickinson Brothers' visual documentation of the Great Exhibition.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Great Exhibition print culture)",
            "author": "Dickinson Brothers (publishers)",
            "productionPlace": "London",
            "region": "Britain",
            "style": "Exhibition lithograph",
            "material": "Color lithograph",
            "recordType": "artwork",
            "historicalBackgroundZh": "该室内图像体现了博览会如何通过图像出版扩大影响：展陈秩序、商品密度与观众行为被编码为可流通的视觉知识。",
            "historicalBackgroundEn": "This interior image shows how exhibition publishing expanded the event's reach: display order, commodity density, and visitor behavior were encoded as portable visual knowledge.",
        },
        "sources": ["https://dome.mit.edu/handle/1721.3/43705"],
    },
    "industrial_reform-s005-i01": {
        "title": "Floral furnishing chintz (False Principles example)",
        "description": "Printed furnishing chintz with naturalistic floral motifs, discussed in Henry Cole's design-reform critique of 'false principles.'",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Design Reform critique context)",
            "author": "Britain artist",
            "productionPlace": "Britain",
            "region": "Britain",
            "style": "Victorian furnishing print",
            "material": "Printed cotton textile (chintz)",
            "recordType": "artwork",
            "historicalBackgroundZh": "该印花棉布常在“False Principles”语境中被作为反例：其写实花卉与缎带错视效果被批评为偏离结构与用途的装饰“真实”。",
            "historicalBackgroundEn": "In 'False Principles' discourse, this chintz was treated as a negative example: its naturalistic flowers and illusionistic ribbon effects were criticized as ornament detached from structure and use.",
        },
        "sources": ["https://www.vam.ac.uk/articles/wallpaper-design-reform"],
    },
    "industrial_reform-s005-i02": {
        "title": "R. W. Winfield, gas jet lamp in the form of a convolvulus",
        "description": "Gas jet lamp attributed to R. W. Winfield of Birmingham, cited in slide text as an 1848 example criticized in design-reform debates.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Design Reform critique context)",
            "author": "R. W. Winfield",
            "productionPlace": "Birmingham",
            "region": "Britain",
            "style": "Victorian historicist industrial decorative object",
            "material": "Metal (gilt metal / gas-light fitting)",
            "recordType": "artwork",
            "historicalBackgroundZh": "该器物在课程中作为科尔“False Principles”批评案例，反映维多利亚中期关于自然模仿装饰、工业复制与“良好品味”标准的争论。",
            "historicalBackgroundEn": "In class this object functions as a 'False Principles' case from Cole's critique, reflecting mid-Victorian debates over naturalistic imitation, industrial reproduction, and standards of good taste.",
        },
        "sources": ["https://journalhosting.ucalgary.ca/index.php/racar/article/download/38323/29271/106724"],
    },
    "industrial_reform-s006-i01": {
        "title": "Heywood, Higginbottom and Smith wallpaper with Crystal Palace and Serpentine",
        "description": "Machine-printed wallpaper depicting the Crystal Palace and Serpentine landscape, presented in design-reform teaching context.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Design Reform and Great Exhibition context)",
            "author": "Heywood, Higginbottom and Smith",
            "productionPlace": "Manchester",
            "region": "Britain",
            "style": "Victorian wallpaper design",
            "material": "Machine-printed wallpaper on paper",
            "recordType": "artwork",
            "historicalBackgroundZh": "该壁纸将展览建筑景观转化为可家居消费的图像商品，体现了博览会视觉文化向日常室内空间扩散的机制，也成为设计改革批评的对象。",
            "historicalBackgroundEn": "This wallpaper turned exhibition architecture into domestic visual commodity, showing how Great Exhibition imagery moved into interiors and became a target of design-reform criticism.",
        },
        "sources": ["https://www.vam.ac.uk/articles/wallpaper-design-reform"],
    },
    "industrial_reform-s006-i02": {
        "title": "R. W. Winfield, gas jet lamp in the form of a convolvulus (repeat detail)",
        "description": "Repeat image of the Winfield convolvulus gas lamp, reused on the slide that quotes Henry Cole's critique of imitative ornament.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Design Reform critique context)",
            "author": "R. W. Winfield",
            "productionPlace": "Birmingham",
            "region": "Britain",
            "style": "Victorian historicist industrial decorative object",
            "material": "Metal (gilt metal / gas-light fitting)",
            "recordType": "artwork",
            "historicalBackgroundZh": "该图为同一灯具在后续批评页中的重复呈现，用于强调科尔对“自然摹仿式装饰”及其视觉夸张性的反对。",
            "historicalBackgroundEn": "This is a repeated image of the same lamp on the following critique slide, used to emphasize Cole's opposition to natural-imitation ornament and visual excess.",
        },
        "sources": ["https://journalhosting.ucalgary.ca/index.php/racar/article/download/38323/29271/106724"],
    },
    "industrial_reform-s007-i01": {
        "title": "Felix Summerly tea service (Henry Cole design, Minton manufacture)",
        "description": "Part of the Felix Summerly tea service designed by Henry Cole and manufactured by Minton for reform-minded domestic consumption.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Design Reform domestic ware context)",
            "author": "Henry Cole (design) and Minton & Co. (manufacture)",
            "productionPlace": "Stoke-on-Trent",
            "region": "Britain",
            "style": "Design Reform industrial tableware",
            "material": "Earthenware",
            "recordType": "artwork",
            "historicalBackgroundZh": "该茶具体现“以可负担日用品推广良好设计”的改革路径：通过标准化生产与简化造型，把审美教育嵌入中产家庭日常器用。",
            "historicalBackgroundEn": "This tea set exemplifies reform through affordable domestic goods: standardized production and simplified form embedded aesthetic education into everyday middle-class use.",
        },
        "sources": ["https://collections.vam.ac.uk/item/O8085/henry-cole-tea-service-milk-jug-cole-henry-sir/"],
    },
    "industrial_reform-s007-i02": {
        "title": "“Minster” jug (Henry Cole / Felix Summerly context)",
        "description": "Relief-decorated 'Minster' jug shown on the comparison side of the slide and associated with Henry Cole's design-reform domestic ware program.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Design Reform domestic ware context)",
            "author": "Stoke-on-Trent artist",
            "productionPlace": "Stoke-on-Trent",
            "region": "Britain",
            "style": "Gothic Revival influenced domestic ceramic",
            "material": "Earthenware / parian-type ceramic",
            "recordType": "reference",
            "historicalBackgroundZh": "该壶在课程中作为对比对象，显示设计改革并非单一风格，而是在宗教历史风格、工业可制造性与家庭消费之间持续调和。",
            "historicalBackgroundEn": "Used in class as a comparison object, this jug shows that design reform was not a single style but an ongoing negotiation between historicist language, manufacturability, and domestic consumption.",
        },
        "sources": ["https://www.britishmuseum.org/collection/object/H_2009-8049-38"],
    },
    "industrial_reform-s008-i01": {
        "title": "The Medieval Court, Great Exhibition (Louis Haghe view)",
        "description": "Lithographic view of the Medieval Court at the Great Exhibition, associated with Louis Haghe and Dickinson publication.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Great Exhibition medieval display context)",
            "author": "Louis Haghe; Dickinson Brothers (publishers)",
            "productionPlace": "London",
            "region": "Britain",
            "style": "Exhibition view lithograph",
            "material": "Color lithograph",
            "recordType": "artwork",
            "historicalBackgroundZh": "该图展示中世纪展区如何把宗教器物、金工与装饰工艺编排为可观赏的历史场景，体现博览会以“风格化历史”组织现代消费视觉的机制。",
            "historicalBackgroundEn": "This view shows how the Medieval Court staged ecclesiastical objects, metalwork, and ornament as a consumable historical scene, demonstrating how the Exhibition organized modern visual consumption through stylized history.",
        },
        "sources": ["https://www.rct.uk/collection/themes/exhibitions/victoria-albert-art-love/the-queens-gallery-buckingham-palace/the-great-exhibition-the-medieval-court"],
    },
    "industrial_reform-s008-i02": {
        "title": "The Great Stove (A.W.N. Pugin; Hardman and Minton)",
        "description": "The Great Stove shown as a Gothic Revival centerpiece linked to Pugin, John Hardman & Co., and Minton tiles.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Great Exhibition medieval display context)",
            "author": "A.W.N. Pugin; John Hardman & Co.; Minton",
            "productionPlace": "London / Birmingham / Stoke-on-Trent",
            "region": "Britain",
            "style": "Gothic Revival exhibition decorative arts",
            "material": "Ironwork with ceramic tile decoration",
            "recordType": "artwork",
            "historicalBackgroundZh": "该炉具体现1851年展陈中“工业制造 + 历史风格”混合策略：通过哥特构件与彩釉面板，把中世纪权威转译为现代国家设计品味。",
            "historicalBackgroundEn": "This stove exemplifies the 1851 strategy of combining industrial manufacture with historicist style: Gothic structure and polychrome panels translated medieval authority into modern national design taste.",
        },
        "sources": ["https://library.si.edu/image-gallery/101071", "https://www.rct.uk/collection/themes/exhibitions/victoria-albert-art-love/the-queens-gallery-buckingham-palace/the-great-exhibition-the-medieval-court"],
    },
    "industrial_reform-s008-i03": {
        "title": "Jardiniere from the Medieval Court (attributed to A.W.N. Pugin)",
        "description": "Small medievalizing jardiniere associated with the Great Exhibition Medieval Court and attributed in scholarship to Pugin's design context.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Great Exhibition medieval display context)",
            "author": "A.W.N. Pugin context; Hardman and Minton workshop tradition",
            "productionPlace": "Britain",
            "region": "Britain",
            "style": "Gothic Revival polychrome decorative object",
            "material": "Gilt metal frame with decorated ceramic panels",
            "recordType": "artwork",
            "historicalBackgroundZh": "该器物被用于中世纪展区语境，展示哥特语汇在小型陈设物中的工业化转译方式：金属框架与彩饰面板共同强化“历史风格可复制化”的设计逻辑。",
            "historicalBackgroundEn": "Shown in Medieval Court context, this object demonstrates industrial translation of Gothic language into small decorative furnishings: metal framing and colored panels reinforce the logic of reproducible historicist style.",
        },
        "sources": ["https://bifmo.furniturehistorysociety.org/entry/the-great-exhibition-1851-medieval-court/", "https://collections.vam.ac.uk/item/O116534/jardiniere-pugin-augustus-welby-northmore/"],
    },
    "industrial_reform-s010-i01": {
        "title": "Frontispiece to The True Principles of Pointed or Christian Architecture",
        "description": "Frontispiece image from A.W.N. Pugin's architectural treatise on pointed (Gothic) architecture.",
        "metadata": {
            "year": "",
            "period": "Early to mid-19th century (Gothic Revival theory context)",
            "author": "A.W.N. Pugin",
            "productionPlace": "London",
            "region": "Britain",
            "style": "Gothic Revival architectural print culture",
            "material": "Printed book frontispiece",
            "recordType": "artwork",
            "historicalBackgroundZh": "该扉页把“建筑、信仰、工艺伦理”整合为普金理论的视觉入口，成为设计改革课程中理解哥特复兴思想体系的关键文献图像。",
            "historicalBackgroundEn": "This frontispiece serves as a visual gateway to Pugin's integration of architecture, faith, and craft ethics, making it a key documentary image for understanding Gothic Revival theory in design reform.",
        },
        "sources": ["https://archive.org/details/trueprinciplesof00pugi", "https://www.gla.ac.uk/myglasgow/library/files/special/teach/gothic/front.html"],
    },
    "industrial_reform-s011-i01": {
        "title": "A.W.N. Pugin portrait with quotation context (reference)",
        "description": "Reference portrait of A.W.N. Pugin used alongside the quote slide to emphasize his moral and pedagogical claims for architecture.",
        "metadata": {
            "year": "",
            "period": "Mid-19th century (Gothic Revival context)",
            "author": "Britain artist",
            "productionPlace": "Britain",
            "region": "Britain",
            "style": "Victorian portrait reference",
            "material": "Oil portrait reproduction",
            "recordType": "reference",
            "historicalBackgroundZh": "该肖像配合引文使用，强调普金将建筑视为社会道德教育媒介的立场；在课程中用于连接理论文本与人物形象传播。",
            "historicalBackgroundEn": "Used with the quote slide, this portrait stresses Pugin's claim that architecture functions as a medium of moral education; in class it links theoretical text with image-based persona building.",
        },
        "sources": ["https://www.npg.org.uk/collections/search/portrait/mw1404/Augustus-Welby-Northmore-Pugin", WEB_SOURCES["britannica_pugin"]],
    },
}

BACKGROUND_RULES = [
    {
        "keywords": [
            "art nouveau",
            "horta",
            "guimard",
            "mucha",
            "galle",
            "van de velde",
            "lautrec",
            "mackmurdo",
            "josef hoffmann",
            "wiener werkstatte",
            "charles rennie mackintosh",
        ],
        "en": (
            "Art Nouveau emerged in the late nineteenth century, drew heavily on natural forms, "
            "and used flowing 'whiplash' curves while seeking to unify fine and applied arts."
        ),
        "zh": "新艺术运动兴起于19世纪末，强调自然母题与“鞭线”曲线，并尝试统一纯艺术与应用艺术。",
        "sources": [WEB_SOURCES["met_art_nouveau"]],
    },
    {
        "keywords": ["crystal palace", "great exhibition", "henry cole", "chamber of horrors", "false principles"],
        "en": (
            "Closely tied to the Great Exhibition (1851), where reformers such as Henry Cole used museum "
            "display and criticism to improve public taste and design standards under industrial production."
        ),
        "zh": "与1851年大博览会密切相关；亨利·科尔等改革者通过博物馆展示与批评机制，推动工业时代的设计标准与公共审美改良。",
        "sources": [WEB_SOURCES["va_building_museum"]],
    },
    {
        "keywords": ["william morris", "arts and crafts", "merton abbey", "john ruskin"],
        "en": (
            "Associated with Arts and Crafts critiques of industrialization, privileging skilled handwork, "
            "material honesty, and socially meaningful design in response to factory standardization."
        ),
        "zh": "关联工艺美术运动对工业化的批判，强调手工技艺、材料诚实性与具有社会意义的设计，以回应工厂化标准生产。",
        "sources": [WEB_SOURCES["va_arts_and_crafts"]],
    },
    {
        "keywords": ["a.w.n. pugin", "pugin", "true principles of christian architecture", "the christian architect"],
        "en": (
            "Linked to A.W.N. Pugin's Gothic Revival theory, which framed medieval architecture and ornament "
            "as morally grounded alternatives to eclectic industrial-era taste."
        ),
        "zh": "与A.W.N.普金的哥特复兴理论相关，他将中世纪建筑与装饰视为对工业时代折衷审美的道德性替代方案。",
        "sources": [WEB_SOURCES["britannica_pugin"]],
    },
    {
        "keywords": ["nkisi", "mangaaka", "yombe", "power figure"],
        "en": (
            "Kongo power figures (nkisi nkondi) activated spiritual force through inserted materials and were used "
            "in oath-taking, conflict mediation, and protection in community legal-spiritual life."
        ),
        "zh": "刚果权力雕像（nkisi nkondi）通过嵌入材料激活灵力，用于誓约、纠纷调解与护佑，兼具法律与宗教功能。",
        "sources": [WEB_SOURCES["met_nkisi"]],
    },
    {
        "keywords": ["dogon seated couple", "barnes foundation master"],
        "en": (
            "Dogon couple sculptures are often interpreted through themes of lineage, social complementarity, "
            "and ritual authority, with strong regional sculptural identities in Mali."
        ),
        "zh": "多贡成对雕像常与血缘、社会互补关系及仪式权力相关，并体现马里地区鲜明的雕刻传统。",
        "sources": [WEB_SOURCES["met_dogon_couple"]],
    },
    {
        "keywords": ["iyoba", "pendant mask", "court of benin", "edo artist", "queen mother"],
        "en": (
            "Benin ivory pendant masks (iyoba) relate to royal court authority and memory politics, and are often "
            "discussed today within histories of colonial collecting and global museum display."
        ),
        "zh": "贝宁王国的象牙母后挂饰（iyoba）关联宫廷权力与王朝记忆，也常在当代被置于殖民收藏史与全球博物馆展示史中讨论。",
        "sources": [WEB_SOURCES["met_iyoba_mask"], WEB_SOURCES["bm_iyoba_mask"]],
    },
    {
        "keywords": ["christopher dresser", "teapot", "hukin", "heath", "electroplated"],
        "en": (
            "Christopher Dresser's metalwork exemplifies industrial-age design expertise, translating botanical "
            "study and abstract geometry into manufacturable modern forms."
        ),
        "zh": "克里斯托弗·德雷瑟的金属器设计体现工业时代的专业设计观，将植物研究与几何抽象转化为可制造的现代形态。",
        "sources": [WEB_SOURCES["met_dresser_teapot"], WEB_SOURCES["met_dresser_kettle"]],
    },
    {
        "keywords": ["mackmurdo chair", "arthur haygate mackmurdo"],
        "en": (
            "Mackmurdo's furniture is frequently cited as a bridge between Arts and Crafts and early Art Nouveau, "
            "especially for linear rhythmic ornament and integrated interior design thinking."
        ),
        "zh": "麦克默多家具常被视为连接工艺美术运动与早期新艺术的重要桥梁，尤其体现在线性节奏装饰与整体室内设计观上。",
        "sources": [WEB_SOURCES["huntington_mackmurdo_chair"], WEB_SOURCES["met_art_nouveau"]],
    },
    {
        "keywords": ["metropolitan museum", "the met", "british museum", "brooklyn museum"],
        "en": "Current understanding is shaped by museum collection and display frameworks.",
        "zh": "当下的理解方式受到博物馆收藏与陈列框架的影响。",
        "sources": [],
    },
    {
        "keywords": ["dogon", "bamana", "kongo", "edo", "bamileke", "yoruba", "benin"],
        "en": "Rooted in specific African ritual, court, or community contexts before modern museum display.",
        "zh": "根植于非洲特定族群的仪式、宫廷或社区语境，早于现代博物馆展示体系。",
        "sources": [],
    },
    {
        "keywords": ["napoleon", "empire", "bonaparte"],
        "en": "Connected to Napoleonic / Empire politics and visual statecraft in Europe.",
        "zh": "与拿破仑帝国政治及其在欧洲的视觉国家建构相关。",
        "sources": [],
    },
]


@dataclass
class SlideData:
    slide_number: int
    slide_text: str
    image_targets: List[str]


def slide_sort_key(path: str) -> int:
    m = re.search(r"slide(\d+)\.xml$", path)
    return int(m.group(1)) if m else 0


def collapse_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def unique_order(values: Sequence[str]) -> List[str]:
    out = []
    seen = set()
    for value in values:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def parse_slide_text(xml_bytes: bytes) -> str:
    root = ET.fromstring(xml_bytes)
    texts = []
    for node in root.findall(".//a:t", NS):
        if node.text and node.text.strip():
            texts.append(node.text.strip())
    return collapse_spaces(" ".join(texts))


def parse_slide_images(xml_bytes: bytes) -> List[str]:
    root = ET.fromstring(xml_bytes)
    embeds = []
    embed_key = f"{{{NS['r']}}}embed"
    for node in root.findall(".//a:blip", NS):
        rid = node.attrib.get(embed_key)
        if rid:
            embeds.append(rid)
    return embeds


def parse_relationships(xml_bytes: bytes) -> Dict[str, str]:
    root = ET.fromstring(xml_bytes)
    mapping = {}
    for rel in root.findall("pr:Relationship", NS):
        rel_type = rel.attrib.get("Type", "")
        if rel_type.endswith("/image"):
            mapping[rel.attrib["Id"]] = rel.attrib.get("Target", "")
    return mapping


def normalize_target(base: str, target: str) -> str:
    return posixpath.normpath(posixpath.join(posixpath.dirname(base), target))


def normalize_year_range(start: str, end: str) -> str:
    if not end:
        return start
    if len(end) == 2:
        end = start[:2] + end
    return f"{start}-{end}"


def extract_year(text: str) -> str:
    normalized = text.replace("–", "-")
    pattern = re.compile(r"(?:c(?:a)?\.?\s*)?(1[4-9]\d{2}|20[0-2]\d)(?:\s*-\s*(\d{2,4}))?")
    candidates: List[Tuple[int, int, str, str]] = []
    for match in pattern.finditer(normalized):
        start_year = int(match.group(1))
        if start_year >= 2025:
            # Course dates or current-year class schedule markers.
            continue

        end_raw = match.group(2) or ""
        year_text = normalize_year_range(match.group(1), end_raw)
        end_year = int(year_text.split("-")[1]) if "-" in year_text and year_text.split("-")[1].isdigit() else start_year

        window_left = max(0, match.start() - 35)
        window_right = min(len(normalized), match.end() + 80)
        window = normalized[window_left:window_right]
        window_lower = window.lower()

        score = 0
        if YEAR_CONTEXT_HINTS.search(window):
            score += 4
        if "century" in window_lower or "th century" in window_lower:
            score += 3
        if "c." in match.group(0).lower() or "ca." in match.group(0).lower():
            score += 2
        if any(token in window_lower for token in ["artist", "wood", "bronze", "porcelain", "oil", "lithograph"]):
            score += 2
        if 2025 <= end_year <= 2035:
            score -= 6
        span = end_year - start_year
        if span >= 70:
            # Most likely an artist lifespan rather than a work creation range.
            score -= 5
        elif span >= 40:
            score -= 2

        candidates.append((score, -match.start(), year_text, window))

    if not candidates:
        return ""

    candidates.sort(reverse=True)
    best_score, _, best_year, _ = candidates[0]
    if best_score < 0:
        return ""
    return best_year


def extract_century_period(text: str) -> str:
    normalized = text.replace("–", "-")
    normalized = re.sub(r"(\d{1,2})\s+(st|nd|rd|th)\b", r"\1\2", normalized, flags=re.IGNORECASE)
    patterns = [
        r"(\d{1,2}(?:st|nd|rd|th)\s*-\s*(?:early|mid|late)\s+\d{1,2}(?:st|nd|rd|th)\s+century)",
        r"((?:early|mid|late)\s+\d{1,2}(?:st|nd|rd|th)\s*-\s*(?:early|mid|late)\s+\d{1,2}(?:st|nd|rd|th)\s+century)",
        r"((?:early|mid|late)\s+\d{1,2}(?:st|nd|rd|th)\s*-\s*\d{1,2}(?:st|nd|rd|th)\s+century)",
        r"((?:early|mid|late)\s+\d{1,2}(?:st|nd|rd|th)\s+century)",
        r"(\d{1,2}(?:st|nd|rd|th)\s*-\s*\d{1,2}(?:st|nd|rd|th)\s+century)",
        r"(\d{1,2}(?:st|nd|rd|th)\s+century)",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if match:
            return collapse_spaces(match.group(1))
    return ""


def ordinal_century(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def period_from_year(year_text: str) -> str:
    if not year_text:
        return ""
    start_s = year_text.split("-")[0]
    if not start_s.isdigit():
        return ""
    start = int(start_s)
    century = (start - 1) // 100 + 1
    return f"{ordinal_century(century)} century (c. {year_text})"


def extract_period(text: str, year: str) -> str:
    century_period = extract_century_period(text)
    if century_period:
        return century_period
    return period_from_year(year)


def classify_record_type(title: str, description: str) -> str:
    blob = f"{title} {description}".lower()
    reference_markers = [
        "just for context",
        "not required",
        "study slide",
        "good study slide",
        "stars of the lecture",
        "happy valentine",
        "youtube.com",
        "a witty writer",
        "cleanliness should be the first consideration",
        "credit card not required",
        "parsons cooper hewitt",
        "it is not that men are ill fed",
    ]
    if any(marker in blob for marker in reference_markers):
        return "reference"
    if not description.strip():
        return "reference"
    return "artwork"


def apply_web_enrichment(title: str, description: str, metadata: Dict[str, str], existing_sources: List[str]) -> Tuple[Dict[str, str], List[str]]:
    blob = f"{title} {description}".lower()
    updated = dict(metadata)
    sources = list(existing_sources)

    for rule in WEB_ENRICHMENT_OVERRIDES:
        any_match = any(keyword in blob for keyword in rule.get("match_any", []))
        all_match = all(keyword in blob for keyword in rule.get("match_all", []))
        if not any_match:
            continue
        if rule.get("match_all") and not all_match:
            continue

        for key, value in rule.get("set", {}).items():
            current = updated.get(key, "")
            if key in {"historicalBackgroundZh", "historicalBackgroundEn", "style"}:
                updated[key] = value
            elif key == "author":
                if not current or str(current).endswith("artist") or current in {"N/A", "N/A (reference)"}:
                    updated[key] = value
            elif value and (not current or current in {"N/A", "N/A (reference)"}):
                updated[key] = value

        sources = unique_order([*sources, *rule.get("sources", [])])

    return updated, sources


def apply_manual_item_override(
    item_id: str,
    title: str,
    description: str,
    metadata: Dict[str, str],
    existing_sources: List[str],
) -> Tuple[str, str, Dict[str, str], List[str]]:
    override = MANUAL_ITEM_OVERRIDES.get(item_id)
    if not override:
        return title, description, metadata, existing_sources

    updated_title = override.get("title", title)
    updated_description = override.get("description", description)
    updated_metadata = dict(metadata)
    updated_metadata.update(override.get("metadata", {}))
    if "sources" in override:
        updated_sources = unique_order(override.get("sources", []))
    else:
        updated_sources = unique_order(existing_sources)
    return updated_title, updated_description, updated_metadata, updated_sources


def clean_name(name: str) -> str:
    return collapse_spaces(name.strip(" ,.;:()[]{}\"'"))


def trim_author_candidate(name: str) -> str:
    candidate = clean_name(name)
    if ". " in candidate:
        prefix, rest = candidate.split(". ", 1)
        if prefix.lower() in {"england", "france", "britain", "london", "paris", "america"}:
            candidate = clean_name(rest)
    low = candidate.lower()
    if low.startswith("wiener werkstatte "):
        candidate = candidate.split(" ", 2)[2]
    if low.startswith("co. ") or low.startswith("company "):
        return ""

    words = candidate.split()
    while words and words[-1].lower().strip(".:-") in AUTHOR_OBJECT_WORDS:
        words.pop()
    return clean_name(" ".join(words))


def is_valid_author(name: str) -> bool:
    if not name:
        return False
    if any(ch.isdigit() for ch in name):
        return False
    words = name.split()
    if len(words) < 2 or len(words) > 5:
        return False
    lower_words = [w.lower().strip(".:-") for w in words]
    if any(w in AUTHOR_STOPWORDS for w in lower_words):
        return False
    if any(w in AUTHOR_OBJECT_WORDS for w in lower_words):
        return False
    if any(w in {"the", "and", "of", "for", "with"} for w in lower_words):
        return False
    if lower_words[0] in NON_PERSON_PREFIXES:
        return False
    if name.isupper():
        return False
    return True


def extract_author(text: str) -> str:
    pattern = r"\b([A-Z][A-Za-z'’\-.]+(?:\s+(?:[A-Z][A-Za-z'’\-.]+|de|van|von|da|del|du|la)){1,6})\s*,"
    for match in re.finditer(pattern, text):
        candidate = trim_author_candidate(match.group(1))
        if not is_valid_author(candidate):
            continue
        tail = text[match.end() : match.end() + 120]
        if AUTHOR_CONTEXT_HINTS.search(tail):
            return candidate

    artist_phrase = re.search(r"\b([A-Z][A-Za-z'’\-.]+(?:\s+[A-Z][A-Za-z'’\-.]+){0,2}\s+artist)\b", text)
    if artist_phrase:
        return clean_name(artist_phrase.group(1))

    return ""


def detect_region(text: str, default_region: str) -> str:
    low = text.lower()
    hits = [region for region, words in REGION_KEYWORDS.items() if any(word in low for word in words)]
    if not hits:
        return default_region
    return " / ".join(unique_order(hits))


def detect_production_place(text: str, default_place: str) -> str:
    low = text.lower()
    hits = []
    for place, words in PLACE_KEYWORDS:
        if any(word in low for word in words):
            hits.append(place)
    if not hits:
        return default_place
    # Keep the place field readable in UI.
    return " / ".join(unique_order(hits)[:3])


def detect_style(text: str, default_style: str) -> str:
    low = text.lower()
    for style, words in STYLE_KEYWORDS.items():
        if any(word in low for word in words):
            return style
    return default_style


def extract_material(text: str) -> str:
    low = text.lower()
    found = []
    for pattern, label in MATERIAL_PATTERNS:
        if re.search(pattern, low):
            found.append(label)
    found = unique_order(found)
    if "Iron alloy" in found and "Iron" in found:
        found = [entry for entry in found if entry != "Iron"]
    if "Hard-paste porcelain" in found and "Porcelain" in found:
        found = [entry for entry in found if entry != "Porcelain"]
    if "Soft-paste porcelain" in found and "Porcelain" in found:
        found = [entry for entry in found if entry != "Porcelain"]
    if not found:
        return "Not stated in source slide."
    return ", ".join(found)


def infer_historical_background(text: str, deck: dict, style: str, region: str) -> Tuple[str, str, List[str]]:
    low = text.lower()
    notes_en: List[str] = []
    notes_zh: List[str] = []
    sources: List[str] = []

    for rule in BACKGROUND_RULES:
        if any(keyword in low for keyword in rule["keywords"]):
            notes_en.append(rule["en"])
            notes_zh.append(rule["zh"])
            sources.extend(rule.get("sources", []))

    if not notes_en:
        notes_en.append(deck["default_background_en"])
        notes_zh.append(deck["default_background_zh"])

    if style:
        notes_en.append(f"Style context: {style}.")
        notes_zh.append(f"风格语境：{style}。")
    if region:
        notes_en.append(f"Regional context: {region}.")
        notes_zh.append(f"地域语境：{region}。")

    merged_en = " ".join(unique_order(notes_en))[:460]
    merged_zh = " ".join(unique_order(notes_zh))[:280]
    return merged_zh, merged_en, unique_order(sources)


def is_boilerplate_title(candidate: str) -> bool:
    low = candidate.lower()
    boilerplate = [
        "topics",
        "week",
        "required slides",
        "stars of",
        "lecture",
        "just for",
        "credit card",
        "background",
    ]
    return any(b in low for b in boilerplate)


def derive_title(text: str, deck_title: str, slide_num: int, image_idx: int) -> str:
    if not text:
        return f"{deck_title} - Slide {slide_num} Image {image_idx}"

    working = collapse_spaces(text)
    if ":" in working and len(working.split(":")) > 1:
        working = working.split(":", 1)[1].strip()

    cut = re.split(
        r"\b(?:c(?:a)?\.?\s*)?(?:1[4-9]\d{2}|20[0-2]\d)\b|\b\d{1,2}(?:st|nd|rd|th)\s+century\b",
        working,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    cut = clean_name(cut)

    if not cut or is_boilerplate_title(cut):
        segments = [clean_name(seg) for seg in re.split(r"[.;]", text) if clean_name(seg)]
        non_boiler = [seg for seg in segments if not is_boilerplate_title(seg)]
        cut = non_boiler[0] if non_boiler else segments[0] if segments else ""

    if not cut:
        cut = f"{deck_title} - Slide {slide_num} Image {image_idx}"

    return cut[:120]


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


def deck_slides(zf: zipfile.ZipFile) -> List[SlideData]:
    slide_paths = sorted(
        [name for name in zf.namelist() if re.match(r"ppt/slides/slide\d+\.xml$", name)],
        key=slide_sort_key,
    )
    data = []
    for slide_path in slide_paths:
        slide_number = slide_sort_key(slide_path)
        slide_xml = zf.read(slide_path)
        slide_text = parse_slide_text(slide_xml)
        image_rel_ids = parse_slide_images(slide_xml)
        rel_path = f"ppt/slides/_rels/slide{slide_number}.xml.rels"
        rel_mapping = parse_relationships(zf.read(rel_path)) if rel_path in zf.namelist() else {}

        targets = []
        for rid in image_rel_ids:
            target = rel_mapping.get(rid)
            if target:
                targets.append(normalize_target(slide_path, target))
        data.append(SlideData(slide_number=slide_number, slide_text=slide_text, image_targets=targets))
    return data


def ensure_dirs() -> None:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def build() -> Tuple[List[dict], dict]:
    ensure_dirs()
    all_items: List[dict] = []
    stats = {}

    for deck in DECKS:
        deck_id = deck["id"]
        source: Path = deck["source"]
        if not source.exists():
            print(f"[WARN] Missing source: {source}")
            continue

        with zipfile.ZipFile(source) as zf:
            slides = deck_slides(zf)
            deck_dir = ASSETS_DIR / deck_id
            deck_dir.mkdir(parents=True, exist_ok=True)
            written_cache: Dict[str, str] = {}

            item_count = 0
            skipped = 0
            fallback_author_count = 0
            previous_work_meta: Dict[str, str] = {
                "year": "",
                "period": "",
                "author": "",
                "material": "",
                "production_place": "",
                "region": "",
                "style": "",
            }
            for slide in slides:
                for idx, image_path in enumerate(slide.image_targets, start=1):
                    ext = Path(image_path).suffix.lower()
                    if ext not in SUPPORTED_IMAGE_EXT:
                        skipped += 1
                        continue

                    if image_path in written_cache:
                        asset_rel_path = written_cache[image_path]
                    else:
                        image_bytes = zf.read(image_path)
                        out_name = f"s{slide.slide_number:03d}_i{idx:02d}{ext}"
                        out_path = deck_dir / out_name
                        if not out_path.exists():
                            out_path.write_bytes(image_bytes)
                        asset_rel_path = str(Path("assets") / deck_id / out_name)
                        written_cache[image_path] = asset_rel_path

                    item_id = f"{deck_id}-s{slide.slide_number:03d}-i{idx:02d}"
                    title = derive_title(slide.slide_text, deck["title"], slide.slide_number, idx)
                    item_description = slide.slide_text

                    year = extract_year(slide.slide_text)
                    period = extract_period(slide.slide_text, year)
                    region = detect_region(slide.slide_text, deck["default_region"])
                    production_place = detect_production_place(slide.slide_text, region)
                    style = detect_style(slide.slide_text, deck["default_style"])
                    material = extract_material(slide.slide_text)
                    historical_background_zh, historical_background_en, historical_background_sources = infer_historical_background(
                        slide.slide_text, deck, style, region
                    )

                    author = extract_author(slide.slide_text)
                    used_fallback_author = False
                    if not author:
                        author = f"{production_place} artist"
                        used_fallback_author = True

                    slide_text_low = slide.slide_text.lower()
                    detail_like = any(
                        marker in slide_text_low
                        for marker in ["see previous slide", "detail of", "same image", "details at right", "text added"]
                    )
                    if detail_like:
                        if not year and previous_work_meta["year"]:
                            year = previous_work_meta["year"]
                        if not period and previous_work_meta["period"]:
                            period = previous_work_meta["period"]
                        if material == "Not stated in source slide." and previous_work_meta["material"]:
                            material = previous_work_meta["material"]
                        if used_fallback_author and previous_work_meta["author"] and not previous_work_meta["author"].endswith("artist"):
                            author = previous_work_meta["author"]
                            used_fallback_author = False
                        if production_place in {"Europe", "Britain / Europe", "Sub-Saharan Africa", "Africa"} and previous_work_meta["production_place"]:
                            production_place = previous_work_meta["production_place"]

                    record_type = classify_record_type(title, slide.slide_text)

                    if not year and period and record_type == "artwork":
                        year = f"c. {period}"
                    if record_type == "reference":
                        year = year or "N/A (reference)"
                        period = period or "N/A (reference)"

                    metadata_values = {
                        "year": year,
                        "period": period,
                        "author": author,
                        "productionPlace": production_place,
                        "region": region,
                        "style": style,
                        "material": material,
                        "recordType": record_type,
                        "historicalBackgroundZh": historical_background_zh,
                        "historicalBackgroundEn": historical_background_en,
                    }
                    metadata_values, historical_background_sources = apply_web_enrichment(
                        title, item_description, metadata_values, historical_background_sources
                    )
                    title, item_description, metadata_values, historical_background_sources = apply_manual_item_override(
                        item_id, title, item_description, metadata_values, historical_background_sources
                    )

                    year = metadata_values.get("year", "")
                    period = metadata_values.get("period", "")
                    author = metadata_values.get("author", "")
                    production_place = metadata_values.get("productionPlace", "")
                    region = metadata_values.get("region", "")
                    style = metadata_values.get("style", "")
                    material = metadata_values.get("material", "")
                    record_type = metadata_values.get("recordType", record_type)
                    historical_background_zh = metadata_values.get("historicalBackgroundZh", "")
                    historical_background_en = metadata_values.get("historicalBackgroundEn", "")

                    if not author:
                        author = f"{production_place or region} artist"
                        used_fallback_author = True

                    if used_fallback_author:
                        fallback_author_count += 1

                    study_description = build_study_description(
                        material, period, historical_background_zh, historical_background_en
                    )
                    historical_background_combined = (
                        f"{historical_background_zh}\n{historical_background_en}"
                        if historical_background_zh or historical_background_en
                        else ""
                    )

                    tags = [
                        deck["title"],
                        *deck["tags"],
                        region,
                        style,
                        production_place,
                    ]
                    if period:
                        tags.append(period)
                    if year:
                        tags.append(year)
                    if material != "Not stated in source slide.":
                        tags.append(material)

                    if record_type == "artwork" and (year or period or material != "Not stated in source slide." or not author.endswith("artist")):
                        previous_work_meta = {
                            "year": year,
                            "period": period,
                            "author": author,
                            "material": material if material != "Not stated in source slide." else "",
                            "production_place": production_place,
                            "region": region,
                            "style": style,
                        }

                    all_items.append(
                        {
                            "id": item_id,
                            "deckId": deck_id,
                            "deckTitle": deck["title"],
                            "slideNumber": slide.slide_number,
                            "imageIndex": idx,
                            "title": title,
                            "description": item_description,
                            "studyDescription": study_description,
                            "image": asset_rel_path,
                            "metadata": {
                                "year": year,
                                "period": period,
                                "author": author,
                                "productionPlace": production_place,
                                "region": region,
                                "style": style,
                                "material": material,
                                "recordType": record_type,
                                "historicalBackground": historical_background_combined,
                                "historicalBackgroundZh": historical_background_zh,
                                "historicalBackgroundEn": historical_background_en,
                                "historicalBackgroundSources": historical_background_sources,
                            },
                            "tags": sorted(unique_order([t for t in tags if t])),
                        }
                    )
                    item_count += 1

            stats[deck_id] = {
                "slides": len(slides),
                "items": item_count,
                "skippedUnsupportedImages": skipped,
                "fallbackAuthors": fallback_author_count,
            }

    all_items.sort(key=lambda x: (x["deckTitle"], x["slideNumber"], x["imageIndex"]))
    return all_items, stats


def write_comparison_table(items: List[dict]) -> Path:
    out_path = DATA_DIR / "comparison_table.csv"
    header = [
        "id",
        "course",
        "slide",
        "image_index",
        "title",
        "record_type",
        "image_path",
        "year_creation",
        "period_creation",
        "author",
        "production_place",
        "region",
        "style",
        "material",
        "historical_background_zh",
        "historical_background_en",
        "historical_background_sources",
        "study_description",
        "raw_slide_text",
    ]

    with out_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(header)
        for item in items:
            meta = item.get("metadata", {})
            writer.writerow(
                [
                    item.get("id", ""),
                    item.get("deckTitle", ""),
                    item.get("slideNumber", ""),
                    item.get("imageIndex", ""),
                    item.get("title", ""),
                    meta.get("recordType", ""),
                    item.get("image", ""),
                    meta.get("year", ""),
                    meta.get("period", ""),
                    meta.get("author", ""),
                    meta.get("productionPlace", ""),
                    meta.get("region", ""),
                    meta.get("style", ""),
                    meta.get("material", ""),
                    meta.get("historicalBackgroundZh", ""),
                    meta.get("historicalBackgroundEn", ""),
                    " | ".join(meta.get("historicalBackgroundSources", []) or []),
                    item.get("studyDescription", ""),
                    item.get("description", ""),
                ]
            )
    return out_path


def main() -> None:
    items, stats = build()
    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "items": items,
        "decks": [{k: v for k, v in d.items() if k != "source"} for d in DECKS],
        "stats": stats,
    }
    out_path = DATA_DIR / "artworks.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    table_path = write_comparison_table(items)
    print(f"Wrote {len(items)} items -> {out_path}")
    print(f"Wrote comparison table -> {table_path}")
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
