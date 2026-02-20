const DATA_URL = "data/artworks.json";
const STORAGE_OVERRIDES = "artStudy.overrides.v3";
const STORAGE_NOTES = "artStudy.notes.v3";

const state = {
  items: [],
  filtered: [],
  currentIndex: 0,
  overrides: {},
  compareNotes: {},
  detailCategories: new Set(),
  detailItemId: null,
};

const $ = (id) => document.getElementById(id);

const elements = {
  searchInput: $("searchInput"),
  deckFilter: $("deckFilter"),
  recordTypeFilter: $("recordTypeFilter"),
  regionFilter: $("regionFilter"),
  styleFilter: $("styleFilter"),
  categoryFilter: $("categoryFilter"),
  resetFiltersBtn: $("resetFiltersBtn"),
  prevBtn: $("prevBtn"),
  nextBtn: $("nextBtn"),
  counter: $("counter"),
  currentImage: $("currentImage"),
  titleText: $("titleText"),
  metaText: $("metaText"),
  studyText: $("studyText"),
  quickTags: $("quickTags"),
  compareA: $("compareA"),
  compareB: $("compareB"),
  comparePreview: $("comparePreview"),
  compareHints: $("compareHints"),
  differenceNote: $("differenceNote"),
  saveNoteBtn: $("saveNoteBtn"),
  savedNotes: $("savedNotes"),
  detailDialog: $("detailDialog"),
  detailTitle: $("detailTitle"),
  detailImage: $("detailImage"),
  yearInput: $("yearInput"),
  periodInput: $("periodInput"),
  authorInput: $("authorInput"),
  productionPlaceInput: $("productionPlaceInput"),
  regionInput: $("regionInput"),
  styleInput: $("styleInput"),
  materialInput: $("materialInput"),
  historicalBackgroundZhInput: $("historicalBackgroundZhInput"),
  historicalBackgroundEnInput: $("historicalBackgroundEnInput"),
  backgroundSourcesText: $("backgroundSourcesText"),
  backgroundSourcesLinks: $("backgroundSourcesLinks"),
  newCategoryInput: $("newCategoryInput"),
  addCategoryBtn: $("addCategoryBtn"),
  detailCategories: $("detailCategories"),
  studyDescriptionText: $("studyDescriptionText"),
  descriptionText: $("descriptionText"),
  saveDetailBtn: $("saveDetailBtn"),
};

function readJSONFromStorage(key) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

function writeJSONToStorage(key, value) {
  localStorage.setItem(key, JSON.stringify(value));
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeText(value) {
  return String(value || "").trim();
}

function uniqueValues(values) {
  return [...new Set(values.map(normalizeText).filter(Boolean))].sort((a, b) => a.localeCompare(b, "zh-CN"));
}

function getItemById(id) {
  return state.items.find((item) => item.id === id) || null;
}

function buildStudyDescription(meta) {
  const materialZh = meta.material || "未标注";
  const periodZh = meta.period || "未标注";
  const backgroundZh = meta.historicalBackgroundZh || "未标注";
  const materialEn = meta.material || "Not stated in source slide.";
  const periodEn = meta.period || "Not stated in source slide.";
  const backgroundEn = meta.historicalBackgroundEn || "Not stated in source slide.";
  return `材质：${materialZh}。时期：${periodZh}。历史背景：${backgroundZh}\nMaterial: ${materialEn}. Period: ${periodEn}. Historical background: ${backgroundEn}`;
}

function applyAuthorFallback(author, productionPlace, region) {
  const normalized = normalizeText(author);
  if (normalized) return normalized;
  const place = normalizeText(productionPlace) || normalizeText(region) || "Unknown place";
  return `${place} artist`;
}

function getEffectiveItem(item) {
  const baseMeta = item.metadata || {};
  const override = state.overrides[item.id] || {};

  const year = normalizeText(override.year ?? baseMeta.year ?? "");
  const period = normalizeText(override.period ?? baseMeta.period ?? "");
  const productionPlace = normalizeText(override.productionPlace ?? baseMeta.productionPlace ?? baseMeta.region ?? "");
  const region = normalizeText(override.region ?? baseMeta.region ?? "");
  const style = normalizeText(override.style ?? baseMeta.style ?? "");
  const material = normalizeText(override.material ?? baseMeta.material ?? "");
  const historicalBackgroundZh = normalizeText(
    override.historicalBackgroundZh ?? override.historicalBackground ?? baseMeta.historicalBackgroundZh ?? ""
  );
  const historicalBackgroundEn = normalizeText(
    override.historicalBackgroundEn ?? baseMeta.historicalBackgroundEn ?? ""
  );
  const historicalBackgroundSources = Array.isArray(baseMeta.historicalBackgroundSources)
    ? baseMeta.historicalBackgroundSources
    : [];
  const recordType = normalizeText(baseMeta.recordType || "artwork");
  const author = applyAuthorFallback(override.author ?? baseMeta.author ?? "", productionPlace, region);
  const historicalBackground = [historicalBackgroundZh, historicalBackgroundEn].filter(Boolean).join("\n");

  const metadata = {
    year,
    period,
    author,
    productionPlace,
    region,
    style,
    material,
    recordType,
    historicalBackground,
    historicalBackgroundZh,
    historicalBackgroundEn,
    historicalBackgroundSources,
  };

  const customCategories = override.categories || [];
  const categories = uniqueValues([...(item.tags || []), ...customCategories]);

  return {
    ...item,
    metadata,
    categories,
    studyDescription: buildStudyDescription(metadata),
  };
}

function allEffectiveItems() {
  return state.items.map(getEffectiveItem);
}

function getPairKey(idA, idB) {
  return [idA, idB].sort().join("__");
}

function formatMeta(item) {
  const meta = item.metadata;
  const parts = [];
  if (meta.year) parts.push(`年份: ${meta.year}`);
  if (meta.period) parts.push(`时期: ${meta.period}`);
  if (meta.author) parts.push(`作者: ${meta.author}`);
  if (meta.productionPlace) parts.push(`生产地: ${meta.productionPlace}`);
  if (meta.region) parts.push(`地区: ${meta.region}`);
  if (meta.style) parts.push(`风格: ${meta.style}`);
  if (meta.recordType === "reference") parts.push("类型: 参考图");
  return parts.join(" | ") || "暂无元数据";
}

function renderTags(container, tags, removable = false) {
  container.innerHTML = "";
  tags.forEach((tag) => {
    const span = document.createElement("span");
    span.textContent = tag;
    if (removable) {
      span.style.cursor = "pointer";
      span.title = "点击移除";
      span.addEventListener("click", () => {
        state.detailCategories.delete(tag);
        renderTags(elements.detailCategories, [...state.detailCategories], true);
      });
    }
    container.appendChild(span);
  });
}

function populateSelect(selectEl, options, fallbackValue) {
  const previous = selectEl.value;
  selectEl.innerHTML = "";
  options.forEach((optionText) => {
    const option = document.createElement("option");
    option.value = optionText;
    option.textContent = optionText;
    selectEl.appendChild(option);
  });
  if (options.includes(previous)) {
    selectEl.value = previous;
  } else {
    selectEl.value = fallbackValue;
  }
}

function renderSourceLinks(container, sources) {
  container.innerHTML = "";
  const list = (sources || []).filter(Boolean);
  if (!list.length) {
    const span = document.createElement("span");
    span.textContent = "暂无来源链接";
    container.appendChild(span);
    return;
  }

  list.forEach((url) => {
    const a = document.createElement("a");
    a.href = url;
    a.target = "_blank";
    a.rel = "noopener";
    a.textContent = url;
    container.appendChild(a);
  });
}

function rebuildFilters() {
  const items = allEffectiveItems();
  const decks = uniqueValues(items.map((i) => i.deckTitle));
  const regions = uniqueValues(items.map((i) => i.metadata.region));
  const styles = uniqueValues(items.map((i) => i.metadata.style));
  const categories = uniqueValues(items.flatMap((i) => i.categories || []));

  populateSelect(elements.deckFilter, ["全部课程", ...decks], "全部课程");
  populateSelect(elements.recordTypeFilter, ["全部类型", "作品", "参考图"], "全部类型");
  populateSelect(elements.regionFilter, ["全部地区", ...regions], "全部地区");
  populateSelect(elements.styleFilter, ["全部风格", ...styles], "全部风格");
  populateSelect(elements.categoryFilter, ["全部分类", ...categories], "全部分类");
}

function applyFilters() {
  const search = elements.searchInput.value.trim().toLowerCase();
  const deck = elements.deckFilter.value;
  const recordTypeLabel = elements.recordTypeFilter.value;
  const region = elements.regionFilter.value;
  const style = elements.styleFilter.value;
  const category = elements.categoryFilter.value;
  const wantedRecordType =
    recordTypeLabel === "作品" ? "artwork" : recordTypeLabel === "参考图" ? "reference" : "all";

  const items = allEffectiveItems().filter((item) => {
    if (deck !== "全部课程" && item.deckTitle !== deck) return false;
    if (wantedRecordType !== "all" && item.metadata.recordType !== wantedRecordType) return false;
    if (region !== "全部地区" && item.metadata.region !== region) return false;
    if (style !== "全部风格" && item.metadata.style !== style) return false;
    if (category !== "全部分类" && !(item.categories || []).includes(category)) return false;

    if (!search) return true;
    const haystack = [
      item.title,
      item.description,
      item.studyDescription,
      item.metadata.year,
      item.metadata.period,
      item.metadata.author,
      item.metadata.productionPlace,
      item.metadata.region,
      item.metadata.style,
      item.metadata.material,
      item.metadata.recordType,
      item.metadata.historicalBackground,
      item.metadata.historicalBackgroundZh,
      item.metadata.historicalBackgroundEn,
      ...(item.metadata.historicalBackgroundSources || []),
      ...(item.categories || []),
    ]
      .join(" ")
      .toLowerCase();

    return haystack.includes(search);
  });

  state.filtered = items;
  if (state.currentIndex >= state.filtered.length) {
    state.currentIndex = Math.max(0, state.filtered.length - 1);
  }

  renderCurrent();
  populateCompareSelectors();
}

function renderCurrent() {
  const total = state.filtered.length;
  if (!total) {
    elements.counter.textContent = "0 / 0";
    elements.currentImage.removeAttribute("src");
    elements.titleText.textContent = "当前筛选无结果";
    elements.metaText.textContent = "请更改筛选条件";
    elements.studyText.textContent = "";
    elements.quickTags.innerHTML = "";
    return;
  }

  const item = state.filtered[state.currentIndex];
  elements.counter.textContent = `${state.currentIndex + 1} / ${total}`;
  elements.currentImage.src = item.image;
  elements.currentImage.alt = item.title;
  elements.titleText.textContent = item.title;
  elements.metaText.textContent = formatMeta(item);
  elements.studyText.textContent = item.studyDescription || "";
  renderTags(elements.quickTags, item.categories || []);
}

function stepCurrent(delta) {
  if (!state.filtered.length) return;
  state.currentIndex = (state.currentIndex + delta + state.filtered.length) % state.filtered.length;
  renderCurrent();
}

function openDetailForCurrent() {
  const item = state.filtered[state.currentIndex];
  if (!item) return;

  state.detailItemId = item.id;
  state.detailCategories = new Set(item.categories || []);

  elements.detailTitle.textContent = `${item.title} (${item.deckTitle} - Slide ${item.slideNumber})`;
  elements.detailImage.src = item.image;

  elements.yearInput.value = item.metadata.year || "";
  elements.periodInput.value = item.metadata.period || "";
  elements.authorInput.value = item.metadata.author || "";
  elements.productionPlaceInput.value = item.metadata.productionPlace || "";
  elements.regionInput.value = item.metadata.region || "";
  elements.styleInput.value = item.metadata.style || "";
  elements.materialInput.value = item.metadata.material || "";
  elements.historicalBackgroundZhInput.value = item.metadata.historicalBackgroundZh || "";
  elements.historicalBackgroundEnInput.value = item.metadata.historicalBackgroundEn || "";
  const sources = item.metadata.historicalBackgroundSources || [];
  elements.backgroundSourcesText.value = sources.join("\n");
  renderSourceLinks(elements.backgroundSourcesLinks, sources);

  elements.studyDescriptionText.value = item.studyDescription || "";
  elements.descriptionText.value = item.description || "";

  elements.newCategoryInput.value = "";
  renderTags(elements.detailCategories, [...state.detailCategories], true);
  elements.detailDialog.showModal();
}

function saveDetailChanges() {
  const item = getItemById(state.detailItemId);
  if (!item) return;

  const region = normalizeText(elements.regionInput.value);
  const productionPlace = normalizeText(elements.productionPlaceInput.value) || region;
  const author = applyAuthorFallback(elements.authorInput.value, productionPlace, region);

  const baseCategories = new Set(item.tags || []);
  const allCategories = [...state.detailCategories];
  const customCategories = allCategories.filter((c) => !baseCategories.has(c));

  state.overrides[item.id] = {
    year: normalizeText(elements.yearInput.value),
    period: normalizeText(elements.periodInput.value),
    author,
    productionPlace,
    region,
    style: normalizeText(elements.styleInput.value),
    material: normalizeText(elements.materialInput.value),
    historicalBackgroundZh: normalizeText(elements.historicalBackgroundZhInput.value),
    historicalBackgroundEn: normalizeText(elements.historicalBackgroundEnInput.value),
    categories: uniqueValues(customCategories),
  };

  writeJSONToStorage(STORAGE_OVERRIDES, state.overrides);
  rebuildFilters();
  applyFilters();
  renderSavedNotes();
  elements.detailDialog.close();
}

function addDetailCategory() {
  const newCategory = normalizeText(elements.newCategoryInput.value);
  if (!newCategory) return;
  state.detailCategories.add(newCategory);
  elements.newCategoryInput.value = "";
  renderTags(elements.detailCategories, [...state.detailCategories], true);
}

function populateCompareSelectors() {
  const options = allEffectiveItems();
  const previousA = elements.compareA.value;
  const previousB = elements.compareB.value;

  const markup = options
    .map(
      (item) =>
        `<option value="${escapeHtml(item.id)}">${escapeHtml(item.title.slice(0, 55))} [${escapeHtml(
          item.deckTitle
        )} S${item.slideNumber}]</option>`
    )
    .join("");

  elements.compareA.innerHTML = markup;
  elements.compareB.innerHTML = markup;

  if (options.length < 2) {
    elements.comparePreview.innerHTML = "";
    elements.compareHints.textContent = "数据不足，无法比较";
    elements.differenceNote.value = "";
    return;
  }

  const ids = options.map((o) => o.id);
  elements.compareA.value = ids.includes(previousA) ? previousA : ids[0];
  elements.compareB.value = ids.includes(previousB) ? previousB : ids[1] || ids[0];

  if (elements.compareA.value === elements.compareB.value && ids.length > 1) {
    elements.compareB.value = ids.find((id) => id !== elements.compareA.value) || ids[0];
  }

  renderCompare();
}

function shortText(text, maxLen = 90) {
  const value = normalizeText(text);
  if (!value) return "未标注";
  if (value.length <= maxLen) return value;
  return `${value.slice(0, maxLen)}...`;
}

function differenceLine(label, valueA, valueB) {
  const a = normalizeText(valueA) || "未标注";
  const b = normalizeText(valueB) || "未标注";
  if (a === b) return `${label}: 二者相同 (${a})`;
  return `${label}: A = ${a}；B = ${b}`;
}

function renderCompare() {
  const idA = elements.compareA.value;
  const idB = elements.compareB.value;
  const rawA = idA ? getItemById(idA) : null;
  const rawB = idB ? getItemById(idB) : null;
  if (!rawA || !rawB) return;

  const itemA = getEffectiveItem(rawA);
  const itemB = getEffectiveItem(rawB);

  elements.comparePreview.innerHTML = `
    <div class="compare-card">
      <img src="${escapeHtml(itemA.image)}" alt="A" />
      <div><strong>A</strong> ${escapeHtml(itemA.title.slice(0, 40))}</div>
      <div class="mini">${escapeHtml(itemA.deckTitle)} / S${itemA.slideNumber}</div>
    </div>
    <div class="compare-card">
      <img src="${escapeHtml(itemB.image)}" alt="B" />
      <div><strong>B</strong> ${escapeHtml(itemB.title.slice(0, 40))}</div>
      <div class="mini">${escapeHtml(itemB.deckTitle)} / S${itemB.slideNumber}</div>
    </div>
  `;

  const bgA = [itemA.metadata.historicalBackgroundZh, itemA.metadata.historicalBackgroundEn].filter(Boolean).join(" | ");
  const bgB = [itemB.metadata.historicalBackgroundZh, itemB.metadata.historicalBackgroundEn].filter(Boolean).join(" | ");

  const hints = [
    differenceLine("年份", itemA.metadata.year, itemB.metadata.year),
    differenceLine("时期", itemA.metadata.period, itemB.metadata.period),
    differenceLine("作者", itemA.metadata.author, itemB.metadata.author),
    differenceLine("生产地", itemA.metadata.productionPlace, itemB.metadata.productionPlace),
    differenceLine("地区", itemA.metadata.region, itemB.metadata.region),
    differenceLine("风格", itemA.metadata.style, itemB.metadata.style),
    differenceLine("材质", itemA.metadata.material, itemB.metadata.material),
    differenceLine("历史背景（中英）", shortText(bgA, 120), shortText(bgB, 120)),
    differenceLine("课程", itemA.deckTitle, itemB.deckTitle),
  ];

  const basePrompt =
    "写作建议：先写共性，再写差异。差异至少覆盖 1) 形式语言 2) 材质与工艺 3) 历史背景与社会语境。";

  elements.compareHints.innerHTML = `<ul>${hints.map((h) => `<li>${escapeHtml(h)}</li>`).join("")}</ul><p>${escapeHtml(
    basePrompt
  )}</p>`;

  const key = getPairKey(itemA.id, itemB.id);
  elements.differenceNote.value = state.compareNotes[key] || "";
}

function saveCompareNote() {
  const idA = elements.compareA.value;
  const idB = elements.compareB.value;
  if (!idA || !idB || idA === idB) return;

  const key = getPairKey(idA, idB);
  const value = normalizeText(elements.differenceNote.value);
  if (!value) {
    delete state.compareNotes[key];
  } else {
    state.compareNotes[key] = value;
  }

  writeJSONToStorage(STORAGE_NOTES, state.compareNotes);
  renderSavedNotes();
}

function renderSavedNotes() {
  const entries = Object.entries(state.compareNotes);
  elements.savedNotes.innerHTML = "";

  if (!entries.length) {
    const li = document.createElement("li");
    li.textContent = "暂无保存记录";
    li.style.cursor = "default";
    elements.savedNotes.appendChild(li);
    return;
  }

  entries
    .sort(([a], [b]) => a.localeCompare(b))
    .forEach(([pairKey, note]) => {
      const [idA, idB] = pairKey.split("__");
      const itemA = getItemById(idA);
      const itemB = getItemById(idB);
      if (!itemA || !itemB) return;

      const li = document.createElement("li");
      li.innerHTML = `<strong>${escapeHtml(itemA.title.slice(0, 18))}</strong> vs <strong>${escapeHtml(
        itemB.title.slice(0, 18)
      )}</strong><br /><span>${escapeHtml(note.slice(0, 54))}${note.length > 54 ? "..." : ""}</span>`;

      li.addEventListener("click", () => {
        elements.compareA.value = idA;
        elements.compareB.value = idB;
        renderCompare();
      });
      elements.savedNotes.appendChild(li);
    });
}

function bindEvents() {
  elements.searchInput.addEventListener("input", applyFilters);
  elements.deckFilter.addEventListener("change", applyFilters);
  elements.recordTypeFilter.addEventListener("change", applyFilters);
  elements.regionFilter.addEventListener("change", applyFilters);
  elements.styleFilter.addEventListener("change", applyFilters);
  elements.categoryFilter.addEventListener("change", applyFilters);

  elements.resetFiltersBtn.addEventListener("click", () => {
    elements.searchInput.value = "";
    elements.deckFilter.value = "全部课程";
    elements.recordTypeFilter.value = "全部类型";
    elements.regionFilter.value = "全部地区";
    elements.styleFilter.value = "全部风格";
    elements.categoryFilter.value = "全部分类";
    state.currentIndex = 0;
    applyFilters();
  });

  elements.prevBtn.addEventListener("click", () => stepCurrent(-1));
  elements.nextBtn.addEventListener("click", () => stepCurrent(1));
  elements.currentImage.addEventListener("click", openDetailForCurrent);

  elements.addCategoryBtn.addEventListener("click", addDetailCategory);
  elements.saveDetailBtn.addEventListener("click", saveDetailChanges);

  elements.compareA.addEventListener("change", renderCompare);
  elements.compareB.addEventListener("change", renderCompare);
  elements.saveNoteBtn.addEventListener("click", saveCompareNote);

  window.addEventListener("keydown", (event) => {
    if (elements.detailDialog.open) return;
    if (event.key === "ArrowLeft") stepCurrent(-1);
    if (event.key === "ArrowRight") stepCurrent(1);
  });
}

async function init() {
  state.overrides = readJSONFromStorage(STORAGE_OVERRIDES);
  state.compareNotes = readJSONFromStorage(STORAGE_NOTES);

  const response = await fetch(DATA_URL);
  if (!response.ok) {
    throw new Error(`无法加载数据: ${response.status}`);
  }

  const payload = await response.json();
  state.items = payload.items || [];
  state.filtered = allEffectiveItems();
  state.currentIndex = 0;

  rebuildFilters();
  bindEvents();
  applyFilters();
  renderSavedNotes();
}

init().catch((error) => {
  elements.titleText.textContent = "加载失败";
  elements.metaText.textContent = error.message;
  elements.studyText.textContent = "";
});
