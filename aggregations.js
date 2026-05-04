// Pure aggregations — take curated rows, return shaped data per route.
// Prices are already numeric (effective_price_czk) and bucketed (price_band)
// in out.c-competitor-monitoring.products_curated, so JS-side parsing is gone.

export const COMPETITORS = ["dreamlux", "matracezahubicku", "mpo_matrace", "prospanek"];
export const COMPETITOR_LABELS = {
  dreamlux: "Dreamlux",
  matracezahubicku: "Matrace za hubičku",
  mpo_matrace: "MPO Matrace",
  prospanek: "Pro Spánek",
};
export const SIZES = ["90x200", "160x200", "180x200"];

// Ordering and labels match the SQL `price_band` values (entry/mid/upper-mid/premium/luxury).
export const SEGMENTS = [
  { key: "entry",     label: "Entry (≤ 5 000 Kč)",            min: 0,     max: 5000 },
  { key: "mid",       label: "Střední (5 000–10 000 Kč)",     min: 5000,  max: 10000 },
  { key: "upper-mid", label: "Vyšší střední (10–20 000 Kč)",  min: 10000, max: 20000 },
  { key: "premium",   label: "Premium (20–40 000 Kč)",        min: 20000, max: 40000 },
  { key: "luxury",    label: "Luxus (> 40 000 Kč)",           min: 40000, max: Infinity },
];

const FACT_TYPES_ORDER = [
  "delivery_free_threshold_czk", "delivery_min_cost_czk", "delivery_to_room_available",
  "trial_period_days", "warranty_years", "warranty_extended_years", "returns_period_days",
  "financing_available", "showroom_available", "old_mattress_takeback", "personal_pickup_available",
];
const SCOPE_ORDER = ["sitewide", "category", "product", "shipping", "bundle"];

// Coerce values that may come back from Snowflake driver as strings on some types.
function num(v) {
  if (v == null) return null;
  const n = typeof v === "number" ? v : parseFloat(v);
  return Number.isFinite(n) ? n : null;
}

function dateStr(v) {
  if (!v) return null;
  if (typeof v === "string") return v.slice(0, 10);
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  return String(v).slice(0, 10);
}

// (competitor,source_url,size) repeats across snapshot days — keep latest snapshot per product.
function latestPerProduct(products) {
  const m = new Map();
  for (const r of products) {
    const k = `${r.competitor}::${r.source_url}::${r.size}`;
    const d = dateStr(r.fetched_at_date) ?? "";
    const prev = m.get(k);
    if (!prev || d > (dateStr(prev.fetched_at_date) ?? "")) m.set(k, r);
  }
  return [...m.values()];
}

function applySize(rows, size) {
  return size && size !== "all" ? rows.filter((r) => r.size === size) : rows;
}

export function portfolioMatrix(products, size) {
  const latest = applySize(latestPerProduct(products), size);
  const matrix = Object.fromEntries(SEGMENTS.map((s) => [s.key, Object.fromEntries(COMPETITORS.map((c) => [c, 0]))]));
  const totals = Object.fromEntries(COMPETITORS.map((c) => [c, 0]));
  for (const r of latest) {
    const seg = r.price_band;
    if (!seg || !COMPETITORS.includes(r.competitor) || !matrix[seg]) continue;
    matrix[seg][r.competitor]++;
    totals[r.competitor]++;
  }
  return { matrix, totals };
}

// All products in latest snapshot, sorted desc by effective price.
// Client-side filtering picks top N / bottom N / all, plus competitor + availability.
export function allProductsRanked(products, size) {
  const latest = applySize(latestPerProduct(products), size);
  return latest
    .map((r) => ({
      competitor: r.competitor,
      source_url: r.source_url,
      size: r.size,
      name: r.name || "",
      subcategory: r.subcategory || "",
      brand: r.brand_normalized || r.brand || "",
      price: num(r.effective_price_czk),
      is_available: (() => {
        const v = r.is_available;
        if (v === true || v === "true") return true;
        if (v === false || v === "false") return false;
        return null;
      })(),
    }))
    .filter((r) => r.price != null)
    .sort((a, b) => b.price - a.price);
}

// Histogram bins of effective price per competitor. 2 500 Kč bins up to 60 000.
export function priceHistogram(products, size) {
  const latest = applySize(latestPerProduct(products), size);
  const BIN = 2500;
  const MAX = 60000;
  const nBins = Math.ceil(MAX / BIN);
  const labels = [];
  for (let i = 0; i < nBins; i++) labels.push(`${(i * BIN) / 1000}k`);
  const series = Object.fromEntries(COMPETITORS.map((c) => [c, new Array(nBins).fill(0)]));
  for (const r of latest) {
    const p = num(r.effective_price_czk);
    if (p == null || !COMPETITORS.includes(r.competitor)) continue;
    const idx = Math.min(Math.floor(p / BIN), nBins - 1);
    series[r.competitor][idx]++;
  }
  return { labels, series };
}

export function servicesMatrix(serviceFacts) {
  // Latest fact_value per (fact_type, competitor).
  const latest = new Map();
  for (const r of serviceFacts) {
    const k = `${r.fact_type}::${r.competitor}`;
    const d = dateStr(r.fetched_at_date) ?? "";
    const prev = latest.get(k);
    if (!prev || d > (dateStr(prev.fetched_at_date) ?? "")) latest.set(k, r);
  }
  const factTypes = [...new Set([...FACT_TYPES_ORDER, ...serviceFacts.map((f) => f.fact_type)])];
  return factTypes.map((ft) => {
    const cells = {};
    for (const c of COMPETITORS) {
      const f = latest.get(`${ft}::${c}`);
      cells[c] = f ? { value: f.fact_value, quote: f.fact_quote } : null;
    }
    const ours = cells.prospanek?.value ?? null;
    const theirs = COMPETITORS.filter((c) => c !== "prospanek").map((c) => cells[c]?.value).filter((v) => v != null);
    const differs = ours != null && theirs.length > 0 && theirs.some((v) => v !== ours);
    const prospanekLeads = ours != null && theirs.length < COMPETITORS.length - 1;
    return { factType: ft, cells, differs, prospanekLeads };
  });
}

export function activePromos(promos, today = new Date()) {
  const todayIso = today.toISOString().slice(0, 10);
  const dedup = new Map();
  for (const p of promos) {
    const k = `${p.competitor}::${p.promo_text}`;
    const d = dateStr(p.fetched_at_date) ?? "";
    const prev = dedup.get(k);
    if (!prev || d > (dateStr(prev.fetched_at_date) ?? "")) dedup.set(k, p);
  }
  const filtered = [...dedup.values()].filter((p) => !p.ends_at || dateStr(p.ends_at) >= todayIso);
  filtered.sort((a, b) => {
    const sa = SCOPE_ORDER.indexOf(a.scope), sb = SCOPE_ORDER.indexOf(b.scope);
    if (sa !== sb) return (sa === -1 ? 99 : sa) - (sb === -1 ? 99 : sb);
    return String(dateStr(a.ends_at) || "9999-12-31").localeCompare(String(dateStr(b.ends_at) || "9999-12-31"));
  });
  return filtered;
}

export function promoSummary(promos) {
  const today = new Date().toISOString().slice(0, 10);
  const out = Object.fromEntries(COMPETITORS.map((c) => [c, { total: 0, scopes: {}, endingSoon: 0 }]));
  for (const p of promos) {
    if (!out[p.competitor]) continue;
    out[p.competitor].total++;
    const scope = p.scope || "unknown";
    out[p.competitor].scopes[scope] = (out[p.competitor].scopes[scope] || 0) + 1;
    const ends = dateStr(p.ends_at);
    if (ends) {
      const days = (new Date(ends) - new Date(today)) / (1000 * 60 * 60 * 24);
      if (days >= 0 && days <= 7) out[p.competitor].endingSoon++;
    }
  }
  return out;
}

export function notableInsights(products, serviceFacts, promos) {
  const items = [];
  const latest = latestPerProduct(products);
  const ourCount = latest.filter((r) => r.competitor === "prospanek").length;
  const others = COMPETITORS.filter((c) => c !== "prospanek");
  const otherCounts = others.map((c) => latest.filter((r) => r.competitor === c).length);
  const avgOther = otherCounts.length ? otherCounts.reduce((a, b) => a + b, 0) / otherCounts.length : 0;

  if (ourCount > 0 && avgOther > 0) {
    const ratio = ourCount / avgOther;
    if (ratio < 0.7) {
      items.push(`Pro Spánek sleduje ${ourCount} produktů oproti průměru konkurence ${Math.round(avgOther)} — nejužší sortiment.`);
    } else if (ratio > 1.3) {
      items.push(`Sortiment Pro Spánek (${ourCount}) je o ${Math.round((ratio - 1) * 100)} % nad průměrem konkurence.`);
    } else {
      items.push(`Pro Spánek sleduje ${ourCount} produktů, v souladu s průměrem konkurence ${Math.round(avgOther)}.`);
    }
  }

  const premium = SEGMENTS.find((s) => s.key === "premium");
  if (premium) {
    const counts = COMPETITORS.map((c) => ({
      c,
      n: latest.filter((r) => r.competitor === c && r.price_band === "premium").length,
    }));
    counts.sort((a, b) => b.n - a.n);
    if (counts[0].n > 0) {
      const label = COMPETITOR_LABELS[counts[0].c] || counts[0].c;
      items.push(`${label} vede v premium segmentu (20–40 tis. Kč) s ${counts[0].n} produkty.`);
    }
  }

  const allPriced = latest.map((r) => ({ ...r, _p: num(r.effective_price_czk) })).filter((r) => r._p != null);
  allPriced.sort((a, b) => b._p - a._p);
  if (allPriced[0]) {
    const top = allPriced[0];
    items.push(`Nejdražší sledovaný produkt: ${top.name} (${COMPETITOR_LABELS[top.competitor] || top.competitor}) za ${Math.round(top._p).toLocaleString("cs-CZ")} Kč.`);
  }

  const activeNow = activePromos(promos);
  if (activeNow.length > 0) {
    const sitewide = activeNow.filter((p) => p.scope === "sitewide").length;
    if (sitewide > 0) {
      items.push(`${sitewide} celosíťových akcí ${sitewide === 1 ? "běží" : "běží"} právě napříč konkurenty.`);
    } else {
      items.push(`${activeNow.length} aktivních akcí napříč konkurenty — žádná celosíťová.`);
    }
  }

  const sm = servicesMatrix(serviceFacts);
  const ourGaps = sm.filter((row) => row.cells.prospanek == null && Object.values(row.cells).some((c) => c != null)).length;
  if (ourGaps > 0) {
    items.push(`${ourGaps} ${ourGaps === 1 ? "služba ještě není" : "služeb ještě není"} zachycena pro Pro Spánek (mezera k doplnění).`);
  }

  return items.slice(0, 5);
}

export function segmentHeatmap(products) {
  const { matrix } = portfolioMatrix(products, "all");
  let max = 0;
  for (const seg of SEGMENTS) for (const c of COMPETITORS) max = Math.max(max, matrix[seg.key][c]);
  return { matrix, max };
}

export function kpis(products, serviceFacts, promos) {
  const latest = latestPerProduct(products);
  const ours = latest.filter((r) => r.competitor === "prospanek").length;
  const others = COMPETITORS.filter((c) => c !== "prospanek");
  const otherCounts = others.map((c) => latest.filter((r) => r.competitor === c).length);
  const avgOther = otherCounts.length ? Math.round(otherCounts.reduce((a, b) => a + b, 0) / otherCounts.length) : 0;
  const activeNow = activePromos(promos).length;
  const sm = servicesMatrix(serviceFacts);
  const leads = sm.filter((row) => row.prospanekLeads).length;
  return { ourProducts: ours, avgCompetitor: avgOther, activePromos: activeNow, servicesLed: leads };
}

export function lastFetchedAt(rows) {
  let latest = null;
  for (const r of rows) {
    const d = dateStr(r.fetched_at_date);
    if (d && (!latest || d > latest)) latest = d;
  }
  return latest;
}

// Latest day of events, ordered by severity within each type. Returns the full
// list (no capping) so client-side filters (competitor, direction) operate on
// the complete day's data. Severity-rank means high-impact rows surface first
// when no filter is applied.
export function newsOfTheDay(events) {
  const sevRank = { high: 1, medium: 2, low: 3 };
  const typeRank = { price_changed: 1, removed_product: 2, new_product: 3 };
  if (!events.length) return { date: null, items: [], totals: {} };

  const latest = events.reduce((acc, e) => {
    const d = dateStr(e.event_date);
    return d && (!acc || d > acc) ? d : acc;
  }, null);

  const todayEvents = events
    .filter((e) => dateStr(e.event_date) === latest)
    .map((e) => ({
      ...e,
      pct_change: e.pct_change == null || e.pct_change === "" ? null : num(e.pct_change),
      old_value: e.old_value == null || e.old_value === "" ? null : num(e.old_value),
      new_value: e.new_value == null || e.new_value === "" ? null : num(e.new_value),
    }))
    .sort((a, b) => {
      const t = (typeRank[a.event_type] ?? 9) - (typeRank[b.event_type] ?? 9);
      if (t !== 0) return t;
      return (sevRank[a.severity] ?? 9) - (sevRank[b.severity] ?? 9);
    });

  return {
    date: latest,
    items: todayEvents,
    totals: {
      price: todayEvents.filter((e) => e.event_type === "price_changed").length,
      new: todayEvents.filter((e) => e.event_type === "new_product").length,
      removed: todayEvents.filter((e) => e.event_type === "removed_product").length,
    },
  };
}

// ---------- Brand / producer aggregations ----------

// Normalize a brand value: strip ®/™, trim, uppercase. Empty / "—" / "?" → null.
// Mirrors the SQL `brand_normalized` so Tempur® and Tempur collapse to one key.
function normBrand(v) {
  if (v == null) return null;
  const t = String(v).replace(/[®™]/g, "").trim();
  if (!t) return null;
  if (t === "—" || t === "-" || t === "?" || t.toLowerCase() === "n/a") return null;
  return t.toUpperCase();
}

// Build canonical {NORM -> mostCommonOriginal} map. If a brand appears as
// "Tropico" 8x and "TROPICO" 2x, "Tropico" wins as the display string. The
// raw display is also ®/™-stripped so we never show "Tempur®" alongside "Tempur".
function brandDisplayMap(rows) {
  const counts = new Map();
  for (const r of rows) {
    const src = r.brand_normalized || r.brand;
    const n = normBrand(src);
    if (!n) continue;
    const raw = String(src).replace(/[®™]/g, "").trim();
    if (!counts.has(n)) counts.set(n, new Map());
    const inner = counts.get(n);
    inner.set(raw, (inner.get(raw) || 0) + 1);
  }
  const out = new Map();
  for (const [n, inner] of counts) {
    let best = null, bestN = -1;
    for (const [raw, c] of inner) {
      if (c > bestN) { best = raw; bestN = c; }
    }
    out.set(n, best);
  }
  return out;
}

const HOUSE_BRANDS = {
  // Competitor → set of normalised brand keys considered "house" / vlastní značka.
  mpo_matrace: new Set(["MPO"]),
  matracezahubicku: new Set(["FAVORIT", "MATRACEX"]),
  dreamlux: new Set(),
  prospanek: new Set(),
};

function isHouseBrand(competitor, normBrandKey) {
  return HOUSE_BRANDS[competitor]?.has(normBrandKey) || false;
}

function median(arr) {
  if (!arr || !arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

// Per-competitor brand counts on the latest snapshot.
// Returns: { brands: [{norm, display, total, perCompetitor:{c:count}, atProspanek, shopCount}], displayMap }
export function brandsAcrossCompetitors(products) {
  const latest = latestPerProduct(products);
  const display = brandDisplayMap(latest);
  const map = new Map(); // norm -> { perC: {c:n}, total: n }
  for (const r of latest) {
    const n = normBrand(r.brand_normalized || r.brand);
    if (!n) continue;
    if (!COMPETITORS.includes(r.competitor)) continue;
    if (!map.has(n)) map.set(n, { perC: Object.fromEntries(COMPETITORS.map((c) => [c, 0])), total: 0 });
    const e = map.get(n);
    e.perC[r.competitor]++;
    e.total++;
  }
  const brands = [...map.entries()].map(([n, e]) => {
    const shopCount = COMPETITORS.filter((c) => e.perC[c] > 0).length;
    return {
      norm: n,
      display: display.get(n) || n,
      total: e.total,
      perCompetitor: e.perC,
      atProspanek: e.perC.prospanek > 0,
      shopCount,
    };
  });
  brands.sort((a, b) => b.shopCount - a.shopCount || b.total - a.total || a.display.localeCompare(b.display));
  return { brands };
}

// Brand share at Pro Spánek (latest snapshot) — donut input.
// Returns [{norm, display, count, pct}], sorted desc by count.
export function brandsAtProspanek(products) {
  const latest = latestPerProduct(products).filter((r) => r.competitor === "prospanek");
  const display = brandDisplayMap(latest);
  const counts = new Map();
  for (const r of latest) {
    const n = normBrand(r.brand_normalized || r.brand);
    if (!n) continue;
    counts.set(n, (counts.get(n) || 0) + 1);
  }
  const total = [...counts.values()].reduce((a, b) => a + b, 0);
  const arr = [...counts.entries()].map(([n, c]) => ({
    norm: n,
    display: display.get(n) || n,
    count: c,
    pct: total ? (c / total) * 100 : 0,
  }));
  arr.sort((a, b) => b.count - a.count || a.display.localeCompare(b.display));
  return { rows: arr, total };
}

// Same-brand price comparison across shops, restricted to size 180×200 for fairness.
// Only brands present at ≥2 shops included.
// Returns [{norm, display, medianByCompetitor:{c:median|null}, prospanekUndercut:{c:bool}}]
export function brandPriceComparison(products, size = "180x200") {
  const latest = latestPerProduct(products).filter((r) => r.size === size);
  const display = brandDisplayMap(latest);
  const groups = new Map(); // norm -> {c -> [prices]}
  for (const r of latest) {
    const n = normBrand(r.brand_normalized || r.brand);
    if (!n) continue;
    if (!COMPETITORS.includes(r.competitor)) continue;
    const p = num(r.effective_price_czk);
    if (p == null) continue;
    if (!groups.has(n)) groups.set(n, Object.fromEntries(COMPETITORS.map((c) => [c, []])));
    groups.get(n)[r.competitor].push(p);
  }
  const out = [];
  for (const [n, perC] of groups) {
    const medians = Object.fromEntries(COMPETITORS.map((c) => [c, median(perC[c])]));
    const present = COMPETITORS.filter((c) => medians[c] != null);
    if (present.length < 2) continue;
    const ours = medians.prospanek;
    const undercut = Object.fromEntries(COMPETITORS.map((c) => [
      c,
      c !== "prospanek" && ours != null && medians[c] != null && medians[c] < ours,
    ]));
    out.push({
      norm: n,
      display: display.get(n) || n,
      medianByCompetitor: medians,
      undercut,
      shopCount: present.length,
    });
  }
  out.sort((a, b) => b.shopCount - a.shopCount || a.display.localeCompare(b.display));
  return out;
}

// Discount share by brand (across all shops + sizes in latest snapshot).
// "On sale" = sale_price_czk_num present & < regular_price_czk_num, OR discount_pct_num > 0.
// Returns [{norm, display, total, onSale, salePct, avgDiscount}], filtered to brands with ≥3 products.
export function brandDiscounts(products, minProducts = 3) {
  const latest = latestPerProduct(products);
  const display = brandDisplayMap(latest);
  const stats = new Map(); // norm -> {total, onSale, discountSum, discountN}
  for (const r of latest) {
    const n = normBrand(r.brand_normalized || r.brand);
    if (!n) continue;
    if (!stats.has(n)) stats.set(n, { total: 0, onSale: 0, discountSum: 0, discountN: 0 });
    const e = stats.get(n);
    e.total++;
    const sale = num(r.sale_price_czk_num);
    const reg = num(r.regular_price_czk_num);
    const pct = num(r.discount_pct_num);
    const onSale = (sale != null && reg != null && sale < reg) || (pct != null && pct > 0);
    if (onSale) {
      e.onSale++;
      if (pct != null && pct > 0) {
        e.discountSum += pct;
        e.discountN++;
      }
    }
  }
  const arr = [...stats.entries()]
    .filter(([, e]) => e.total >= minProducts)
    .map(([n, e]) => ({
      norm: n,
      display: display.get(n) || n,
      total: e.total,
      onSale: e.onSale,
      salePct: e.total ? (e.onSale / e.total) * 100 : 0,
      avgDiscount: e.discountN ? e.discountSum / e.discountN : null,
    }));
  arr.sort((a, b) => b.salePct - a.salePct || b.total - a.total || a.display.localeCompare(b.display));
  return arr;
}

// Top-line KPIs for the producers page.
export function brandKpis(products) {
  const latest = latestPerProduct(products);
  const display = brandDisplayMap(latest);

  // Producers tracked at Pro Spánek
  const ourBrands = new Set();
  for (const r of latest.filter((r) => r.competitor === "prospanek")) {
    const n = normBrand(r.brand_normalized || r.brand);
    if (n) ourBrands.add(n);
  }

  // Producers shared across ≥2 shops.
  const presence = new Map();
  for (const r of latest) {
    const n = normBrand(r.brand_normalized || r.brand);
    if (!n || !COMPETITORS.includes(r.competitor)) continue;
    if (!presence.has(n)) presence.set(n, new Set());
    presence.get(n).add(r.competitor);
  }
  const sharedBrands = [...presence.values()].filter((s) => s.size >= 2).length;
  const sharedWithProspanek = [...presence.entries()]
    .filter(([, s]) => s.has("prospanek") && s.size >= 2)
    .length;

  // House-brand share among non-prospanek shops.
  const otherShops = COMPETITORS.filter((c) => c !== "prospanek");
  let totalOther = 0, houseOther = 0;
  for (const r of latest) {
    if (!otherShops.includes(r.competitor)) continue;
    const n = normBrand(r.brand_normalized || r.brand);
    if (!n) continue;
    totalOther++;
    if (isHouseBrand(r.competitor, n)) houseOther++;
  }
  const housePct = totalOther ? (houseOther / totalOther) * 100 : 0;

  // Average discount across brands (only products on sale with discount_pct_num > 0).
  const pcts = latest
    .map((r) => num(r.discount_pct_num))
    .filter((p) => p != null && p > 0);
  const avgDiscount = pcts.length ? pcts.reduce((a, b) => a + b, 0) / pcts.length : null;

  return {
    brandsAtProspanek: ourBrands.size,
    sharedBrands,
    sharedWithProspanek,
    housePct,
    avgDiscount,
    displayMap: Object.fromEntries(display),
  };
}

// Median effective price per (fetched_at_date, competitor) for a given size.
// Returns { dates: [...], series: { competitor: [median per date] } } for line charts.
export function priceTrend(products, size = "180x200") {
  const filtered = products.filter((r) => r.size === size && num(r.effective_price_czk) != null);
  const dateSet = new Set();
  const byKey = new Map();
  for (const r of filtered) {
    const d = dateStr(r.fetched_at_date);
    if (!d || !COMPETITORS.includes(r.competitor)) continue;
    dateSet.add(d);
    const k = `${d}::${r.competitor}`;
    const arr = byKey.get(k) ?? [];
    arr.push(num(r.effective_price_czk));
    byKey.set(k, arr);
  }
  const dates = [...dateSet].sort();
  const series = Object.fromEntries(COMPETITORS.map((c) => [c, dates.map((d) => {
    const arr = byKey.get(`${d}::${c}`);
    if (!arr || !arr.length) return null;
    const sorted = [...arr].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  })]));
  return { dates, series };
}
