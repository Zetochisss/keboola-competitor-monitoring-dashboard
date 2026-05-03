// Pure aggregations — take raw rows, return shaped data per route.

export const COMPETITORS = ["dreamlux", "matracezahubicku", "mpo_matrace", "prospanek"];
export const SIZES = ["90x200", "160x200", "180x200"];

// Rough Czech mattress market thresholds (CZK). Tune as needed.
export const SEGMENTS = [
  { key: "entry",     label: "Entry (≤ 5 000)",            min: 0,     max: 5000 },
  { key: "mid",       label: "Mid (5 000–10 000)",         min: 5000,  max: 10000 },
  { key: "upper_mid", label: "Upper-mid (10 000–20 000)",  min: 10000, max: 20000 },
  { key: "premium",   label: "Premium (20 000–40 000)",    min: 20000, max: 40000 },
  { key: "luxury",    label: "Luxury (> 40 000)",          min: 40000, max: Infinity },
];

const FACT_TYPES_ORDER = [
  "delivery_free_threshold_czk", "delivery_min_cost_czk", "delivery_to_room_available",
  "trial_period_days", "warranty_years", "warranty_extended_years", "returns_period_days",
  "financing_available", "showroom_available", "old_mattress_takeback", "personal_pickup_available",
];
const SCOPE_ORDER = ["sitewide", "category", "product", "shipping", "bundle"];

function effectivePrice(r) {
  const sale = parseFloat(r.sale_price_czk);
  const reg = parseFloat(r.regular_price_czk);
  if (Number.isFinite(sale) && sale > 0) return sale;
  if (Number.isFinite(reg) && reg > 0) return reg;
  return null;
}

function segmentFor(price) {
  if (price == null) return null;
  for (const s of SEGMENTS) if (price >= s.min && price < s.max) return s.key;
  return null;
}

// (competitor,source_url,size) repeats daily — keep latest snapshot per product.
function latestPerProduct(products) {
  const m = new Map();
  for (const r of products) {
    const k = `${r.competitor}::${r.source_url}::${r.size}`;
    const prev = m.get(k);
    if (!prev || (r.fetched_at_date ?? "") > (prev.fetched_at_date ?? "")) m.set(k, r);
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
    const seg = segmentFor(effectivePrice(r));
    if (!seg || !COMPETITORS.includes(r.competitor)) continue;
    matrix[seg][r.competitor]++;
    totals[r.competitor]++;
  }
  return { matrix, totals };
}

export function topExpensivePerCompetitor(products, size, n = 5) {
  const latest = applySize(latestPerProduct(products), size);
  const out = {};
  for (const c of COMPETITORS) {
    out[c] = latest
      .filter((r) => r.competitor === c)
      .map((r) => ({ ...r, _price: effectivePrice(r) }))
      .filter((r) => r._price != null)
      .sort((a, b) => b._price - a._price)
      .slice(0, n);
  }
  return out;
}

export function servicesMatrix(serviceFacts) {
  // Latest fact_value per (fact_type, competitor).
  const latest = new Map();
  for (const r of serviceFacts) {
    const k = `${r.fact_type}::${r.competitor}`;
    const prev = latest.get(k);
    if (!prev || (r.fetched_at_date ?? "") > (prev.fetched_at_date ?? "")) latest.set(k, r);
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
    return { factType: ft, cells, differs };
  });
}

export function activePromos(promos, today = new Date()) {
  const todayIso = today.toISOString().slice(0, 10);
  // Promos re-extracted daily — dedupe by (competitor, promo_text), keep latest.
  const dedup = new Map();
  for (const p of promos) {
    const k = `${p.competitor}::${p.promo_text}`;
    const prev = dedup.get(k);
    if (!prev || (p.fetched_at_date ?? "") > (prev.fetched_at_date ?? "")) dedup.set(k, p);
  }
  const filtered = [...dedup.values()].filter((p) => !p.ends_at || String(p.ends_at).slice(0, 10) >= todayIso);
  filtered.sort((a, b) => {
    const sa = SCOPE_ORDER.indexOf(a.scope), sb = SCOPE_ORDER.indexOf(b.scope);
    if (sa !== sb) return (sa === -1 ? 99 : sa) - (sb === -1 ? 99 : sb);
    return String(a.ends_at || "9999-12-31").localeCompare(String(b.ends_at || "9999-12-31"));
  });
  return filtered;
}

export function lastFetchedAt(rows) {
  let latest = null;
  for (const r of rows) if (r.fetched_at_date && (!latest || r.fetched_at_date > latest)) latest = r.fetched_at_date;
  return latest;
}
