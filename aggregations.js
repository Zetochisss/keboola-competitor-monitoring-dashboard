// Pure aggregations — take raw rows, return shaped data per route.

export const COMPETITORS = ["dreamlux", "matracezahubicku", "mpo_matrace", "prospanek"];
export const COMPETITOR_LABELS = {
  dreamlux: "Dreamlux",
  matracezahubicku: "Matrace za hubičku",
  mpo_matrace: "MPO Matrace",
  prospanek: "Pro Spánek",
};
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

export function topExpensivePerCompetitor(products, size, n = 10) {
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

// Histogram bins of effective price per competitor. Returns array of bin centers
// and per-competitor counts. Bins are 2500 CZK wide up to 60 000.
export function priceHistogram(products, size) {
  const latest = applySize(latestPerProduct(products), size);
  const BIN = 2500;
  const MAX = 60000;
  const nBins = Math.ceil(MAX / BIN);
  const labels = [];
  for (let i = 0; i < nBins; i++) labels.push(`${(i * BIN) / 1000}k`);
  const series = Object.fromEntries(COMPETITORS.map((c) => [c, new Array(nBins).fill(0)]));
  for (const r of latest) {
    const p = effectivePrice(r);
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
    // prospanek-leads = we have a value, at least one competitor doesn't.
    const prospanekLeads = ours != null && theirs.length < COMPETITORS.length - 1;
    return { factType: ft, cells, differs, prospanekLeads };
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

// Per-competitor promo summary: total + scope breakdown.
export function promoSummary(promos) {
  const today = new Date().toISOString().slice(0, 10);
  const out = Object.fromEntries(COMPETITORS.map((c) => [c, { total: 0, scopes: {}, endingSoon: 0 }]));
  for (const p of promos) {
    if (!out[p.competitor]) continue;
    out[p.competitor].total++;
    const scope = p.scope || "unknown";
    out[p.competitor].scopes[scope] = (out[p.competitor].scopes[scope] || 0) + 1;
    if (p.ends_at) {
      const days = (new Date(p.ends_at) - new Date(today)) / (1000 * 60 * 60 * 24);
      if (days >= 0 && days <= 7) out[p.competitor].endingSoon++;
    }
  }
  return out;
}

// "What's notable today" — hand-derived. Returns 3-5 short text items.
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
      items.push(`Pro Spánek tracks ${ourCount} products vs ${Math.round(avgOther)} avg competitor — narrowest portfolio.`);
    } else if (ratio > 1.3) {
      items.push(`Pro Spánek's portfolio (${ourCount}) is ${Math.round((ratio - 1) * 100)}% above competitor average.`);
    } else {
      items.push(`Pro Spánek tracks ${ourCount} products, in line with competitor average of ${Math.round(avgOther)}.`);
    }
  }

  // Premium-segment leader.
  const premium = SEGMENTS.find((s) => s.key === "premium");
  if (premium) {
    const counts = COMPETITORS.map((c) => ({
      c,
      n: latest.filter((r) => r.competitor === c && (() => { const p = effectivePrice(r); return p >= premium.min && p < premium.max; })()).length,
    }));
    counts.sort((a, b) => b.n - a.n);
    if (counts[0].n > 0) {
      const label = COMPETITOR_LABELS[counts[0].c] || counts[0].c;
      items.push(`${label} leads the premium segment (20–40k Kč) with ${counts[0].n} products.`);
    }
  }

  // Most expensive product overall.
  const allPriced = latest.map((r) => ({ ...r, _p: effectivePrice(r) })).filter((r) => r._p != null);
  allPriced.sort((a, b) => b._p - a._p);
  if (allPriced[0]) {
    const top = allPriced[0];
    items.push(`Most expensive product tracked: ${top.name} (${COMPETITOR_LABELS[top.competitor] || top.competitor}) at ${Math.round(top._p).toLocaleString("cs-CZ")} Kč.`);
  }

  // Active promo count.
  const activeNow = activePromos(promos);
  if (activeNow.length > 0) {
    const sitewide = activeNow.filter((p) => p.scope === "sitewide").length;
    if (sitewide > 0) {
      items.push(`${sitewide} sitewide promotion${sitewide === 1 ? "" : "s"} running across competitors right now.`);
    } else {
      items.push(`${activeNow.length} active promotions across competitors — none sitewide.`);
    }
  }

  // Service-fact gaps.
  const sm = servicesMatrix(serviceFacts);
  const ourGaps = sm.filter((row) => row.cells.prospanek == null && Object.values(row.cells).some((c) => c != null)).length;
  if (ourGaps > 0) {
    items.push(`${ourGaps} service fact${ourGaps === 1 ? "" : "s"} not yet captured for Pro Spánek (gap to fill).`);
  }

  return items.slice(0, 5);
}

// Heatmap: competitor x segment, product counts. Matches portfolioMatrix shape but
// returns rows oriented by competitor and includes max for color scaling.
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
  // services we lead on: facts where prospanek has a value, others (some) don't.
  const sm = servicesMatrix(serviceFacts);
  const leads = sm.filter((row) => row.prospanekLeads).length;
  return { ourProducts: ours, avgCompetitor: avgOther, activePromos: activeNow, servicesLed: leads };
}

export function lastFetchedAt(rows) {
  let latest = null;
  for (const r of rows) if (r.fetched_at_date && (!latest || r.fetched_at_date > latest)) latest = r.fetched_at_date;
  return latest;
}
