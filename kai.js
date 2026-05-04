// In-app KAI assistant. Anthropic streaming via SSE.
// Reads ANTHROPIC_API_KEY from env (set as #anthropic_api_key user_property
// on the Keboola Data App config). Uses Sonnet 4.6 — same model as the
// extractor. The "semantic layer" is a compact textual snapshot of the
// dashboard's current state, injected once into the system prompt so the
// model can answer factual questions without round-tripping through the data.

import Anthropic from "@anthropic-ai/sdk";
import {
  COMPETITORS, COMPETITOR_LABELS,
  kpis, allProductsRanked,
  brandKpis, brandsAtProspanek, brandsAcrossCompetitors,
  brandPriceComparison, brandDiscounts,
  newsOfTheDay, priceTrend, lastFetchedAt,
  segmentHeatmap, SEGMENTS, activePromos, promoSummary,
} from "./aggregations.js";

const MODEL = "claude-sonnet-4-6";
const MAX_TOKENS = 1500;

function fmtNum(n) {
  if (n == null || !Number.isFinite(Number(n))) return "—";
  return Math.round(Number(n)).toLocaleString("cs-CZ");
}

function fmtPct(n) {
  if (n == null || !Number.isFinite(Number(n))) return "—";
  return `${Number(n).toFixed(1)} %`;
}

// Compact context — under ~1k tokens, structured for a model to scan.
export function buildContext(d) {
  const products = d.products_curated || [];
  const events = d.product_events || [];
  const promos = d.promos || [];
  const facts = d.service_facts || [];

  const k = kpis(products, facts, promos);
  const bk = brandKpis(products);
  const ourBrands = brandsAtProspanek(products);
  const cross = brandsAcrossCompetitors(products);
  const compare = brandPriceComparison(products, "180x200");
  const discounts = brandDiscounts(products, 3);
  const heat = segmentHeatmap(products);
  const news = newsOfTheDay(events);
  const trend = priceTrend(products, "180x200");
  const promoSum = promoSummary(activePromos(promos));

  const lines = [];
  lines.push(`# Aktuální stav Pro Spánek competitor monitoring`);
  lines.push(`Poslední aktualizace: ${lastFetchedAt(products) || "?"}`);
  lines.push(`Sledované weby: ${COMPETITORS.map((c) => COMPETITOR_LABELS[c]).join(", ")}`);
  lines.push(`Velikosti matrací: 90×200, 160×200, 180×200`);
  lines.push("");

  lines.push(`## Hlavní KPI`);
  lines.push(`- Produkty Pro Spánek: ${k.ourProducts}`);
  lines.push(`- Průměr konkurenta: ${k.avgCompetitor} produktů`);
  lines.push(`- Aktivní akce napříč konkurencí: ${k.activePromos}`);
  lines.push(`- Služby kde Pro Spánek vede: ${k.servicesLed}`);
  lines.push("");

  lines.push(`## Producenti (značky)`);
  lines.push(`- Producentů sledovaných u Pro Spánku: ${bk.brandsAtProspanek}`);
  lines.push(`- Producentů sdílených s konkurencí: ${bk.sharedWithProspanek}`);
  lines.push(`- Podíl vlastních značek u konkurence: ${fmtPct(bk.housePct)}`);
  lines.push(`- Průměrná reálná sleva (mimo strukturální): ${fmtPct(bk.avgDiscount)}`);
  lines.push("");

  lines.push(`### Mix producentů u Pro Spánku (top 8)`);
  for (const r of ourBrands.rows.slice(0, 8)) {
    lines.push(`- ${r.display}: ${r.count} produktů (${fmtPct(r.pct)})`);
  }
  lines.push("");

  lines.push(`### Producenti u 2+ obchodů (top 10)`);
  for (const b of cross.brands.slice(0, 10)) {
    const shops = COMPETITORS
      .filter((c) => (b.perCompetitor[c] || 0) > 0)
      .map((c) => `${COMPETITOR_LABELS[c]} (${b.perCompetitor[c]})`)
      .join(", ");
    lines.push(`- ${b.display}: ${b.shopCount} obchodů — ${shops}`);
  }
  lines.push("");

  if (compare.length) {
    lines.push(`### Stejný producent — srovnání mediánových cen 180×200 (Kč)`);
    for (const c of compare.slice(0, 8)) {
      const cells = COMPETITORS
        .filter((s) => c.medianByCompetitor && c.medianByCompetitor[s] != null)
        .map((s) => `${COMPETITOR_LABELS[s]}: ${fmtNum(c.medianByCompetitor[s])}`)
        .join(" · ");
      const undercut = c.undercut ? " ⚠️ konkurence podbízí" : "";
      lines.push(`- ${c.display}: ${cells}${undercut}`);
    }
    lines.push("");
  }

  if (discounts.length) {
    const real = discounts.filter((d) => !d.structural).slice(0, 8);
    if (real.length) {
      lines.push(`### Sleva podle značky (reálné akce, ≥5 %, ne strukturální)`);
      for (const r of real) {
        lines.push(`- ${r.display}: ${r.onSale}/${r.total} ve slevě (${fmtPct(r.salePct)}), průměr ${fmtPct(r.avgDiscount)}`);
      }
      const struct = discounts.filter((d) => d.structural).map((d) => d.display);
      if (struct.length) {
        lines.push(`(Strukturální MSRP — vždy "ve slevě", ignorováno: ${struct.join(", ")})`);
      }
      lines.push("");
    }
  }

  lines.push(`## Cenové segmenty (počet produktů, poslední snapshot)`);
  for (const s of SEGMENTS) {
    const row = COMPETITORS.map((c) => `${COMPETITOR_LABELS[c]}: ${heat.matrix[s.key][c]}`).join(" · ");
    lines.push(`- ${s.label} → ${row}`);
  }
  lines.push("");

  if (trend.dates.length >= 2) {
    lines.push(`## Vývoj mediánové ceny 180×200 (Kč)`);
    for (const c of COMPETITORS) {
      const last = trend.series[c].filter((v) => v != null).slice(-2);
      if (last.length === 2) {
        const delta = last[1] - last[0];
        const arrow = delta > 0 ? "↑" : delta < 0 ? "↓" : "→";
        lines.push(`- ${COMPETITOR_LABELS[c]}: ${fmtNum(last[0])} → ${fmtNum(last[1])} ${arrow}`);
      }
    }
    lines.push("");
  }

  if (news.totals?.price) {
    lines.push(`## Novinky dne (denní změny vs. předchozí snapshot)`);
    lines.push(`- Změn cen: ${news.totals.price} · Nových: ${news.totals.new || 0} · Odebraných: ${news.totals.removed || 0}`);
    const top = news.items.filter((e) => e.event_type === "price_changed").slice(0, 5);
    if (top.length) {
      lines.push(`Top změny cen:`);
      for (const e of top) {
        const dir = Number(e.pct_change) < 0 ? "zlevnění" : "zdražení";
        lines.push(`- ${COMPETITOR_LABELS[e.competitor]} · ${e.name?.slice(0, 60)} · ${e.old_value} → ${e.new_value} Kč (${dir} ${fmtPct(Math.abs(Number(e.pct_change)))})`);
      }
    }
    lines.push("");
  }

  if (promos.length) {
    lines.push(`## Akce u konkurence`);
    for (const c of COMPETITORS) {
      const s = promoSum[c] || { total: 0, endingSoon: 0 };
      if (!s.total) continue;
      lines.push(`- ${COMPETITOR_LABELS[c]}: ${s.total} aktivních${s.endingSoon ? ` (${s.endingSoon} končí do 7 dnů)` : ""}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

const SYSTEM_PROMPT_PREFIX = `Jsi KAI, interní AI asistent pro Pro Spánek competitor monitoring dashboard.

Pravidla:
- Odpovídej česky, pokud uživatel nepíše v jiném jazyce.
- Stručně a věcně. Když je odpověď v datech níže, opři se o ně. Když není, řekni to a navrhni, kde se podívat.
- Pro Spánek je "my"; ostatní weby jsou konkurence. Cílem je strategická konkurenční inteligence.
- Pokud uživatel chce porovnání cen napříč obchody, používej rozměr 180×200 jako referenční (data v kontextu).
- Strukturální slevy (Tempur, Sealy, Serta apod. — všechny produkty s identickou slevou) jsou trvalé MSRP označení, ne aktivní akce. Nezaměňuj.
- Pokud uživatel chce konkrétní řádek z dat, navrhni ať otevře příslušnou stránku dashboardu (Sortiment / Producenti / Akce / Trendy).

DATA SNAPSHOT:
`;

export function buildSystemPrompt(context) {
  return SYSTEM_PROMPT_PREFIX + context;
}

export function getClient() {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) {
    throw new Error("ANTHROPIC_API_KEY není nastaven. Přidej ho jako #anthropic_api_key user_property na Data App konfiguraci a restartuj.");
  }
  return new Anthropic({ apiKey: key });
}

// Stream a chat turn to the response as SSE. messages = [{role, content}, ...].
export async function streamChat(res, messages, contextStr) {
  const client = getClient();
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders?.();

  const send = (obj) => res.write(`data: ${JSON.stringify(obj)}\n\n`);

  try {
    const stream = await client.messages.stream({
      model: MODEL,
      max_tokens: MAX_TOKENS,
      system: buildSystemPrompt(contextStr),
      messages,
    });
    for await (const event of stream) {
      if (event.type === "content_block_delta" && event.delta?.type === "text_delta") {
        send({ type: "delta", text: event.delta.text });
      }
    }
    const final = await stream.finalMessage();
    send({ type: "done", usage: final.usage });
  } catch (err) {
    console.error("[kai] error:", err);
    send({ type: "error", message: err.message || "Chyba při komunikaci s KAI." });
  } finally {
    res.end();
  }
}
