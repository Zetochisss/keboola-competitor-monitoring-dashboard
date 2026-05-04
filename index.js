// Internal dashboard for Czech mattress competitor monitoring.
// Reads curated tables from out.c-competitor-monitoring via Snowflake
// (products_curated, product_events) plus raw service_facts and promos
// (curation deferred until extraction quality improves).

import express from "express";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as data from "./data.js";

// Keboola Data App config bootstrap. The Custom Python component pattern
// (`CommonInterface()` + `os.environ[...] = params["#anthropic_api_key"]`)
// has no JS equivalent, so we read /data/config.json directly. Keboola
// decrypts encrypted (`#`-prefixed) parameters before writing this file.
// Falls through silently when the file isn't there (local dev).
function bootstrapKbcSecrets() {
  const candidates = [
    process.env.KBC_DATADIR ? `${process.env.KBC_DATADIR}/config.json` : null,
    "/data/config.json",
  ].filter(Boolean);
  for (const p of candidates) {
    try {
      if (!fs.existsSync(p)) continue;
      const cfg = JSON.parse(fs.readFileSync(p, "utf-8"));
      const params = cfg?.parameters || {};
      const key = params["#anthropic_api_key"] ?? params.anthropic_api_key;
      if (key && !process.env.ANTHROPIC_API_KEY) {
        process.env.ANTHROPIC_API_KEY = String(key);
        console.log(`[bootstrap] ANTHROPIC_API_KEY loaded from ${p}`);
      }
      return;
    } catch (e) {
      console.warn(`[bootstrap] could not read ${p}:`, e.message);
    }
  }
  console.log("[bootstrap] no Keboola config.json found; relying on existing env vars");
}
bootstrapKbcSecrets();
import {
  portfolioMatrix, allProductsRanked, servicesMatrix, activePromos,
  priceHistogram, promoSummary, notableInsights, segmentHeatmap, kpis,
  lastFetchedAt, newsOfTheDay, priceTrend,
  brandsAcrossCompetitors, brandsAtProspanek, brandPriceComparison,
  brandDiscounts, brandKpis,
  COMPETITORS, COMPETITOR_LABELS, SIZES, SEGMENTS,
} from "./aggregations.js";
import { buildContext, streamChat } from "./kai.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = parseInt(process.env.PORT || "3000", 10);

const app = express();
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.static(path.join(__dirname, "public")));
app.use(express.json({ limit: "256kb" }));

let mode = "live";

let cache = { ts: 0, payload: null };
const CACHE_TTL_MS = 5 * 60 * 1000;

async function getData() {
  if (cache.payload && Date.now() - cache.ts < CACHE_TTL_MS) return cache.payload;
  cache = { ts: Date.now(), payload: await data.fetchAll() };
  return cache.payload;
}

function todayCs() {
  return new Date().toLocaleDateString("cs-CZ", { day: "numeric", month: "long", year: "numeric" });
}

function fmtDate(iso) {
  if (!iso) return null;
  const s = typeof iso === "string" ? iso.slice(0, 10) : new Date(iso).toISOString().slice(0, 10);
  const [y, m, d] = s.split("-").map(Number);
  return new Date(y, m - 1, d).toLocaleDateString("cs-CZ", { day: "numeric", month: "long", year: "numeric" });
}

function commonLocals(d) {
  return {
    mode,
    competitors: COMPETITORS,
    competitorLabels: COMPETITOR_LABELS,
    sizes: SIZES,
    segments: SEGMENTS,
    today: todayCs(),
    lastFetched: fmtDate(lastFetchedAt([
      ...(d.products_curated || []), ...(d.service_facts || []), ...(d.promos || []),
    ])),
  };
}

app.get("/", async (_req, res, next) => {
  try {
    const d = await getData();
    res.render("overview", {
      ...commonLocals(d),
      active: "overview",
      title: "Přehled",
      kpi: kpis(d.products_curated, d.service_facts, d.promos),
      heatmap: segmentHeatmap(d.products_curated),
      insights: notableInsights(d.products_curated, d.service_facts, d.promos),
    });
  } catch (err) { next(err); }
});

app.get("/portfolio", async (req, res, next) => {
  try {
    const size = (req.query.size || "all").toString();
    const d = await getData();
    const { matrix, totals } = portfolioMatrix(d.products_curated, size);
    const ranked = allProductsRanked(d.products_curated, size);
    const hist = priceHistogram(d.products_curated, size);
    res.render("portfolio", {
      ...commonLocals(d), active: "portfolio", title: "Sortiment",
      size, matrix, totals, ranked, hist,
    });
  } catch (err) { next(err); }
});

app.get("/services", async (_req, res, next) => {
  try {
    const d = await getData();
    res.render("services", {
      ...commonLocals(d), active: "services", title: "Služby",
      rows: servicesMatrix(d.service_facts),
    });
  } catch (err) { next(err); }
});

app.get("/promos", async (_req, res, next) => {
  try {
    const d = await getData();
    res.render("promos", {
      ...commonLocals(d), active: "promos", title: "Akce",
      promos: activePromos(d.promos),
      summary: promoSummary(activePromos(d.promos)),
    });
  } catch (err) { next(err); }
});

app.get("/producers", async (_req, res, next) => {
  try {
    const d = await getData();
    const products = d.products_curated || [];
    res.render("producers", {
      ...commonLocals(d), active: "producers", title: "Producenti",
      kpi: brandKpis(products),
      mix: brandsAtProspanek(products),
      across: brandsAcrossCompetitors(products),
      compare: brandPriceComparison(products, "180x200"),
      discounts: brandDiscounts(products, 3),
    });
  } catch (err) { next(err); }
});

app.get("/trends", async (_req, res, next) => {
  try {
    const d = await getData();
    const news = newsOfTheDay(d.product_events || []);
    const trend = priceTrend(d.products_curated, "180x200");
    res.render("trends", {
      ...commonLocals(d), active: "trends", title: "Trendy",
      news,
      newsDateLabel: news.date ? fmtDate(news.date) : null,
      trend,
    });
  } catch (err) { next(err); }
});

// In-app KAI assistant. Accepts {messages: [{role, content}, ...]}; streams
// SSE deltas back. System prompt is rebuilt per turn from cached dashboard data
// so the model always answers against the current snapshot.
app.post("/api/chat", async (req, res) => {
  try {
    const messages = Array.isArray(req.body?.messages) ? req.body.messages : null;
    if (!messages || !messages.length) {
      res.status(400).json({ error: "Missing messages." });
      return;
    }
    const cleaned = messages
      .filter((m) => m && (m.role === "user" || m.role === "assistant") && typeof m.content === "string")
      .map((m) => ({ role: m.role, content: m.content.slice(0, 4000) }))
      .slice(-12);
    if (!cleaned.length || cleaned[cleaned.length - 1].role !== "user") {
      res.status(400).json({ error: "Last message must be from user." });
      return;
    }
    const d = await getData();
    const context = buildContext(d);
    await streamChat(res, cleaned, context);
  } catch (err) {
    console.error("[api/chat] error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: err.message || "Server error." });
    } else {
      res.write(`data: ${JSON.stringify({ type: "error", message: err.message })}\n\n`);
      res.end();
    }
  }
});

app.get("/health", (_req, res) => res.send("OK"));
app.post("/", (_req, res) => res.send("OK"));

app.use((err, _req, res, _next) => {
  console.error("[app] route error:", err);
  res.status(500).type("text/plain").send(`Internal error: ${err.message}\n\nMode: ${mode}`);
});

(async () => {
  try {
    mode = (await data.init()).mode;
  } catch (err) {
    console.error("[app] data init failed:", err.message);
    mode = "empty";
  }
  app.listen(PORT, () => console.log(`Competitor dashboard listening on port ${PORT} (mode: ${mode})`));
})();
