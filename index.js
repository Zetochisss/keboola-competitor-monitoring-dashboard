// Internal dashboard for Czech mattress competitor monitoring.
// Reads curated tables from out.c-competitor-monitoring via Snowflake
// (products_curated, product_events) plus raw service_facts and promos
// (curation deferred until extraction quality improves).

import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as data from "./data.js";
import {
  portfolioMatrix, topExpensivePerCompetitor, servicesMatrix, activePromos,
  priceHistogram, promoSummary, notableInsights, segmentHeatmap, kpis,
  lastFetchedAt, newsOfTheDay, priceTrend,
  COMPETITORS, COMPETITOR_LABELS, SIZES, SEGMENTS,
} from "./aggregations.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = parseInt(process.env.PORT || "3000", 10);

const app = express();
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.static(path.join(__dirname, "public")));

let mode = "snowflake";

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
    const top = topExpensivePerCompetitor(d.products_curated, size, 10);
    const hist = priceHistogram(d.products_curated, size);
    res.render("portfolio", {
      ...commonLocals(d), active: "portfolio", title: "Sortiment",
      size, matrix, totals, top, hist,
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
    process.exit(1);
  }
  app.listen(PORT, () => console.log(`Competitor dashboard listening on port ${PORT} (mode: ${mode})`));
})();
