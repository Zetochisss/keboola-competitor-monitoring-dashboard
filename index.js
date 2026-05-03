// Internal dashboard for Czech mattress competitor monitoring.
// Reads from Keboola Storage bucket in.c-competitor-monitoring (via Snowflake)
// when running as a Keboola Data App; falls back to local CSVs otherwise.

import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as data from "./data.js";
import {
  portfolioMatrix, topExpensivePerCompetitor, servicesMatrix, activePromos,
  priceHistogram, promoSummary, notableInsights, segmentHeatmap, kpis,
  lastFetchedAt, COMPETITORS, COMPETITOR_LABELS, SIZES, SEGMENTS,
} from "./aggregations.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = parseInt(process.env.PORT || "3000", 10);

const app = express();
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.static(path.join(__dirname, "public")));

let mode = "sample";

// 5-min in-process cache. Tables are <1K rows and refresh daily; this keeps
// Snowflake load near zero with no noticeable staleness.
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

function commonLocals(d) {
  return {
    mode,
    competitors: COMPETITORS,
    competitorLabels: COMPETITOR_LABELS,
    sizes: SIZES,
    segments: SEGMENTS,
    today: todayCs(),
    lastFetched: lastFetchedAt([
      ...(d.products_raw || []), ...(d.service_facts || []), ...(d.promos || []),
    ]),
  };
}

app.get("/", async (_req, res, next) => {
  try {
    const d = await getData();
    res.render("overview", {
      ...commonLocals(d),
      active: "overview",
      title: "Overview",
      kpi: kpis(d.products_raw, d.service_facts, d.promos),
      heatmap: segmentHeatmap(d.products_raw),
      insights: notableInsights(d.products_raw, d.service_facts, d.promos),
    });
  } catch (err) { next(err); }
});

app.get("/portfolio", async (req, res, next) => {
  try {
    const size = (req.query.size || "all").toString();
    const d = await getData();
    const { matrix, totals } = portfolioMatrix(d.products_raw, size);
    const top = topExpensivePerCompetitor(d.products_raw, size, 10);
    const hist = priceHistogram(d.products_raw, size);
    res.render("portfolio", {
      ...commonLocals(d), active: "portfolio", title: "Portfolio",
      size, matrix, totals, top, hist,
    });
  } catch (err) { next(err); }
});

app.get("/services", async (_req, res, next) => {
  try {
    const d = await getData();
    res.render("services", {
      ...commonLocals(d), active: "services", title: "Services",
      rows: servicesMatrix(d.service_facts),
    });
  } catch (err) { next(err); }
});

app.get("/promos", async (_req, res, next) => {
  try {
    const d = await getData();
    res.render("promos", {
      ...commonLocals(d), active: "promos", title: "Promos",
      promos: activePromos(d.promos),
      summary: promoSummary(activePromos(d.promos)),
    });
  } catch (err) { next(err); }
});

app.get("/trends", async (_req, res, next) => {
  try {
    const d = await getData();
    res.render("trends", {
      ...commonLocals(d), active: "trends", title: "Trends",
    });
  } catch (err) { next(err); }
});

// Health endpoints for Keboola Data App probe.
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
    console.error("[app] data init failed, falling back to sample:", err.message);
    mode = "sample";
  }
  app.listen(PORT, () => console.log(`Competitor dashboard listening on port ${PORT} (mode: ${mode})`));
})();
