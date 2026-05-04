// Reads curated tables that Keboola's input mapping deposits at
// /data/in/tables/ as CSVs. The cleaning logic lives in the SQL transformations
// (out.c-competitor-monitoring.*), so the CSVs are already clean — JS only
// coerces strings to numbers/dates as needed in aggregations.js.
//
// Local dev: set CSV_DIR to a folder of curated CSVs, or place them under
// competitor-monitoring/data/exports/latest/.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const KBC_INPUT_DIR = process.env.KBC_DATADIR
  ? path.join(process.env.KBC_DATADIR, "in", "tables")
  : "/data/in/tables";
const ENV_CSV_DIR = process.env.CSV_DIR;
const LOCAL_DEV_DIR = path.resolve(__dirname, "..", "competitor-monitoring", "data", "exports", "latest");

// Order matches data app input mapping destinations.
const TABLES = ["products_curated", "product_events", "service_facts", "promos"];

function findCsv(name) {
  // Keboola input mapping deposits the file at the destination filename
  // (with .csv) but historic exports may live as bare table names.
  for (const dir of [KBC_INPUT_DIR, ENV_CSV_DIR, LOCAL_DEV_DIR].filter(Boolean)) {
    for (const filename of [`${name}.csv`, name]) {
      const p = path.join(dir, filename);
      if (fs.existsSync(p)) return p;
    }
  }
  return null;
}

// Minimal CSV parser — handles quoted fields, escaped quotes, embedded newlines.
function parseCsv(text) {
  const rows = [];
  let cur = [""], q = false, i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (q) {
      if (ch === '"' && text[i + 1] === '"') { cur[cur.length - 1] += '"'; i += 2; continue; }
      if (ch === '"') { q = false; i++; continue; }
      cur[cur.length - 1] += ch; i++; continue;
    }
    if (ch === '"') { q = true; i++; continue; }
    if (ch === ",") { cur.push(""); i++; continue; }
    if (ch === "\n" || ch === "\r") {
      if (ch === "\r" && text[i + 1] === "\n") i++;
      rows.push(cur); cur = [""]; i++; continue;
    }
    cur[cur.length - 1] += ch; i++;
  }
  if (cur.length > 1 || cur[0] !== "") rows.push(cur);
  if (!rows.length) return [];
  const header = rows[0].map((h) => h.trim());
  return rows.slice(1).map((r) => Object.fromEntries(header.map((h, idx) => [h, r[idx] ?? ""])));
}

function loadCsv(name) {
  const f = findCsv(name);
  return f ? parseCsv(fs.readFileSync(f, "utf-8")) : [];
}

export async function init() {
  const probe = findCsv(TABLES[0]);
  const dir = probe ? path.dirname(probe) : "(none found)";
  console.log(`[data] Reading from: ${dir}`);
  return { mode: probe ? "live" : "empty" };
}

export async function fetchAll() {
  const out = {};
  for (const t of TABLES) out[t] = loadCsv(t);
  for (const t of TABLES) console.log(`[data] ${t}: ${out[t].length} rows`);
  return out;
}
