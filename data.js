// Snowflake direct (Keboola Workspace creds) with sample-CSV fallback for local dev.
// Required env (set by Keboola Data App when a Workspace mapping is attached to
// in.c-competitor-monitoring): SNOWFLAKE_ACCOUNT, _USER, _PASSWORD, _WAREHOUSE,
// _DATABASE, _SCHEMA. Queries use unqualified table names — connection's default
// db+schema must point at the workspace mapped to in.c-competitor-monitoring.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import snowflake from "snowflake-sdk";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// CSV lookup order (first match wins):
//   1. KBC_DATADIR or /data/in/tables/  — Keboola Data App input-mapping path
//   2. CSV_DIR env var                  — explicit override
//   3. ../competitor-monitoring/data/exports/latest/  — local dev fallback
const KBC_INPUT_DIR = process.env.KBC_DATADIR
  ? path.join(process.env.KBC_DATADIR, "in", "tables")
  : "/data/in/tables";
const ENV_CSV_DIR = process.env.CSV_DIR;
const LOCAL_DEV_DIR = path.resolve(__dirname, "..", "competitor-monitoring", "data", "exports", "latest");
const TABLES = ["products_raw", "service_facts", "promos", "run_meta"];

function findCsv(name) {
  // Keboola input mapping deposits the file with no .csv extension by default.
  for (const dir of [KBC_INPUT_DIR, ENV_CSV_DIR, LOCAL_DEV_DIR].filter(Boolean)) {
    for (const filename of [name, `${name}.csv`]) {
      const p = path.join(dir, filename);
      if (fs.existsSync(p)) return p;
    }
  }
  return null;
}

let conn = null;

function hasEnv() {
  return Boolean(
    process.env.SNOWFLAKE_ACCOUNT && process.env.SNOWFLAKE_USER &&
    process.env.SNOWFLAKE_PASSWORD && process.env.SNOWFLAKE_DATABASE &&
    process.env.SNOWFLAKE_SCHEMA,
  );
}

function connect() {
  if (conn) return Promise.resolve(conn);
  conn = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USER,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
  });
  return new Promise((res, rej) =>
    conn.connect((err, c) => (err ? rej(err) : res(c))),
  );
}

function runSql(sql) {
  return new Promise((res, rej) =>
    conn.execute({ sqlText: sql, complete: (e, _s, r) => (e ? rej(e) : res(r ?? [])) }),
  );
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

function loadSample(name) {
  const f = findCsv(name);
  return f ? parseCsv(fs.readFileSync(f, "utf-8")) : [];
}

export async function init() {
  if (!hasEnv()) {
    const probe = findCsv(TABLES[0]);
    const dir = probe ? path.dirname(probe) : "(none found)";
    console.log(`[data] SNOWFLAKE_* env missing — CSV mode. Reading from: ${dir}`);
    return { mode: "csv" };
  }
  await connect();
  console.log(`[data] Snowflake connected: ${process.env.SNOWFLAKE_DATABASE}.${process.env.SNOWFLAKE_SCHEMA}`);
  return { mode: "snowflake" };
}

export async function fetchAll() {
  if (!hasEnv()) return Object.fromEntries(TABLES.map((t) => [t, loadSample(t)]));
  const out = {};
  for (const t of TABLES) {
    const rows = await runSql(`SELECT * FROM "${t}"`);
    // Snowflake returns upper-case keys by default — lowercase for consistency.
    out[t] = rows.map((r) => {
      const o = {};
      for (const [k, v] of Object.entries(r)) o[k.toLowerCase()] = v;
      return o;
    });
  }
  return out;
}
