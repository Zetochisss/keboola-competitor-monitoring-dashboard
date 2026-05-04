// Snowflake reads via Keboola Workspace credentials. Local dev requires the
// same SNOWFLAKE_* env vars; CSV fallback was removed when the curated tables
// became the single source of truth (out.c-competitor-monitoring.*).

import snowflake from "snowflake-sdk";

const TABLES = ["products_curated", "product_events", "service_facts", "promos"];

let conn = null;

function requireEnv() {
  const missing = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"]
    .filter((k) => !process.env[k]);
  if (missing.length) {
    throw new Error(`Missing Snowflake env vars: ${missing.join(", ")}. Local dev requires the same Workspace credentials Keboola injects in production.`);
  }
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

export async function init() {
  requireEnv();
  await connect();
  console.log(`[data] Snowflake connected: ${process.env.SNOWFLAKE_DATABASE}.${process.env.SNOWFLAKE_SCHEMA}`);
  return { mode: "snowflake" };
}

export async function fetchAll() {
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
