# keboola-competitor-monitoring-dashboard

Internal dashboard (Node + Express + EJS) for the daily mattress competitor monitoring side project. Deployed as a **Keboola Data App** in project 374 (GCP US East4).

## Where the code actually lives

This repo is the **deploy repo** — a flat-layout copy that the Keboola Data App runtime clones. **Development happens in the [universal-keboola-operation-tooling](https://github.com/Zetochisss/universal-keboola-operation-tooling) monorepo under `dashboard/`** and is mirrored here.

If you push to the monorepo only, the Data App keeps running stale code. After every monorepo edit, sync to here:

```
SRC=path/to/monorepo
DST=path/to/this/repo
cp $SRC/dashboard/{aggregations.js,data.js,index.js,package.json,package-lock.json} $DST/
cp -R $SRC/dashboard/{public,views} $DST/
git -C $DST add -A && git -C $DST commit -m "sync from monorepo" && git -C $DST push
```

Then **Restart** the Data App in Keboola UI.

## Layout

```
.
├── README.md
├── package.json + package-lock.json
├── index.js, data.js, aggregations.js
├── public/         (CSS)
├── views/          (EJS templates)
└── keboola-config/
    ├── setup.sh                          (runs `npm ci --omit=dev` in /app)
    ├── supervisord/services/app.conf     (runs `node /app/index.js`)
    └── nginx/sites/default.conf          (8888 → 127.0.0.1:3000)
```

## Data sources (precedence)

1. **Keboola input mapping** at `/data/in/tables/<table>` — what the deployed Data App uses
2. `CSV_DIR` env var — explicit override
3. `../competitor-monitoring/data/exports/latest/` — local dev fallback
4. **Snowflake direct** if `SNOWFLAKE_*` env vars are set — currently optional (input mapping is the active path)
