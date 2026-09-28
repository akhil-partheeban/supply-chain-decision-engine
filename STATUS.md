# Status Report

Verified against live AWS resources, a fresh query of the local DuckDB file, the
actual API/dashboard code, and a real (non-mocked) call to the Anthropic API — not
against docstrings, dashboard captions, or prior documentation. Generated 2026-09-16.

---

## 1. Deployment

**Live:** `https://nczgm5fen7.us-east-2.awsapprunner.com`, AWS account `650126342842`,
region `us-east-2`.

- `GET /health` → `200 {"status":"ok","db":"ok"}` — the container's cold-start fetch
  of the DuckDB snapshot from S3 (`docker/fetch_db.py`) is confirmed working in
  production, not just locally.
- `GET /suppliers/?limit=2` → `200`, returns real scorecard rows. Working end to end.
- `POST /decisions/ask` → **`500 Internal Server Error`**. Root cause below (§3).

**Infra vs. Terraform state:** S3 bucket, ECR repo, both IAM roles, and the App
Runner service are all in Terraform state and match AWS reality — `terraform plan`
shows `0 to add, 0 to destroy`. **One drift item exists right now:** the deployed
App Runner service is missing the `ANTHROPIC_API_KEY` runtime environment variable
(`terraform plan` shows `1 to change` to add it back). Confirmed directly via
`aws apprunner describe-service` — the live service's
`RuntimeEnvironmentVariables` has exactly 3 keys (`AWS_REGION`, `DUCKDB_PATH`,
`DUCKDB_S3_URI`), not the 4 in the Terraform config. This happened because the key
was exported in one shell session but a later `terraform apply` was run from a
fresh shell where it had reset to empty — nothing in the tooling enforces that the
var is actually present at apply time. **This is a live, unresolved drift as of
this report**, not a hypothetical.

**Not live:** `gcp/terraform` (Cloud Run) — written and `terraform validate`-checked
only, never applied. No GCP project touched by this codebase.

**Also true right now, worth fixing separately:** `aws/terraform/tfplan` sits
untracked in the repo. `.gitignore` only excludes `aws/terraform/*.tfplan` (with a
dot before the extension) — the actual file is named `tfplan` with no leading dot,
so **it does not match the ignore pattern** and is one `git add -A` away from being
committed. That file's binary plan data embeds the real `ANTHROPIC_API_KEY` in
plaintext. Fix the glob or delete the file; don't commit it.

---

## 2. Data layer — fresh row counts, queried directly against `data/duckdb/supply_chain.duckdb`

| Layer | Table | Rows | Source | Live or static? |
|---|---|---:|---|---|
| bronze | customers | 99,441 | Olist CSV | Static — loaded once (single `_loaded_at`/`_source_file`, 2026-08-02) |
| bronze | geolocation | 1,000,163 | Olist CSV | Static |
| bronze | order_items | 112,650 | Olist CSV | Static |
| bronze | order_payments | 103,886 | Olist CSV | Static |
| bronze | order_reviews | 99,224 | Olist CSV | Static |
| bronze | orders | 99,441 | Olist CSV | Static |
| bronze | product_category_name_translation | 71 | Olist CSV | Static |
| bronze | products | 32,951 | Olist CSV | Static |
| bronze | sellers | 3,095 | Olist CSV | Static |
| bronze | world_bank_lpi | 245 | World Bank LPI API | One-time pull, scoped to 5 countries (`--countries` flag) on 2026-08-02, not the full dataset |
| bronze | comtrade_trade_flows | 2,800 | UN Comtrade API (world-agg + partner-breakdown, same table) | Pulled multiple times (4 distinct `_run_id`s, 2026-08-06 → 2026-08-31) via `ingestion.comtrade_pipeline` upsert — **but stale**: last pull was 16 days before this report, not "weekly" as the DAG name implies (see §4/§5 — no Airflow is actually running) |
| gold | gold_executive_summary | 1 | Olist derived | Static snapshot of last `dbt build` (2026-08-31) |
| gold | gold_supplier_scorecard | 3,095 | Olist derived | Static |
| gold | gold_concentration_risk | 3,095 | Olist derived | Static |
| gold | gold_geo_concentration | 23 | Olist derived | Static |
| gold | gold_sourcing_cost_drivers | 66 | Olist derived | Static |
| gold | gold_risk_score_validation | 3 | Olist derived | Static |
| gold | gold_macro_context | 1 | World Bank LPI + Comtrade world-agg (Brazil only) | Static, last refreshed 2026-08-02/08-31 |
| gold | gold_country_logistics_scorecard | 5 | World Bank LPI | Static, 5 countries only (DEU, USA, CHN, BRA, ARG) |
| gold | gold_trade_balance | 2 | Comtrade world-agg | Static, reporter 76 (Brazil) only, periods 2024/2025 |
| gold | gold_trade_concentration | 1,287 | Comtrade partner-breakdown | Last pulled 2026-08-31; covers reporters 76 (Brazil) and 32 (Argentina) × commodities 85/33/94 |
| gold | gold_trade_concentration_shift | 573 | Comtrade partner-breakdown | Same as above |

**Bottom line on "live" data:** nothing in this system is actually live in the sense
of updating on its own. Olist is a one-time Kaggle dump, correctly treated as
static. Comtrade and World Bank LPI *can* be re-pulled and *are* designed to
accumulate a time series (upsert, not full-replace, for the partner-breakdown
path) — but the only thing that has ever triggered a pull is a person running the
ingestion script by hand. The "last refreshed" caption on the dashboard's Comtrade
panel is accurate as a caption; the surrounding claim of a live weekly pipeline is
not currently true in practice (§5).

---

## 3. Feature-by-feature status

| Feature | Status | Evidence / caveat |
|---|---|---|
| Executive Summary KPI tiles | **WORKING** | Reads `gold.gold_executive_summary` directly; verified the row exists and matches `reports/pipeline_metrics.json`'s last dbt run |
| Supplier Risk table (dashboard) | **WORKING** | Reads `gold.gold_supplier_scorecard`, 3,095 rows confirmed present |
| Concentration Risk by State chart | **WORKING** | Reads `gold.gold_geo_concentration`, 23 states confirmed |
| Supplier Concentration (HHI) table | **WORKING** | Reads `gold.gold_concentration_risk` |
| Sourcing Cost Drivers chart | **WORKING** | Reads `gold.gold_sourcing_cost_drivers`, 66 categories confirmed |
| Trade-Partner Concentration (Comtrade) panel | **PARTIALLY WORKING** | Renders real data (1,287 rows), but hardcoded to `reporter_code = 76` (Brazil) even though reporter 32 (Argentina) data also exists in the same table and is invisible in the UI. Also implicitly framed as "live" in the section header — it isn't (§2, §5) |
| `GET /suppliers/` and `/suppliers/{id}` | **WORKING** | Tested live against the deployed API, returns real rows |
| `GET /health` | **PARTIALLY WORKING — has a real bug** | `api/main.py`: the top-level `"status"` key is hardcoded to `"ok"` regardless of whether the DB check inside actually succeeded — only the nested `"db"` field reflects the real result. A broken DB connection would still report overall `status: ok`. Confirmed by reading the code, not by breaking it live. |
| **AI Decision Assistant / `POST /decisions/ask`** | **BROKEN** | Not a stub — `agent/decision_agent.py` is a real, non-trivial Anthropic SDK tool-calling loop (`run_sql` + `get_executive_summary` tools, a genuine multi-turn loop). But it has **never worked end-to-end**, confirmed two independent ways today: (1) in production, the deployed service has no `ANTHROPIC_API_KEY` at all → immediate 500; (2) tested directly against Anthropic's real API with a valid key, in a clean environment: **`404 not_found_error: model: claude-sonnet-4-20250514`** — the hardcoded model ID in `agent/decision_agent.py` no longer exists on Anthropic's backend. This means the assistant would fail even with the key fixed. Per `DECISIONS.md` (Phase 4/8), this code has zero CI coverage and had never previously been tested past an auth-failure stage — today's test is the first real end-to-end attempt on record, and it fails. |
| Dashboard's "Ask a question" widget | **BROKEN** (inherits the above) | Calls `/decisions/ask` over HTTP; will surface the same 500/404 chain once the API call is actually made |

---

## 4. Known issues / tech debt

- **AI assistant is broken at the model level**, independent of the deployment
  issue: `MODEL = "claude-sonnet-4-20250514"` in `agent/decision_agent.py` is
  retired (`404 not_found_error`, confirmed live). Needs to be repointed at a
  currently-served model before anything else about the agent matters.
- **Production is missing `ANTHROPIC_API_KEY`** (§1) — a second, independent reason
  `/decisions/ask` fails right now, caused by env-var non-persistence across shell
  sessions during manual `terraform apply` runs, not by any code defect.
- **`aws/terraform/tfplan` isn't actually gitignored** (§1) and contains the real
  Anthropic key in plaintext — the glob has a bug (`*.tfplan` vs. a file literally
  named `tfplan`).
- **Region defaults were wrong repo-wide until this session**: `aws/terraform/variables.tf`,
  `aws/deploy.sh`, and `scripts/export_to_s3.py` all defaulted to `us-east-1` while
  every real resource lives in `us-east-2`. Fixed today; verify no other script
  still carries the same default.
- **`scripts/export_to_s3.py` only reads AWS credentials from
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars**, not a `~/.aws/credentials`
  profile — inconsistent with every other AWS command in this project. Silently
  makes an unauthenticated S3 request (403) if those two vars aren't exported,
  rather than erroring clearly.
- **Apple Silicon build trap**: `docker build` (no `--platform`) on an arm64 Mac
  produces an image that crash-loops on App Runner (`exec format error`, amd64-only
  service). `aws/deploy.sh`'s plain `docker build` step doesn't protect against
  this. Confirmed by reproducing it live during this session's deploy.
- **A `CREATE_FAILED` App Runner service cannot be fixed by re-running `apply`** —
  AWS requires deleting and recreating it. Not documented anywhere before this
  session.
- **Local dev environment isn't actually isolated**: `.venv/bin/python` is a
  symlink into the machine's shared Anaconda base install, not a real virtualenv.
  `pip install -r requirements-dev.txt` mutates the shared base environment
  (confirmed: it triggered an `aiobotocore`/`botocore` version conflict warning on
  this machine). The test suite could not even be collected
  (`ModuleNotFoundError: anthropic`, `responses`, `moto`) until `requirements-dev.txt`
  was explicitly installed — meaning this environment had never actually been set
  up per the project's own README before today.
- **`test_agent.py` has zero CI coverage** and is a real (non-mocked) live smoke
  test requiring a real API key — confirmed by reading it. `DECISIONS.md` already
  documents this gap and proposes mocking the Anthropic client the same way
  Comtrade/World Bank calls are mocked; that work hasn't been done.
- **`api/main.py`'s `/health` endpoint bug** (§3): overall `status` field doesn't
  actually reflect DB health.
- Test suite otherwise passes cleanly once dependencies are installed: **29 passed,
  1 failed** (`tests/test_agent.py`, the live Anthropic call — failure is the model
  404 above, not a flaky test).
- `dbt build`'s last real run was 2026-08-31 (17 models, 39 tests, 0 failures, per
  `reports/pipeline_metrics.json`) — matches what's actually in the DuckDB file
  today; nothing has silently drifted between the report and reality.

---

## 5. What's not built

- **No streaming pipeline.** No Kafka, no Kinesis, no event bus of any kind.
- **No orchestration actually running.** `dags/comtrade_weekly_dag.py` exists as an
  Airflow TaskFlow DAG and `docker/docker-compose.airflow.yml` exists to run it,
  but **no Airflow container is running anywhere** — confirmed via `docker ps -a`,
  nothing Airflow-related is up. Every data refresh to date has been a person
  manually running `python -m ingestion.comtrade_pipeline` or `dbt build`. The
  "weekly" cadence in the DAG's `schedule="@weekly"` has never actually fired.
- **No Snowflake, no Redshift, no BigQuery.** The entire warehouse is a single
  embedded DuckDB file (currently 98MB), rebuilt in place by dbt.
- **No managed scheduler for the S3/App Runner refresh cycle either** —
  `deploy.sh` and `export_to_s3.py` are both manual, human-triggered scripts, not
  wired into anything that runs on a schedule or on a git push
  (`auto_deployments_enabled = false` by design, per `aws/README.md`).
- **GCP deployment is unapplied Terraform only** (§1) — no Cloud Run service exists.
- In short: this is DuckDB + dbt batch processing, invoked by hand, deployed to one
  App Runner service that itself has to be manually redeployed after every data or
  code change. Nothing here is "real-time" despite the dashboard's own caption
  calling it "Real-time risk intelligence" — that caption is not accurate as written.
