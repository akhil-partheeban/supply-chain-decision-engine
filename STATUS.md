# Status Report

Verified against live AWS resources, a fresh query of the local DuckDB file, the
actual API/dashboard code, and real (non-mocked) calls — not against docstrings,
dashboard captions, or prior documentation. Originally generated 2026-09-16;
updated 2026-10-04 after the supplier emissions layer (Phase 8), a round of
accuracy fixes to the docs/dashboard, and a production redeploy that fixed the
AI Decision Assistant.

---

## 1. Deployment

**Live:** `https://nczgm5fen7.us-east-2.awsapprunner.com`, AWS account `650126342842`,
region `us-east-2`. Re-verified fresh today:

- `GET /health` → `200 {"status":"ok","db":"ok"}` — working.
- `GET /suppliers/?limit=2` → `200`, returns real scorecard rows. Working end to end.
- `POST /decisions/ask` → **`200`, a real, data-grounded answer.** Tested fresh
  just before writing this report: asked "How many total suppliers are there?",
  got back "3,095" (matches `gold.gold_supplier_scorecard` exactly) with the real
  SQL it ran and genuine extracted action items. **This endpoint is fixed and
  working in production**, confirmed via a dedicated redeploy, not just a code fix
  sitting uncommitted.

**What it took to actually ship this, and the gotcha worth remembering.**
`agent/decision_agent.py`'s hardcoded model ID was repointed from the retired
`claude-sonnet-4-20250514` to `claude-sonnet-5` (commit `de5d5bb`), the image was
rebuilt with `docker buildx build --platform linux/amd64` (plain `docker build` on
this arm64 Mac produces an image that crash-loops on App Runner) and pushed to
ECR, then `terraform apply` added `ANTHROPIC_API_KEY` back to the service's
`RuntimeEnvironmentVariables`. **That `terraform apply` alone was not enough** —
App Runner's `UpdateService` call, when only the environment variables change (not
the image identifier string itself, which is always the unchanged tag `:latest`),
updates configuration without re-pulling the image. The service kept running the
*old* container — confirmed via CloudWatch logs still throwing
`anthropic.NotFoundError: model: claude-sonnet-4-20250514` after the apply
succeeded. Fix: an explicit `aws apprunner start-deployment` after any
config-only `terraform apply` forces App Runner to actually re-pull `:latest` and
deploy it. **Any future deploy that only changes env vars (not the image tag)
needs this same explicit `start-deployment` step, or it will silently keep
serving the old image.**

**Infra vs. Terraform state:** S3 bucket, ECR repo, both IAM roles, and the App
Runner service are all in Terraform state and match AWS reality.

**Not live:** `gcp/terraform` (Cloud Run) — written and `terraform validate`-checked
only, never applied.

**Fixed since the last report:** the `.gitignore` glob bug that failed to exclude
`aws/terraform/tfplan` (a file literally named `tfplan`, not matched by
`*.tfplan`) is fixed — `aws/terraform/tfplan` and `gcp/terraform/tfplan` are now
explicit entries, re-confirmed today via `git check-ignore`. The file was never
actually committed, but is now correctly protected against being added by accident.

**Pushed:** the emissions layer, the docs/dashboard accuracy fixes, and this
report's own prior revision are all on `origin/main` as of this report.

---

## 2. Data layer — fresh row counts, queried directly against `data/duckdb/supply_chain.duckdb`

| Layer | Table | Rows | Source | Live or static? |
|---|---|---:|---|---|
| bronze | customers | 99,441 | Olist CSV | Static — loaded once |
| bronze | geolocation | 1,000,163 | Olist CSV | Static |
| bronze | order_items | 112,650 | Olist CSV | Static |
| bronze | order_payments | 103,886 | Olist CSV | Static |
| bronze | order_reviews | 99,224 | Olist CSV | Static |
| bronze | orders | 99,441 | Olist CSV | Static |
| bronze | product_category_name_translation | 71 | Olist CSV | Static |
| bronze | products | 32,951 | Olist CSV | Static |
| bronze | sellers | 3,095 | Olist CSV | Static |
| bronze | world_bank_lpi | 245 | World Bank LPI API | One-time pull, scoped to 5 countries |
| bronze | comtrade_trade_flows | 2,800 | UN Comtrade API | Pulled multiple times via `ingestion.comtrade_pipeline` upsert — stale, no scheduler actually running it (§5) |
| silver | epa_ghg_emission_factors (seed) | 1,016 | EPA v1.3.0, downloaded + verified | Static — EPA publishes a new version roughly every 1-2 years, not pulled live |
| silver | category_to_naics (seed) | 73 | Hand-built mapping | Static, judgment-based (see DECISIONS.md, Phase 8) |
| silver | silver_order_item_emissions | 112,650 | Olist + EPA derived | Static; 111,047 of 112,650 (98.6%) have a resolvable emissions estimate |
| gold | gold_executive_summary | 1 | Olist derived | Static snapshot of last `dbt build` |
| gold | gold_supplier_scorecard | 3,095 | Olist derived | Static |
| gold | gold_concentration_risk | 3,095 | Olist derived | Static |
| gold | gold_geo_concentration | 23 | Olist derived | Static |
| gold | gold_sourcing_cost_drivers | 66 | Olist derived | Static |
| gold | gold_risk_score_validation | 3 | Olist derived | Static |
| gold | gold_macro_context | 1 | World Bank LPI + Comtrade world-agg (Brazil only) | Static |
| gold | gold_country_logistics_scorecard | 5 | World Bank LPI | Static, 5 countries only |
| gold | gold_trade_balance | 2 | Comtrade world-agg | Static |
| gold | gold_trade_concentration | 1,287 | Comtrade partner-breakdown | Static |
| gold | gold_trade_concentration_shift | 573 | Comtrade partner-breakdown | Static |
| gold | **gold_supplier_emissions** | 3,095 | Olist + EPA derived | Static; 60 of 3,095 sellers (1.9%) have a NULL `emissions_intensity` (no resolvable category) |
| gold | **gold_supplier_risk_emissions_score** | 3,095 | Derived from the above + `gold_supplier_scorecard` | Static; same 60 sellers have a NULL score |
| gold | **gold_supplier_emission_swap_suggestions** | 585 | Derived | Static — 585 suppliers flagged with a qualifying lower-emission alternative |

**Bottom line on "live" data:** unchanged from the last report — nothing in this
system updates on its own. Olist is a one-time Kaggle dump. Comtrade and World
Bank LPI can be re-pulled by hand but nothing triggers that automatically. The new
emissions layer is entirely derived from the existing Olist data plus a static,
versioned EPA reference dataset — there is no "live" component to it at all, by
design (it's explicitly a point-in-time estimate, not a monitoring feed).

---

## 3. Feature-by-feature status

| Feature | Status | Evidence / caveat |
|---|---|---|
| Executive Summary KPI tiles | **WORKING** | Unchanged |
| Supplier Risk table (dashboard) | **WORKING** | Unchanged |
| Concentration Risk by State chart | **WORKING** | Unchanged |
| Supplier Concentration (HHI) table | **WORKING** | Unchanged |
| Sourcing Cost Drivers chart | **WORKING** | Unchanged |
| Trade-Partner Concentration (Comtrade) panel | **PARTIALLY WORKING** | Same caveat as before (hardcoded to reporter_code=76); the dashboard's own "(live)" mislabel is now fixed — relabeled "(batch, manual refresh)" and the header comment corrected |
| `GET /suppliers/` and `/suppliers/{id}` | **WORKING** | Unchanged |
| `GET /health` | **PARTIALLY WORKING — has a real bug** | Unchanged: top-level `status` is hardcoded `"ok"` regardless of the nested DB check result |
| **AI Decision Assistant / `POST /decisions/ask`** | **WORKING** | Fixed in code (repointed to `claude-sonnet-5`) and now actually deployed to production — tested fresh today: `200`, a real Claude answer grounded in real SQL results, correct numbers (§1) |
| Dashboard's "Ask a question" widget | **WORKING** (inherits the above) | Calls `/decisions/ask` over HTTP; now gets a real `200` response from production |
| **Emissions tab — risk-vs-emissions scatter** | **WORKING** | Verified live in a browser: renders 3,035 suppliers, both "color by quadrant" and "color by primary category" toggle modes confirmed working, median reference lines render correctly |
| **Emissions tab — swap suggestions table** | **WORKING** | Verified live in a browser against real data; 585 rows, matches the SQL-level verification exactly |
| `gold_supplier_emissions` / `gold_supplier_risk_emissions_score` / `gold_supplier_emission_swap_suggestions` | **WORKING** | All three pass their dbt tests against the real DB; spot-checked numerically (rank ordering, constraint compliance on all 585 swap rows, NULL-handling for the 60 unmappable sellers) — see DECISIONS.md, Phase 8 |

---

## 4. Known issues / tech debt

**Carried over, unchanged:**
- `scripts/export_to_s3.py` only reads AWS credentials from env vars, not a
  profile.
- Apple Silicon build trap (`docker build` without `--platform` produces an
  amd64-incompatible image) — still not guarded against in `aws/deploy.sh`.
- A `CREATE_FAILED` App Runner service still requires delete-and-recreate, not a
  re-apply.
- `.venv/bin/python` is still a symlink into the shared Anaconda base install, not
  an isolated virtualenv.
- `test_agent.py` still has zero CI coverage (unchanged — still a live smoke test
  requiring a real key not configured as a CI secret).
- `api/main.py`'s `/health` endpoint bug (top-level `status` doesn't reflect the
  real DB check) — not fixed, still present.

**Fixed since the last report:**
- **`/decisions/ask` works in production** — fixed and deployed, not just fixed in
  code. See §1 for the full redeploy story and the App Runner
  config-only-apply-doesn't-re-pull-the-image gotcha it surfaced.
- Region defaults (`us-east-1` → `us-east-2`) — fixed and verified.
- `.gitignore` tfplan glob bug — fixed and verified (§1).
- Dashboard's "Real-time risk intelligence" caption and the Comtrade panel's
  "(live)" mislabel — both corrected to accurately describe manual batch refresh.
- README's multiple unqualified "weekly"/"live" claims about the Comtrade
  pipeline — corrected to state plainly that no Airflow instance is deployed
  running it; every refresh to date has been manual.

**New with the emissions layer (Phase 8) — honest limits, not bugs:**
- US EPA emission factors applied to Brazilian marketplace spend — the single
  largest source of error in the emissions estimate (see DECISIONS.md, Phase 8,
  §6, for the full list of four-plus limits: spend-based method, price-distorts-
  intensity, judgment-based category mapping, the 60-seller gap, and the
  single-fixed-year FX/CPI conversion applied uniformly across 2016-2018 orders).
- This is a spend-based Scope 3 *estimate*, not measured emissions data — stated
  directly in the dashboard caption and the README.

**Test suite, current and verified fresh (not copied from a prior run):**
- `pytest tests/ --ignore=tests/test_agent.py`: **29 passed**.
- `dbt build`: **PASS=81, WARN=0, ERROR=0, SKIP=0, NO-OP=0, TOTAL=81** — 3 seeds,
  21 models, 57 data tests. (Previous report: 17 models, 39 tests, pre-emissions.)

---

## 5. What's not built

Unchanged from the last report — the emissions layer added gold-layer SQL and a
dashboard tab, not infrastructure:

- **No streaming pipeline.** No Kafka, no Kinesis, no event bus of any kind.
- **No orchestration actually running.** `dags/comtrade_weekly_dag.py` exists as
  an Airflow TaskFlow DAG, but no Airflow container is running anywhere. Every
  data refresh to date — including loading the new EPA seed — has been a person
  running a script or `dbt build` by hand.
- **No Snowflake, no Redshift, no BigQuery.** The entire warehouse is a single
  embedded DuckDB file, rebuilt in place by dbt.
- **No managed scheduler for the S3/App Runner refresh cycle.** Still fully
  manual, still `auto_deployments_enabled = false` by design.
- **GCP deployment is unapplied Terraform only.**
- In short, unchanged: this is DuckDB + dbt batch processing, invoked by hand.
  The dashboard's own captions now say so accurately (§4) — that's the one thing
  that changed about this section since the last report: the system's actual
  capabilities are the same, but the project no longer describes itself as
  "real-time" anywhere.
