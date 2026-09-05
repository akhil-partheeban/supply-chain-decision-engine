# DECISIONS.md

Engineering decision log for the Supply Chain Decision Engine, written to be read cold
before a technical interview. Each entry: what was built, why, what else was considered,
the question an interviewer is likely to ask, a defensible answer, and the honest
weak point. Organized by phase, in build order.

All numbers cited below are real, computed against the full Olist dataset
(99,441 orders, 3,095 sellers, 112,650 order line items, ~$13.6M in order-item revenue)
after running `python -m ingestion.load_bronze && cd dbt && dbt build`. Where a number
could go stale as the dataset or model logic changes, that's called out explicitly —
don't cite these as eternal facts, re-derive them with `dbt build` if it matters.

---

## Phase 1 — Gold Layer

### 1. Consolidated `gold_supplier_performance` + `gold_supplier_risk` into one `gold_supplier_scorecard`

**What was built.** The two original gold models both grouped by `seller_id` off
nearly-identical joins (sellers ⋈ order_items ⋈ orders ⋈ reviews) — one computed
revenue/freight, the other computed late-delivery-rate/risk_tier. They were merged into
a single `gold_supplier_scorecard` with every seller-level metric in one place.

**Why.** Two models independently deriving "is this seller doing well" off the same raw
joins is a drift risk: if someone tweaks the late-delivery logic in one model, the other
silently goes stale. There's also no analytical reason to split them — any dashboard or
analyst asking "how is this seller doing" wants revenue, freight, delivery, and reviews
in the same row, not two queries joined at the call site.

**Alternatives considered.**
- *Leave them separate, just fix the duplication with a shared silver/intermediate
  model.* Rejected because the split itself wasn't adding value — there's no consumer
  that wants risk fields without performance fields — so a shared upstream model plus
  two thin pass-through gold models would've been pure ceremony.
- *One giant model covering supplier + geography + cost drivers.* Rejected — that
  conflates three different grains (seller, state, product category) into one table,
  which is the kind of "everything view" that becomes unreadable and untestable.

**Likely interview question:** *"Why did you merge these two tables instead of just
having the API join them?"*
**Answer:** Because they're the same grain (one row per seller) and always consumed
together. Joining two same-grain tables at query time instead of at build time is extra
work for every consumer, for no isolation benefit — nothing in the codebase needs
"performance without risk."

**Weakness, stated honestly.** `gold_supplier_scorecard` is now a fairly wide table
(17 columns) mixing volume, cost, delivery, and satisfaction metrics. If the supplier
base grew to the point where different teams owned different metric families (e.g.
finance owns cost, ops owns delivery), this would eventually want to split back out —
but at 3,095 sellers and one consuming team, that's premature.

---

### 2. Moved aggregation out of `silver_sellers`, into gold

**What was built.** The original `silver_sellers` model already grouped by `seller_id`
and computed `total_orders`, `late_delivery_rate`, `avg_delivery_days` — i.e., it was
doing gold-layer work (cross-table aggregation) while living in the silver schema.
`silver_sellers` is now a pure dimension: `seller_id`, `seller_city`, `seller_state`,
nothing computed. All aggregation happens in `gold_supplier_scorecard`.

**Why.** The medallion convention (documented in this repo's own CLAUDE.md) is
bronze = raw, silver = cleaned/typed at natural grain, gold = aggregated for
consumption. An aggregated table sitting in silver breaks that contract — anyone
building a second gold model on top of "clean sellers" would have gotten a
pre-aggregated table instead of a dimension, and had to either work around it or
re-derive the raw grain themselves (which is exactly what `gold_concentration_risk`
and `gold_sourcing_cost_drivers` needed to do).

**Alternatives considered.**
- *Leave it as-is and just add new gold models that also hit bronze directly.*
  Rejected — this is what the code did before, and it's how "late_delivery_rate" ended
  up computed three separate times (`silver_sellers`, the old `gold_supplier_risk`, and
  `gold_executive_summary` all had their own copy of the late-delivery CASE expression).
  One bug fix in one copy and the other two silently diverge.

**Likely interview question:** *"Isn't `silver_sellers` now a trivial pass-through of
`bronze.sellers`? Why does it need to be its own model?"*
**Answer:** It's intentionally thin right now — that's fine. It exists as the seam
where seller-level cleaning (deduplication, city-name normalization, geocoding
enrichment) would go if the source data needed it, without every gold model reaching
into bronze directly. Olist's seller table happens to already be clean, so today the
model does almost nothing — but "does almost nothing today" is different from
"shouldn't exist."

**Weakness, stated honestly.** This is a real refactor of pre-existing logic, and the
regrouping wasn't validated against a "golden" prior output — it was validated by
checking that seller counts, revenue totals, and late-rate aggregates in the new
gold layer land on the same real-data figures the old queries produced (see the git
history: `overall_late_rate` is 7.84% in both the old `gold_executive_summary` logic
style and the new one, computed independently). If an interviewer pushes on "how do
you know the refactor didn't change the numbers," the honest answer is: manual
before/after comparison on this pass, not an automated regression test. That gap is a
fair thing to name as a next step.

---

### 3. Composite `reliability_score` (0–100) instead of a single metric

**What was built.** `gold_supplier_scorecard.reliability_score` is a weighted blend:

```
reliability_score = 50 * on_time_rate
                   + 30 * (avg_review_score / 5)
                   + 20 * max(0, 1 - delivery_days_stddev / 20)
```

`risk_tier` is then derived from the score: `>=85` → LOW, `>=70` → MEDIUM, else HIGH.

**Why these three components, and these weights.** On-time delivery is weighted
heaviest (50%) because it's the most direct, least noisy signal of whether a supplier
does what they promised. Review score (30%) captures the customer-facing outcome that
delivery timing alone misses — a seller can be on time and still ship a broken or
misrepresented product. Delivery-day *consistency* (20%, via stddev, not just the
average) exists because two sellers averaging 8-day delivery are not equivalent risks
if one is steady (±2 days) and the other swings wildly (±20 days) — average delivery
time hides that, and predictability matters as much as speed for planning purposes.
The 50/30/20 split itself is a judgment call, not derived from data — see the weakness
below.

**Why the 20-day cap on the consistency component, specifically.** This wasn't picked
arbitrarily. Across the 1,864 sellers with ≥5 delivered orders (enough to make a stddev
meaningful), the observed distribution of `delivery_days_stddev` is: median 6.8 days,
p75 = 9.1, p90 = 11.7, p95 = 14.3, max = 63. A 20-day cap sits just above the 95th
percentile — so the consistency component only fully bottoms out (score = 0) for
sellers who are genuinely extreme outliers, not for typical variability. Pick a smaller
cap (say, 10 days) and you'd zero out a huge share of otherwise-fine sellers just for
having ordinary logistics noise.

**Alternatives considered.**
- *Late-delivery-rate alone as the risk signal* (the original implementation). Rejected
  — it collapses three different failure modes (unreliable timing, poor product
  quality, inconsistent lead times) into one number, and a seller with a 0% late rate
  but a 2.1/5 average review score would have looked "LOW risk," which is wrong.
- *Equal weighting (33/33/33).* Considered and rejected — on-time delivery is a harder,
  more objective signal than review score (reviews carry sentiment noise: a customer
  can leave a low review for a reason unrelated to the seller, like a slow courier or
  their own buyer's remorse), so weighting it down to equal footing understates its
  reliability as a predictor.
- *A learned model (e.g., logistic regression predicting "will this seller cause a
  problem in the next N orders") instead of a hand-weighted formula.* Rejected for this
  project's scope — there's no labeled "problem" outcome in the Olist data to train
  against, and a hand-weighted, inspectable formula is more defensible in a procurement
  context anyway (a buyer needs to be able to explain *why* a supplier is flagged, not
  just trust a black box).

**Likely interview question:** *"Why 50/30/20 and not some other split? Did you tune
it?"*
**Answer, honestly:** No — it's a deliberately simple, inspectable weighting reflecting
a defensible ordering of signal reliability (on-time delivery > satisfaction >
consistency), not a value tuned against a labeled outcome. I'd frame it to a
stakeholder as "if you disagree with these weights, here's exactly which knob to turn"
— the formula's value is that it's auditable, not that the specific numbers are
optimal.

**Weakness, stated honestly, twice:**
1. **The weights are subjective.** There's no ground-truth "supplier caused a supply
   chain incident" label in this dataset to validate the formula against. If asked to
   defend the *exact* numbers rather than the *structure*, the honest answer is "these
   are a reasonable prior, not a fitted result."
2. **Sellers with 0 or 1 delivered orders get `delivery_days_stddev` coalesced to 0**
   (stddev is mathematically undefined below 2 data points), which means the formula
   treats "no delivery history" as "perfectly consistent" — the opposite of the truth.
   571 of 3,095 sellers (18.5%) have exactly one order. This is a real limitation: a
   brand-new seller with a single lucky on-time delivery can score as high as an
   established seller with a genuine track record. A more careful version would
   down-weight or flag low-volume sellers separately rather than silently assuming
   the best about them.

---

### 4. Supplier concentration via HHI (Herfindahl-Hirschman Index), not just "top supplier %"

**What was built.** `gold_concentration_risk` ranks every seller by revenue share and
computes each seller's contribution to the portfolio HHI
(`sum((revenue_share_pct)^2)`, range 0–10,000). `gold_executive_summary` reports the
summed HHI and buckets it using the standard DOJ/FTC Horizontal Merger Guidelines
convention: <1,500 unconcentrated, 1,500–2,500 moderately concentrated, ≥2,500 highly
concentrated. A per-seller `concentration_flag` also fires independently if any single
seller exceeds 10% of total revenue (a common single-source procurement heuristic).

**Why HHI specifically.** A simple "top-1 supplier's % of spend" metric misses the case
where the *top five* suppliers collectively hold 90% of spend but no single one crosses
an alarming threshold — that's still a concentrated, fragile supply base. HHI captures
the whole distribution's shape in one number (it's the sum of every seller's squared
share, so it's sensitive to a few large players even if none individually looks
extreme) and it's a well-established, named index — "explain a proprietary risk score"
is a much weaker interview answer than "we used HHI, the same index the DOJ uses to
evaluate market concentration in mergers, repurposed for supply-base risk."

**What the real data actually shows, and why that matters.** On this dataset, the
portfolio HHI is **35.68** — deep in "unconcentrated" territory (the top single seller
holds only 1.7% of revenue; the top 5 combined hold 7.6%). That's a genuine, honest
finding, not a bug: Olist is a many-seller marketplace, not a traditional enterprise
supply chain with a handful of strategic vendors, so there's no meaningful single-source
risk to find at the individual-seller level. The geographic cut
(`gold_geo_concentration`) tells a very different story — São Paulo alone accounts for
**64.4%** of total revenue — which is the real concentration risk in this dataset: not
"which vendor," but "which region."

**Alternatives considered.**
- *Top-N share only (e.g., "top 5 suppliers = X% of spend").* Kept as a secondary
  metric (`top5_supplier_revenue_share_pct` in the exec summary) because it's more
  intuitive to a non-technical stakeholder, but not used alone — it doesn't capture
  the full distribution the way HHI does.
- *Gini coefficient.* Considered — also a valid inequality measure — but HHI is the
  domain-standard term procurement/strategic-sourcing conversations actually use
  ("supplier concentration risk"), whereas Gini is more associated with income
  inequality and would need extra explanation in this context.

**Likely interview question:** *"Your HHI says the supply base is basically
unconcentrated. Doesn't that make this metric kind of pointless for this dataset?"*
**Answer:** It correctly reports that individual-seller concentration isn't the risk
here — and that's a useful, real finding, not a wasted metric. It also means the model
is doing its job: if I'd hard-coded a "concentration risk" score without checking
whether the data actually showed concentration, that would be the placeholder metric
this whole phase was explicitly trying to avoid. The metric earns its keep on the
*geographic* cut instead, where it does show a real, large (64%) concentration in São
Paulo.

**Weakness, stated honestly.** HHI over *marketplace sellers* is a different animal
from HHI over *strategic suppliers* in a real enterprise supply chain (e.g., a
manufacturer with 8 raw-material vendors). Olist's few-thousand-seller structure means
this metric will basically always read "unconcentrated" for any e-commerce-marketplace-
shaped dataset, regardless of real operational risk. If defending this project for a
role that's actually about supplier/vendor management (not marketplace analytics), be
upfront that this metric's low reading is a function of the dataset's shape, not
evidence the technique doesn't generalize.

---

### 5. Sourcing cost drivers by product category, freight-normalized

**What was built.** `gold_sourcing_cost_drivers` (new model, no prior equivalent
existed) aggregates by product category (English-translated via
`product_category_name_translation`, joined in the new `silver_order_items` model):
total spend, average price, average freight, **freight cost per kg** (freight value
divided by product weight), and `freight_pct_of_spend` with a HIGH/MEDIUM/LOW tier.
Categories under 30 line items are excluded as too noisy to act on.

**Why freight-per-kg in addition to freight-as-%-of-price.** Freight % of price is
misleading on its own: a cheap, light item can show a high freight percentage purely
because its price is low, not because it's expensive to ship. Freight-per-kg isolates
the actual shipping cost efficiency, independent of the item's price point — the two
metrics together distinguish "this category is expensive to ship" from "this category
is just cheap, so any freight looks big by comparison."

**Why the 25% / 18.5% tier cutoffs.** Derived from the real data, not picked in
advance: across the 66 categories that clear the 30-item materiality bar, the
freight-to-spend ratio has a median of 18.5% and a 75th percentile of 22.8%. HIGH
(>25%) sits just above the top quartile; MEDIUM covers "worse than the median but not
extreme." The single worst category in the real dataset, `home_comfort_2`, runs 54%
freight-to-spend — bulky, low-value items are the textbook case this metric is meant to
surface.

**Alternatives considered.**
- *Freight cost by seller instead of by category.* Rejected as the primary cut —
  freight cost is much more a function of *what's being shipped* (weight, bulk) than
  *who's shipping it*; category is the more actionable grouping for a sourcing
  decision ("should we source lighter packaging for this product line") versus a
  seller-management decision.
- *Incorporating shipping distance (buyer state vs. seller state) via the geolocation
  table.* Considered and explicitly deferred — see weakness below.

**Likely interview question:** *"Why exclude categories under 30 items? Doesn't that
throw away data?"*
**Answer:** It throws away *noisy* data on purpose. A category with 4 line items and
one unusually heavy order would show a wildly unrepresentative freight ratio; the bar
exists so the HIGH/MEDIUM/LOW tiers (which are threshold-based, not rank-based) aren't
gamed by small-sample noise. 66 of the roughly 73 total categories clear this bar, so
the exclusion is small.

**Weakness, stated honestly.** This model does **not** use the `bronze.geolocation`
table, so it can't distinguish "this category is expensive to ship because it's heavy"
from "this category is expensive to ship because its sellers happen to be far from
their buyers." Both effects are folded into one freight-per-kg number. A more complete
version would control for buyer-seller distance (available via zip-code-prefix
centroids in `bronze.geolocation`, currently unused anywhere in the gold layer) before
attributing freight cost to the category itself.

---

### 6. Kept geographic concentration as a separate model from supplier concentration

**What was built.** `gold_geo_concentration` (renamed from the original
`gold_concentration_risk`, which was state-based) is now a distinct model from the new
seller-based `gold_concentration_risk`. Same 20%-of-revenue HIGH threshold as before,
but revenue-weighted instead of order-count-weighted.

**Why keep both, rather than pick one.** They measure genuinely different failure
modes: seller concentration asks "how bad is it if we lose one vendor," geographic
concentration asks "how bad is it if one region has a disruption (customs, weather,
regional logistics strike) and takes out every seller in it at once." A supply base can
score perfectly on one axis and terribly on the other — which is exactly what happens
here (HHI = 35.68, "unconcentrated," vs. São Paulo at 64.4% of revenue, clearly
concentrated). Collapsing these into one metric would hide that split.

**Why revenue-weighted instead of order-count-weighted (a change from the original).**
Order count treats a $20 order the same as a $2,000 order. For a procurement/sourcing
risk question ("how much of our spend is exposed to this region"), revenue is the
correct denominator — it's the actual dollars at risk, not the transaction count.

**Likely interview question:** *"You have two concentration models — why not one
`gold_concentration_risk` with a `grain` column?"*
**Answer:** Because they're genuinely different grains (one row per seller vs. one row
per state) with different column sets — cramming both into one table with a
discriminator column is the kind of "polymorphic table" pattern that makes downstream
queries need to know which grain they're filtering to, which is worse than two small,
clearly-named tables.

**Weakness, stated honestly.** The 20% geographic threshold, like the 10% seller
threshold, is a heuristic, not derived from an established index the way HHI is. It was
kept from the original implementation because it's intuitive (with 23 states
represented, an even split would be ~4.3% each, so 20% is ~4-5x an "even" share) — but
unlike the HHI thresholds, there's no external standard being cited here. Worth naming
that distinction if asked "is this threshold as rigorous as the HHI one" — it isn't.

---

### 7. dbt tests added (there were none before)

**What was built.** `dbt/models/silver/schema.yml` and `dbt/models/gold/schema.yml` add
`unique`/`not_null` tests on every model's primary key, plus `accepted_values` tests on
the two categorical tier columns (`risk_tier`, `freight_burden_tier`). 19 tests total,
all passing (`dbt build`).

**Why.** CLAUDE.md documents `dbt test` as a supported command, but there was nothing
for it to run — an empty test suite is worse than an honest "we don't have tests yet,"
because it silently claims coverage that doesn't exist. The specific tests chosen catch
the two most likely regressions in this kind of aggregation-heavy gold layer: a join
that unintentionally fans out and breaks the "one row per seller" grain (caught by
`unique`), and a `CASE` expression with a gap that lets a tier column go `NULL` (caught
by `not_null` + `accepted_values`).

**Likely interview question:** *"19 tests, all just uniqueness/not-null checks — isn't
that pretty shallow test coverage?"*
**Answer, honestly:** Yes. These are schema-shape tests, not business-logic tests —
they'd catch a broken join or a bad CASE statement, but they wouldn't catch "the
reliability_score formula has a sign error" or "the HHI sum doesn't actually equal
10,000 for a fully concentrated hypothetical." That would need `dbt` singular
(custom SQL) tests asserting specific numeric invariants, which don't exist yet.
Naming this gap directly is a better answer than overstating the coverage.

---

### 8. The Streamlit Cloud sample-data path duplicates the dbt gold-layer logic in Python

**What existed already, and why it's relevant here.** `data/sample_data.py` generates a
small synthetic dataset and hand-builds silver/gold tables directly in SQL via `duckdb`
Python calls, because Streamlit Community Cloud only runs `streamlit_app.py` — it has
no step that shells out to `dbt run`. This predates Phase 1, but every gold-layer change
in this phase had to be mirrored there too, or the deployed Cloud dashboard would read
old-schema tables and break.

**What was done.** `sample_data.py` was updated to rebuild all five gold tables
(`gold_supplier_scorecard`, `gold_concentration_risk`, `gold_geo_concentration`,
`gold_sourcing_cost_drivers`, `gold_executive_summary`) with the *same formulas* as the
dbt models — same reliability-score weights, same HHI calculation, same tier
thresholds — plus synthetic `product_category` / `product_weight_g` fields on the
generated order items so the cost-driver model has something real to compute on.
Verified by running `AppTest` (Streamlit's headless test harness) against both the
real-data DB and a freshly generated sample DB — both render with zero exceptions.

**Why not just install `dbt-duckdb` on Streamlit Cloud and run it there instead of
hand-duplicating the SQL.** Considered. Streamlit Community Cloud's build step can run
arbitrary Python before the app starts, so this is technically possible. It was
rejected for this phase because (a) it would pull dbt-core, dbt-duckdb, and their
dependency tree into the Cloud deploy's install step, meaningfully slowing cold starts
on a free-tier deployment that currently installs a handful of lightweight packages,
and (b) `dbt run` against the *real* Kaggle CSVs isn't an option on Cloud anyway
(`data/raw/` is gitignored and the real CSVs aren't shipped with the repo), so Cloud
would still need the synthetic-data generator regardless — at which point running dbt
on top of synthetic data adds deployment complexity without adding fidelity.

**Likely interview question:** *"You have the same business logic written twice, in
SQL and in Python-embedded-SQL. How do you keep them from drifting apart?"*
**Answer, honestly:** Right now, by hand, and that's a real weak point — there is no
automated check that `sample_data.py`'s formulas match `dbt/models/gold/*.sql`. The
mitigation in place is a comment at the top of `build_sample_db()` pointing back to the
dbt models it mirrors, but a comment doesn't prevent drift. A more robust fix would be a
CI check that diffs the two, or restructuring so both paths call into one shared SQL
template — that's explicitly deferred, not solved, and worth saying so rather than
claiming the duplication is fully handled.

---

### 9. Fixed a silent logging bug in bronze ingestion

**What was found.** `ingestion/load_bronze.py`'s row-count log line used
`log.info("...%,d rows...", row_count)` — Python's `%`-style logging doesn't support
the `,` thousands-separator flag, so every single call to this line raised a
`ValueError` inside the logging module's error handler on every ingestion run. It was
non-fatal (Python's logging module catches formatting errors internally and prints a
traceback to stderr rather than crashing the caller), so ingestion completed
successfully despite it, but every row-count log line was silently broken.

**Why it matters for this phase.** Phase 2 (instrumentation) is explicitly about
having real, citable pipeline numbers — a logging bug that's been silently swallowing
every "rows loaded" message for all nine bronze tables would undermine that from the
start if left in place.

**Fix.** Changed to `log.info("...%s rows...", f"{row_count:,}")` — format the number
with an f-string first, pass the pre-formatted string to `%s`.

**Likely interview question:** *"How did you find this?"*
**Answer:** It surfaced immediately on the first real ingestion run in this session —
`python -m ingestion.load_bronze` printed a full Python traceback for every table
despite the run completing. It's a good example of why you actually run a pipeline
rather than just reading the code: this bug was invisible from a code review of the
format string alone unless you knew `%`-style logging doesn't support comma flags.

---

### 10. Split `requirements.txt` into a Cloud-slim file and `requirements-dev.txt`

**What was found.** CLAUDE.md's documented install step (`pip install -r
requirements.txt`) implied it would install everything needed for local development —
dbt, FastAPI, the decision agent. In reality, root `requirements.txt` had been stripped
down to five packages (streamlit, plotly, duckdb, pandas, python-dotenv) for the
Streamlit Community Cloud deployment, with the full stack commented out below it. That
commented-out block was itself stale: it listed `langchain`, `langchain-anthropic`,
`langchain-openai`, `langchain-community`, and `openai`, none of which
`agent/decision_agent.py` actually imports — the agent uses the `anthropic` SDK
directly with a hand-rolled tool loop, not LangChain. (This LangChain-vs-actual-SDK gap
is a CLAUDE.md/code mismatch, not just a requirements-file mismatch — it's exactly the
kind of claim Phase 4 exists to reconcile against the resume, and it's flagged here for
that pass rather than fixed in Phase 1.)

**What was built.** `requirements.txt` (root) stays slim and Cloud-only, now with a
comment explaining why. A new `requirements-dev.txt` holds the real local-dev stack —
dbt-core, dbt-duckdb, fastapi, uvicorn, anthropic (not langchain), pytest, ruff —
matching what the code actually imports, verified by installing it into a clean venv
and running the full pipeline (`ingestion.load_bronze` → `dbt build` → FastAPI
`TestClient` → Streamlit `AppTest`) against it in this phase.

**Why not just restore the full stack into root `requirements.txt`.** That file is what
Streamlit Community Cloud reads to build the *live, already-deployed* dashboard.
Pushing a change that makes it install dbt-core, dbt-duckdb, fastapi, and their
dependency trees would meaningfully slow — or on a constrained free-tier build,
potentially break — a deployment that currently works, for zero benefit (the Cloud app
only ever runs `streamlit_app.py`, which never touches dbt or FastAPI). Modifying a
live, shared deployment's dependencies without being able to verify the Cloud build
afterward is exactly the kind of action worth avoiding by construction rather than by
carefulness.

**Likely interview question:** *"Why two requirements files instead of one with
optional extras (e.g. `pip install -r requirements.txt[dev]`)?"*
**Answer:** `pip`'s extras mechanism needs a `pyproject.toml`/`setup.py` with defined
extras groups, which this project doesn't have — it's plain `requirements.txt` files.
Given that constraint, two explicit files is more transparent than one file with
inline comments a deploy script has to know to strip; it also means the Cloud
deployment's dependency set is defined by a file that contains *only* what it needs,
not by convention over a shared file.

**Weakness, stated honestly.** Two requirements files for one repo is exactly the kind
of split that drifts if not maintained deliberately — if a new local-dev-only package
is added to `requirements-dev.txt` and someone assumes `requirements.txt` is still the
"real" one, Cloud won't get it (which is usually fine, since Cloud doesn't need
dev-only tooling) but the inverse mistake (adding something Cloud *does* need only to
`requirements-dev.txt`) would silently break the deployed app on the next Cloud
rebuild, with no CI check to catch it before it ships.

---

### Known issue discovered this phase, deliberately deferred to Phase 3

While tracing the `requirements.txt` split (#10 above), `docker/Dockerfile` turned out
to already be broken, independent of anything changed in this phase: it runs
`pip install -r requirements.txt` then `CMD uvicorn api.main:app`, but `requirements.txt`
has not included `fastapi` or `uvicorn` since commit `04005bc` ("cloud deployment setup
with sample data generator"), which trimmed it to the Streamlit-only dependency set.
`docker/Dockerfile` itself dates back to the very first scaffold commit (`098b86e`) and
was never updated when that trim happened five commits later — so the container image
has been unbuildable-and-runnable (it builds, but `uvicorn` isn't on `PATH` when the
`CMD` fires) since that commit, unnoticed because nobody had run `docker compose up`
against it since. This is explicitly left as-is here and will be fixed as part of
Phase 3 (containerizing the FastAPI service for AWS), not patched in passing — a
Dockerfile fix half-done alongside a gold-layer phase is exactly the kind of
scope-creep that hides the actual fix in an unrelated diff.

---

## Phase 1 — real numbers (for citation)

Computed via `dbt build` against the full Olist dataset, current as of this phase.
Re-run `dbt build` and re-query `gold.gold_executive_summary` before citing these in a
context where they need to be exactly current — they will shift slightly if models
change.

| Metric | Value |
|---|---|
| Orders processed | 100,010 (order-item grain rollup; 99,441 distinct orders) |
| Suppliers (sellers) scored | 3,095 |
| Product listings | 34,448 |
| Total revenue (order-item price sum) | $13,591,643.70 |
| Overall late-delivery rate | 7.84% |
| Average reliability score | 84.7 / 100 |
| High-risk suppliers | 8.79% (272 of 3,095) |
| Average review score | 4.09 / 5 |
| Supplier-level HHI | 35.68 (unconcentrated) |
| Top-5 supplier revenue share | 7.61% |
| Geographic concentration (São Paulo) | 64.40% of revenue |
| Product categories scored | 66 (of ~73, min-30-items filter) |
| dbt models | 8 (3 silver, 5 gold) |
| dbt tests | 19, all passing |

---

## Phase 2 — Instrumentation

### 1. A dedicated `scripts/pipeline_report.py` instead of scattering print statements

**What was built.** A standalone, re-runnable script that reads real numbers straight
from the running system — `information_schema` row counts per bronze/silver/gold
table, dbt's own `run_results.json` for per-model timing, and the gold layer for
business/flagging metrics — and both prints a console report and writes a JSON
snapshot (`reports/pipeline_metrics.json`). It's the thing that actually produced every
number in this document's "real numbers" tables; those numbers weren't hand-typed from
eyeballing query results.

**Why a separate script instead of, say, printing more inside `dbt run` or the
dashboard.** Three different audiences want these numbers at three different times: a
resume/portfolio context wants a point-in-time snapshot on demand, not embedded in a
running dashboard session; a `dbt run` hook would only ever see dbt-layer facts (model
timing, test pass/fail) and has no natural way to also report bronze ingestion volume
or gold-layer flagging rates, which live outside dbt's own bookkeeping; and putting
this logic in the dashboard would mean every dashboard page load re-runs a
full-database scan just to produce numbers nobody asked for on that request. A
standalone script run on demand (or on a schedule, if this were wired into CI) is the
simplest thing that serves all three needs without coupling them.

**Why it parses dbt's `run_results.json` instead of timing dbt itself.** dbt already
measures per-model execution time precisely (compile + execute phases, wall-clock, per
node) and writes it to `dbt/target/run_results.json` after every `dbt run`/`dbt
build`. Re-implementing that by wrapping `subprocess.run(["dbt", "build"])` with an
external timer would produce a strictly worse number (subprocess startup overhead
included, no per-model breakdown) for more code. Reading the artifact dbt already
produces is both less code and more accurate.

**Alternatives considered.**
- *A proper metrics/observability stack (Prometheus + Grafana, or even just writing
  metrics to a `metrics` table in DuckDB on every run for time-series tracking).*
  Rejected for this project's scope — this is a batch, on-demand pipeline for a
  portfolio project, not a production service with continuous traffic to monitor. A
  snapshot report answers "what are the real numbers right now," which is what's
  actually needed here; a full observability stack would be true over-engineering for
  a project whose entire "production" surface is a single Streamlit dashboard and one
  FastAPI service.
- *Logging via Python's `logging` module (matching the pattern already used in
  `ingestion/load_bronze.py`) instead of a JSON file.* Considered, and partially
  adopted — bronze ingestion's own row/volume/throughput line (see #2 below) does use
  `logging`, because it's a natural coda to a log stream that's already running.
  `pipeline_report.py`, though, is meant to be read as a snapshot document (or fed to
  another tool), not tailed as a log — JSON output plus a formatted console print
  serves that better than a log line that scrolls off within one run.

**Likely interview question:** *"Why write a JSON file at all — why not just read the
console output?"*
**Answer:** Because the console output is for the person running the script right now;
the JSON file is for everything else — a CI step that wants to assert
`flagging_rates.high_risk_suppliers.flag_rate_pct < 15`, a future dashboard widget that
wants to show "last computed at," or just being able to `git diff` two runs of the
report to see what changed after a model edit, none of which parsing terminal output
would support cleanly.

**Weakness, stated honestly.** This is a snapshot tool, not a monitoring system — it
has no history. Running it twice overwrites `reports/pipeline_metrics.json` with no
trend line, so "is our late-delivery rate getting better or worse over time" isn't
answerable from this alone; you'd need to either keep timestamped copies or push each
run's JSON somewhere with retention (a small metrics table in DuckDB, or an external
time-series store) to get that. That's a deliberate, named scope cut, not an oversight
— building trend tracking for a single-database, single-environment portfolio project
would be solving a problem this project doesn't have yet.

---

### 2. Bronze ingestion instrumentation: rows, volume, elapsed time, throughput

**What was built.** `ingestion/load_bronze.py`'s `load_all()` now times the whole
ingestion run, sums the byte size of every CSV actually loaded, and logs one summary
line: total rows ingested, input volume in MB, elapsed wall-clock seconds, and
throughput in rows/second. On the real Olist dataset this reads: **1,550,922 rows,
120.3 MB input, 2.16s elapsed, ~718K rows/sec.**

**Why throughput and not just a row count.** A raw row count ("we ingested 1.5M rows")
is a volume claim; throughput ("718K rows/sec") is a claim about the pipeline's
engineering, which is the more interesting fact for a resume bullet or an interview —
and it's a genuinely fair one to make here, because DuckDB's `read_csv_auto` is doing
a bulk columnar load, not a row-by-row insert loop, so the number reflects the tool
choice as much as the code around it (see weakness below).

**Why measure elapsed time around the whole `load_all()` call instead of per-table.**
Per-table timing would be more granular, but the per-table row counts already printed
give a reasonable proxy for where time goes (`geolocation` at 1M rows dominates), and
adding a second timer per table for nine tables that together finish in ~2 seconds
is precision this project doesn't need. If ingestion later grew to include the UN
Comtrade/World Bank API sources (which involve network I/O, not just local CSV reads),
per-source timing would become worth the extra code — for CSV-only bronze loading, one
aggregate number is enough.

**Likely interview question:** *"718K rows/sec sounds impressive — is that a fair
number, or is it doing something trivial?"*
**Answer, honestly:** It's fair as a description of what happened, but it's not a
claim about a custom high-performance ingestion engine — it's DuckDB's native
`read_csv_auto` (a vectorized, columnar bulk loader) doing what it's built to do,
called from nine `CREATE TABLE AS SELECT` statements. The engineering credit here is
"picked a columnar engine well-suited to full-replace batch loads and used its bulk
path instead of a Python loop," not "wrote a custom fast CSV parser." That's still a
legitimate, defensible design choice to describe in an interview — it's just important
not to overstate it as bespoke performance engineering.

**Weakness, stated honestly.** The throughput number is specific to this machine, this
DuckDB version, and this exact dataset shape (mostly narrow, well-typed columns) — it's
not a portable benchmark. It would be a mistake to quote "718K rows/sec" as if it
generalizes to, say, a wide table with many string columns or a slower disk. Treat it
as "this pipeline, on this data, ingests fast enough that ingestion time is a non-issue
at this scale" rather than as a universal throughput claim.

---

### 3. Gold-layer flagging/detection rates as first-class reported metrics

**What was built.** `pipeline_report.py` computes, for each of the four gold models
with a HIGH/flag column, the fraction of rows that actually tripped the flag:
high-risk suppliers, single-supplier-dependency, geographic concentration, and
high-freight-burden categories. On the real dataset:

| Flag | Flagged / Total | Rate |
|---|---|---|
| High-risk suppliers (`risk_tier = HIGH`) | 272 / 3,095 | 8.79% |
| Single-supplier dependency (`concentration_flag = HIGH`, seller-level) | 0 / 3,095 | 0.0% |
| Geographic concentration (`concentration_flag = HIGH`, state-level) | 1 / 23 | 4.35% |
| High freight-burden categories | 13 / 66 | 19.7% |

**Why report these as rates, not just raw counts.** A raw count ("272 high-risk
suppliers") is meaningless without the denominator — 272 out of 3,095 is a very
different story than 272 out of 400. Reporting the rate alongside the count is what
makes these numbers usable in a sentence like "the model flags roughly 1 in 11
suppliers as high-risk," which is the kind of claim a resume bullet or a stakeholder
conversation actually wants.

**Why this matters as the payoff of Phase 1's work, specifically.** Phase 1's DECISIONS
entries are full of "here's why this threshold, grounded in the real distribution" —
this is where that grounding gets checked end-to-end: the 0.0% single-supplier flag
rate is the same finding as Phase 1's HHI = 35.68 finding, arrived at independently
through a different query path (a per-row flag count here vs. a portfolio-level sum
there). Both landing on "this dataset shows no individual-supplier concentration risk"
is a small but real consistency check that the two models (the flag threshold and the
HHI calculation) agree with each other.

**Likely interview question:** *"A 0% single-supplier-dependency flag rate — is that a
metric that's just always going to read zero, making it useless?"*
**Answer:** For *this* dataset's shape (a many-seller marketplace), yes, it's likely to
stay near zero — that's the same honest limitation named in Phase 1 for the HHI metric,
and it's worth naming again here rather than treating a boring number as a bug.
The metric would behave very differently, and usefully so, in a dataset shaped like a
real enterprise supply chain with a handful of strategic vendors — which is the
intended use case, not marketplace analytics specifically.

**Weakness, stated honestly.** All four flagging rates are computed against a single
snapshot of the data — there's no historical comparison ("is the high-risk-supplier
rate trending up"), so a stakeholder asking "is this getting better or worse" can't be
answered from this report alone (same limitation as #1 above: no time-series
retention). It also means these rates are exactly as sensitive to the threshold choices
documented in Phase 1 as the underlying gold models are — if those thresholds are
revisited, these percentages move without any change in the underlying supplier
behavior, which is a distinction worth being explicit about if these numbers are ever
quoted outside the context of "using this project's specific thresholds."

---

## Phase 2 — real numbers (for citation)

Produced by `python -m scripts.pipeline_report` against the full Olist dataset, current
as of this phase. Re-run the script before citing these where they need to be exactly
current.

| Metric | Value |
|---|---|
| Rows ingested (bronze) | 1,550,922 across 9 tables |
| Raw input volume | 120.3 MB (CSV) |
| DuckDB file size | 98.0 MB |
| Ingestion elapsed time | 2.16s |
| Ingestion throughput | ~718,000 rows/sec |
| dbt models run | 8 (0 failed) |
| dbt tests run | 19 (0 failed) |
| dbt total elapsed time | 0.85s |
| Suppliers scored | 3,095 |
| High-risk suppliers flagged | 272 (8.79%) |
| Single-supplier dependency flagged | 0 (0.0%) |
| Geographic concentration flagged | 1 of 23 states (4.35%) |
| High freight-burden categories flagged | 13 of 66 (19.7%) |

---

## Phase 3 — AWS Deployment

### 1. Fixed the broken Dockerfile by splitting it in two, instead of patching it

**What was built.** The single `docker/Dockerfile` (broken since commit `04005bc`,
flagged and deliberately deferred in Phase 1) is now two images:
`docker/Dockerfile.api` (FastAPI + the decision agent, built from a new,
narrowly-scoped `requirements-api.txt`) and `docker/Dockerfile.dashboard` (Streamlit,
reusing the existing Cloud-slim root `requirements.txt` unchanged). `docker-compose.yml`
now points each service at its own Dockerfile.

**Why split instead of just fixing the one Dockerfile's requirements reference.** The
API and dashboard have genuinely different dependency needs — the API needs
`anthropic` + `boto3` (for the S3 snapshot fetch, see #3) and nothing UI-related; the
dashboard needs `streamlit`/`plotly` and nothing server-side. A single shared image
installing the union of both dependency sets would work, but it means: a change to the
dashboard's dependencies forces a rebuild of the API image too (and vice versa), the
API image ships with plotly/streamlit it never imports, and — the concrete reason this
matters for Phase 3 specifically — the image being pushed to ECR and deployed to AWS
App Runner should be as small and single-purpose as reasonably possible, since it's
the one with a real cold-start cost on a billed, autoscaling service.

**Alternatives considered.**
- *One Dockerfile, one requirements file, `ARG`/`ENV` to select which command runs
  (matching the original design's minimal-diff intent).* Rejected — this was
  literally the design that broke silently for 5 commits (Phase 1 found it), because
  nothing forced the shared requirements file to stay a superset of both services'
  needs as one side's needs changed independently.
- *A multi-stage Dockerfile building both images from one file with target stages.*
  Considered — Docker supports this (`docker build --target api`) — but two small,
  independently-readable Dockerfiles are more legible for a project whose Docker
  setup exists partly to be read and explained in an interview, not just to build
  correctly.

**Likely interview question:** *"Why not just add `fastapi` and `uvicorn` back to the
one requirements.txt and call it fixed?"*
**Answer:** Because `requirements.txt` (root) is deliberately the Streamlit Cloud
deploy manifest (Phase 1, decision #10) — putting FastAPI's dependencies back into it
would re-couple two things that were just decoupled for a good reason, and would mean
every Streamlit Cloud rebuild installs `anthropic`/`boto3`/`fastapi` it never uses.

**Weakness, stated honestly.** This was verified without Docker itself — no `docker`
binary was available in the environment this was built in (checked; not installed).
Instead, `Dockerfile.api`'s exact file set (`api/`, `agent/`, `fetch_db.py`,
`requirements-api.txt`) was copied into an isolated directory, installed into a clean
venv from scratch, and run with `uvicorn api.main:app` against the real DuckDB file —
confirming `/health` and `/suppliers` both respond correctly with *only* the files and
dependencies the Docker image would actually contain. That's strong evidence the image
would build and run correctly, but it is not the same as an actual `docker build` +
`docker run`, which could still surface something this simulation can't (a base-image
quirk, a missing system library `pip` silently needed, etc.). Flag this distinction if
asked "did you test the actual container" — the honest answer is "I tested an exact
simulation of its contents, not the container runtime itself."

---

### 2. AWS App Runner over ECS Fargate, Lambda, or EKS

**What was built.** `aws/terraform/main.tf` provisions an `aws_apprunner_service` for
the FastAPI container, backed by an ECR repository.

**Why App Runner specifically.** The resume claim being reconciled (Phase 4) is
"FastAPI on Cloud Run" — Cloud Run is GCP's fully-managed container platform: no
cluster to provision, no load balancer to wire up, HTTPS and autoscaling by default,
and you pay for a single container's compute. App Runner is AWS's direct equivalent in
that same category. Choosing it over ECS/EKS isn't just "the easy option" — it's the
option that actually mirrors the architecture being claimed, which matters if an
interviewer asks "how does this compare to what you deployed on GCP."

**Alternatives considered.**
- *ECS Fargate.* The more common "serverless containers" choice at companies already
  invested in ECS, and more configurable (VPC placement, ALB path routing, service
  mesh). Rejected for this project specifically because it requires provisioning a VPC,
  subnets, a load balancer, and a cluster — meaningfully more infrastructure to stand
  up and explain for a single-container service with no need for custom networking.
  Worth naming as "what I'd use in a real production system moving toward
  multi-service architecture," which App Runner isn't really built for.
- *AWS Lambda (with a container image or a Lambda-adapter shim for FastAPI, e.g.
  Mangum).* Rejected — Lambda's request/response model and cold-start behavior fit
  bursty, event-driven workloads better than a service that (in the docker-compose
  setup) already runs as a long-lived process; adapting FastAPI to Lambda's handler
  interface (via Mangum) would also mean the same codebase behaves differently
  locally (`uvicorn`) vs. deployed (Lambda handler), which is exactly the kind of
  environment-parity gap this phase is trying to avoid, not introduce.
- *EKS.* Rejected outright as disproportionate — running a Kubernetes control plane
  for one container is the textbook over-engineering example, not a defensible choice
  here.

**Likely interview question:** *"App Runner is a pretty niche AWS service compared to
ECS/EKS — why would a company actually use it?"*
**Answer:** It's the right fit for exactly this shape of workload: one (or a handful
of) stateless HTTP service(s) that don't need custom networking, service mesh, or
fine-grained scaling policies. It's less commonly reached for at larger orgs mainly
because they're already running ECS/EKS for other services and adding a third
container platform has its own cost — that's an organizational-inertia argument, not a
technical one, and worth being upfront that it is a smaller, less-configurable service
than Fargate if pushed on the comparison.

**Weakness, stated honestly.** App Runner has real limits worth knowing if asked: no
VPC-native networking without an additional VPC connector, less granular autoscaling
control than Fargate + Application Auto Scaling, and it's a smaller service with less
community tooling/Terraform-module coverage than ECS. For a single FastAPI service
backed by a read-only DuckDB snapshot, none of those limits bite — but they would for
a more complex deployment, and that boundary is worth being able to name.

---

### 3. S3 data layer: Parquet per table, plus a separate whole-file DuckDB snapshot

**What was built.** `scripts/export_to_s3.py` writes three distinct things to S3 (or,
for testing, a local directory — same code path, see #5): every bronze/silver/gold
table as a standalone Parquet file (`<dest>/<schema>/<table>.parquet`), the entire
`.duckdb` file as one object, and (optionally, off by default) the raw Olist CSVs.

**Why both Parquet *and* a whole-file snapshot, rather than just one.** They serve
different consumers. Parquet-per-table is the actual "data lake" artifact — it's
queryable by Athena, Redshift Spectrum, or another DuckDB process
(`read_parquet('s3://...')`) without this project's code involved at all, which is
what makes "the data layer is on S3" a meaningful, standards-based claim rather than
"we uploaded a binary blob only our own app can read." The whole-file DuckDB snapshot
exists for a narrower, practical reason: it's what the containerized API fetches on
cold start (`docker/fetch_db.py`, see #4) — pulling one ~100MB file over the network
at startup is simpler and faster than having the API reconstruct its existing local-
file query patterns against remote Parquet on every request.

**Measured Parquet compression, for citation.** On the real dataset: the bronze layer
(9 tables, 120.3 MB as CSV) exports to 53 MB as Parquet; the gold layer (5 tables,
6,280 rows total) exports to 392 KB. Columnar encoding + compression roughly halves
the bronze footprint and makes the gold layer effectively free to store — a fair,
measured claim about *this* dataset's compressibility, not a general "Parquet is N%
smaller" rule (compression ratio depends heavily on column cardinality and repetition,
which varies a lot by table).

**Why DuckDB's `httpfs` extension instead of writing Parquet locally and uploading
with boto3.** DuckDB can `COPY table TO 's3://bucket/key.parquet' (FORMAT PARQUET)`
directly once `httpfs` is loaded — one SQL statement per table, no intermediate local
file, no separate upload step. Round-tripping through a local temp file and then
`boto3.upload_file()` would be more code to do the same thing, and would need explicit
cleanup of the temp files. The cost of this choice is that it's DuckDB's own S3 client
talking to AWS, not boto3 — which is exactly why the test for this path (see #5) had
to be structured differently from the boto3-based upload paths.

**Alternatives considered.**
- *Only the whole-file DuckDB snapshot, skip Parquet entirely.* Simpler, and would
  have been enough to satisfy "the containerized API can fetch its data from S3."
  Rejected because it wouldn't be a real "data layer in S3" in the sense a data
  engineering interview would probe — a single proprietary binary file isn't
  interoperable with the standard tools (Athena, Spark, another team's pipeline) that
  "data lake on S3" usually implies.
- *Delta Lake or Iceberg table format instead of plain Parquet.* Considered and
  rejected as disproportionate for this project's scale (a handful of tables, batch-
  refreshed, no concurrent writers, no need for time-travel queries or schema
  evolution across a long table history) — those formats solve problems (concurrent
  writes, ACID transactions over object storage, partition evolution) this project
  doesn't have. Plain Parquet is the honest choice for the actual requirements here,
  not a compromise.

**Likely interview question:** *"If you already have a DuckDB snapshot the API can
read directly, why bother with the Parquet export at all — isn't that duplicated
effort maintaining two representations of the same data?"*
**Answer:** They're not redundant, they're for different audiences: the DuckDB
snapshot is this project's own internal serving format; the Parquet export is what
makes the data layer usable by tools and teams that have never heard of this project's
codebase. That's a real, common pattern — an application-specific serving store next
to a standards-based data lake export — and naming that distinction clearly is a
better answer than either dropping one or pretending they're the same thing.

**Weakness, stated honestly.** Both artifacts are populated by the same manual
trigger (`scripts/export_to_s3.py`, run by a human or a CI step) — there's no
automatic sync keeping S3 up to date every time `dbt build` runs locally. If someone
runs the pipeline locally and forgets to also run the export, the S3 copy silently
goes stale relative to local. This project doesn't yet wire the export into any kind
of scheduled job or dbt post-hook; that's a deliberate scope cut for a project without
a CI/CD pipeline yet (see #6), not something already solved.

---

### 4. Cold-start S3 fetch (`fetch_db.py`) instead of querying Parquet live from the API

**What was built.** `docker/fetch_db.py` runs once, before `uvicorn` starts (chained
in `Dockerfile.api`'s `CMD`): if `DUCKDB_S3_URI` is set, it downloads that object to
the local `DUCKDB_PATH` via boto3; if unset, it's a complete no-op. The API's own code
(`api/routers/suppliers.py`, etc.) is entirely unchanged — it still just opens a local
`.duckdb` file read-only, exactly as it always has.

**Why fetch-once-at-startup instead of having the API query S3-hosted Parquet
directly on every request (via the same `httpfs` mechanism used for the export).**
Three reasons. First, it means zero changes to any existing query code — every router,
every SQL string, stays exactly as written for local dev; only a new pre-start step
was added. Second, latency: a local DuckDB file backing every API request is
consistently fast; querying Parquet over the network on every request adds S3
round-trip latency to every single API call, not just a one-time cold-start cost.
Third, it composes cleanly with App Runner's own scaling model — every new instance
that spins up fetches the current snapshot once, independent of how many instances are
running, rather than every request from every instance separately touching S3.

**The real cost of this choice, named directly.** The API's view of the data is only
as fresh as the last `export_to_s3.py` run *and* the last container start/redeploy —
if the underlying gold tables change, already-running containers keep serving the old
snapshot until they're redeployed (`auto_deployments_enabled = false`, see #6, makes
this an explicit, not automatic, step). A live-Parquet-query design would give every
request current data at the cost of the latency and request-time S3 dependency
described above. This is a real, named tradeoff — batch freshness for consistent
low-latency reads — not a limitation that went unnoticed.

**Likely interview question:** *"What happens if `fetch_db.py` fails — a bad
`DUCKDB_S3_URI`, missing IAM permission, network blip during a deploy?"*
**Answer:** It's designed to fail loudly, not silently: any exception during the fetch
logs the full error and calls `sys.exit(1)`, which means the container process exits
before `uvicorn` ever starts, which in turn means App Runner's health check
(`/health`, `path` in the Terraform health-check block) never passes and the
deployment is marked failed rather than serving a container with no data. That's a
deliberate choice — a container silently serving empty query results because its data
never arrived would be a much worse failure mode than a visibly failed deployment.

**Weakness, stated honestly.** There's no retry logic — one transient network error
during the S3 download fails the whole container start rather than retrying with
backoff. For a small, infrequently-redeployed service this is an acceptable trade
(a failed deployment can just be retried by re-triggering `start-deployment`), but it
would be worth hardening with a few retries before failing outright if this pattern
were used for a service that redeployed frequently or auto-scaled aggressively (every
new instance's cold start would be a fresh chance to hit a transient failure).

---

### 5. Testing AWS-integrated code without a real AWS account

**What was built.** Two test files exercise the new AWS-touching code without
touching AWS: `tests/test_export_to_s3.py` and `tests/test_fetch_db.py`, using `moto`
(`@mock_aws`) to intercept boto3 calls against an in-process fake S3, plus a
local-filesystem-destination test for the DuckDB `httpfs` Parquet export path that
moto can't reach (see below for why).

**Why moto instead of hitting real AWS in tests, or not testing this code at all.**
Real AWS calls in a test suite mean tests are slow, cost money, require credentials to
even run (breaking CI for anyone without an AWS account configured), and can fail for
reasons unrelated to the code under test (throttling, transient network issues,
region outages). Not testing at all was the other option, and was rejected because
this is exactly the kind of code — file paths, S3 key construction, URI parsing —
where a typo (`.rstrip('/')` in the wrong place, a bucket/key swap) is easy to
introduce and easy to catch with a fast, free, in-process test. `moto` mocks boto3 at
the HTTP layer, so the actual `boto3.client("s3").upload_file(...)` calls in
`export_db_snapshot` and `export_raw_csvs` run for real against a fake backend —
that's a meaningfully stronger test than mocking `boto3` itself out with `unittest.mock`,
since it also catches a wrong parameter name or malformed call, not just "was this
function called."

**Why the Parquet export path is tested differently, and what that gap means.**
`export_parquet()` uses DuckDB's own `httpfs` extension to talk to S3 — DuckDB ships
its own HTTP-based S3 client written in C++, not boto3 — so `moto`'s boto3
interception has nothing to intercept there. That function is instead tested by
pointing `dest` at a local filesystem path, which runs through the *identical*
`COPY "{schema}"."{table}" TO '{target}' (FORMAT PARQUET)` SQL DuckDB would run
against `s3://...`, differing only in the destination URI scheme. This confirms the
per-table iteration, path construction, and Parquet output are all correct. What it
does **not** confirm is that DuckDB's `httpfs` S3 client, this project's credential-
configuration statements (`SET s3_access_key_id=...` etc. in `_configure_httpfs`), and
a real S3 endpoint all work together correctly — that would require either a real AWS
account or a local S3-compatible server (MinIO), and neither was available in the
environment this was built in (no Docker, no AWS credentials — see the top-level
caveat in `aws/README.md`).

**Likely interview question:** *"Your tests never actually touch S3's real HTTP API —
doesn't that mean you could ship a bug in the httpfs credential configuration and
never know until it's in front of a real AWS account?"*
**Answer, honestly:** Yes, and that's the accurate boundary of what was verified here.
The mitigation in place is that `_configure_httpfs`'s statements
(`SET s3_region=...`, `SET s3_access_key_id=...`) are DuckDB's documented, standard
httpfs configuration calls, used exactly as documented — but "used correctly per the
docs" and "verified against a real S3 bucket" are different claims, and only the first
one is true right now. The honest next step, if this needed to be fully verified
without a real AWS account, would be spinning up a local MinIO container (S3-API-
compatible) and pointing `httpfs` at it — not done here because Docker wasn't
available in this environment either.

---

### 6. `auto_deployments_enabled = false`, and no CI/CD pipeline yet

**What was built.** The App Runner service's `auto_deployments_enabled` is explicitly
`false` — pushing a new image tag to ECR does not, by itself, trigger a redeploy.
Deploys happen only via the explicit `aws/deploy.sh` script or a manual
`aws apprunner start-deployment` call.

**Why off by default.** Two reasons, one about safety and one about this project's
specific cold-start design. Safety: auto-deploy-on-push means anyone (or any script)
with ECR push access can silently roll out a change to the running service with no
review step in between — turning that on is a decision a team should make
deliberately, not a default. Specific to this project: because the container fetches
whatever object currently sits at `DUCKDB_S3_URI` on every cold start (#4), an
auto-triggered redeploy could pick up a data snapshot that was mid-upload or
untested, silently changing what the API serves with no code change at all having
happened. An explicit deploy step is also the natural place to sequence "export fresh
data, *then* redeploy" correctly (see `aws/deploy.sh`), which an automatic trigger
watching only ECR pushes couldn't coordinate.

**Why there's no CI/CD pipeline (GitHub Actions, etc.) automating any of this.**
Named directly as a scope cut, not an oversight: wiring `aws/deploy.sh` into GitHub
Actions on every merge to `main` would need repository secrets for AWS credentials,
which is a real security surface to set up correctly (scoped IAM user, OIDC federation
instead of long-lived keys ideally) — worth doing for a project that's actually
operated by a team, disproportionate to build out for a project deployed by one person
running one script by hand when they choose to.

**Likely interview question:** *"So how would a data refresh actually reach
production today?"*
**Answer:** Manually, by design at this stage: re-run the local pipeline
(`ingestion.load_bronze` → `dbt build`), run `scripts.export_to_s3` to push the
refreshed gold layer to S3, then explicitly trigger an App Runner deployment (either
`aws/deploy.sh` end-to-end, or just the `aws apprunner start-deployment` step if only
data changed, not the image). That's an accurate description of a small,
single-operator batch pipeline's actual deploy story — the honest next step toward a
team-operated version would be a scheduled GitHub Actions job doing exactly this
sequence on a cron trigger, using OIDC-federated AWS credentials rather than static
keys.

**Weakness, stated honestly.** This is the same "no automation, manual trigger"
pattern already named as a limitation in Phase 2 (no scheduled re-runs of the
instrumentation report) and Phase 1 (`sample_data.py` drift risk against the dbt
models) — a recurring, consistent gap across this project: everything here is
correct and re-runnable, but nothing runs itself yet. That's a fair, single sentence
to have ready if asked "what's the biggest thing missing from this project as a
whole": scheduled/automated execution, end to end.

---

## Phase 3 — real numbers (for citation)

Produced by running `scripts/export_to_s3.py` against a local directory (verifying the
export logic without needing a real AWS account — see decision #5 above for why) and
by simulating `Dockerfile.api`'s exact contents in an isolated directory (decision #1).

| Metric | Value |
|---|---|
| Bronze layer as Parquet (9 tables) | 53 MB (from 120.3 MB CSV — see #3) |
| Gold layer as Parquet (5 tables) | 392 KB |
| DuckDB snapshot size | 98.0 MB |
| API image dependency count (`requirements-api.txt`) | 7 top-level packages |
| Dev stack dependency count (`requirements-dev.txt`) | 16 top-level packages |
| New AWS-integration tests | 6 (4 export, 2 fetch), all passing via moto |
| Terraform resources defined | 11 (S3 bucket + 3 sub-resources, ECR repo, 2 IAM roles + 2 policy attachments, App Runner service, `aws_caller_identity` data source) |
| `terraform validate` | Passing (schema-verified against AWS provider ~> 5.0; not applied — no AWS credentials in this environment) |

---

## Phase 4 — Resume Reconciliation

The resume line being reconciled: *"DuckDB pipeline ingesting UN Comtrade + World
Bank APIs, dbt bronze/silver/gold, FastAPI on Cloud Run."* Read literally, clause by
clause, against the actual repo state at the start of this phase:

| Claim | State before Phase 4 | State after Phase 4 |
|---|---|---|
| DuckDB pipeline | True | True (unchanged) |
| dbt bronze/silver/gold | True | True (now 13 models, up from 8 — Phase 1 built the Olist gold layer; this phase added the Comtrade/World Bank silver+gold layer) |
| ...ingesting UN Comtrade API | **False** — script existed, had a URL-structure bug that 404'd on every call, no CLI entrypoint, never once run end-to-end, nothing downstream consumed it | **True** — bug fixed, runs against a real (free, no-key) endpoint, loads real Brazil trade data, feeds a gold model |
| ...ingesting World Bank API | **Partially false** — script's core logic was actually correct, but had no CLI entrypoint and had never been run; "ingesting" implies an operating pipeline, not dormant code | **True** — CLI added, hardened against real observed timeouts/slowness, run against the live API, loads real LPI data, feeds the same gold model |
| FastAPI on Cloud Run | **False** — zero GCP artifacts existed anywhere in the repo; nothing to reconcile, only to build | **Built, not deployed** — see decision #5 below for exactly what "built, not deployed" means and doesn't mean |

Two more mismatches were found during this engagement (flagged in Phase 1's closing
notes) and are fixed here as part of the same "make the repo tell the truth" mandate,
even though they weren't in the literal resume line: the agent architecture being
mis-labeled as LangChain (decision #6), and the dashboard's AI assistant being a
non-functional placeholder despite CLAUDE.md describing it as calling the API
(decision #7).

### 1. UN Comtrade ingestion was never-run, broken code — found and fixed

**What was found.** `ingestion/comtrade.py` had no `if __name__ == "__main__":` block,
no CLI, and nothing else in the codebase imported or called it — not even a test. It
had, in other words, never been executed end-to-end by anyone, ever. Testing it for
the first time (this phase) immediately surfaced a real bug: `fetch_trade_flows()` put
`typeCode`, `freqCode`, and `clCode` in the query string
(`?typeCode=C&freqCode=A&clCode=HS&...`), but Comtrade's v1 API — both the free
preview and paid data tiers — expects those three as **path segments**
(`/get/C/A/HS?period=...`). Every single call this function could ever have made
would have 404'd. This was verified directly: hitting the old query-string URL
returned `404`; hitting the corrected path-based URL with an invalid key returned
`401` (routing succeeded, only auth failed) — a live, reproducible before/after.

**Why this went unnoticed for the length of the whole project.** Nothing forced it to
be noticed. The function had a default `COMTRADE_API_KEY=your_comtrade_api_key_here`
placeholder in `.env`, no CLI wrapper existed to invite anyone to just try running it,
`dbt/models/silver/sources.yml` declared `comtrade_trade_flows` as a source (so it
*looked* wired in) but no silver or gold model ever referenced it, and `dbt build`
succeeds either way since an unreferenced source isn't an error. Every signal a
casual reader would check ("is there a source declaration? yes") suggested it worked;
the one signal that would have caught it (actually running it) had no entrypoint to
run.

**What was built.** A `main()` CLI (`python -m ingestion.comtrade`), a fix to the URL
construction, a retry-with-backoff for the free preview API's tight undocumented rate
limit (also discovered empirically during this phase — a second call within seconds
of the first returned `429`), and — critically — actual downstream consumption:
`silver_comtrade_trade_flows.sql` and `gold_trade_balance.sql` (see #4).

**Alternatives considered for the auth model.** The free `/public/v1/preview`
endpoint (no key, ~500-record cap, limited recent window) was made the default,
falling back to the full `/data/v1/get` endpoint automatically once a real
`COMTRADE_API_KEY` is present (`_has_real_api_key()` checks for the literal
placeholder string patterns from `.env.example`, not just non-empty). This was chosen
over *requiring* a real key because a working, runnable-by-default ingestion script is
strictly better evidence for "I actually built and ran this" than a script gated
behind a credential nobody reading the repo can obtain — and the free tier is real,
current UN data, not mocked or fabricated.

**Likely interview question:** *"How did you find this bug, and how do you know it's
actually fixed and not just differently broken?"*
**Answer:** By doing the thing the original code apparently never did — running it
against the real API and reading the actual HTTP response. The before-state (404) and
after-state (401 with a fake key, 200 with the free tier) are both real, reproducible
HTTP responses captured during this phase, not inferred from reading the code. The
final confirmation is the actual data sitting in `bronze.comtrade_trade_flows` right
now: 19 real rows for Brazil's 2023 imports+exports, and the reported FOB export/import
totals ($339.7B / $240.8B) match Brazil's actual reported 2023 trade figures within
the precision of a sanity check — a fabricated or still-broken pipeline wouldn't
produce numbers that happen to line up with reality.

**Weakness, stated honestly.** The free preview API's rate limit is undocumented and
was reverse-engineered empirically (a handful of requests before a 429, recovering
within seconds) — the retry-with-backoff in `fetch_trade_flows()` is tuned against
that observed behavior, not a documented SLA, and could need adjustment if the actual
limit is stricter or looser than what a few manual test calls revealed.

---

### 2. World Bank LPI ingestion: correct logic, still never run — hardened, not rewritten

**What was found.** Unlike Comtrade, `world_bank_lpi.py`'s core request/pagination
logic was actually correct on first real execution — but it also had no CLI
entrypoint and had never been run. Running it for the first time this phase surfaced
a real, reproducible reliability issue rather than a logic bug: requests to
`api.worldbank.org` are slow and occasionally time out from the network this project
was built on — a single all-countries page (per_page=1000) measured at 30-45 seconds,
and a 5-country scoped pull (5 countries x 7 indicators) took roughly 7.5 minutes
wall-clock, well past `requests`' original 30-second timeout.

**What was built.** A `main()` CLI (`python -m ingestion.world_bank_lpi`, with a
`--countries` flag to scope a pull for fast iteration instead of the slow
all-countries default), the timeout raised from 30s to 90s, and a 3-attempt retry
with backoff (`_get_with_retry`) around every request — because a transient timeout
mid-pull previously meant losing all prior progress in that `fetch_indicator` call.

**Why raise the timeout instead of just accepting failures and retrying immediately.**
A request that's going to succeed in 40 seconds and gets a 30-second timeout will
fail on every retry attempt just as reliably as the first try — retrying doesn't fix
a timeout that's shorter than the actual response time, it just wastes the retry
budget. The timeout was raised to a value with real margin over the slowest observed
response (90s vs. a measured worst case of ~45s for one page) *and* retry logic was
added for genuine transient failures (a dropped connection, a momentary spike) — two
different problems, two different fixes, not one fix papering over both.

**Likely interview question:** *"7.5 minutes for 5 countries — is that going to work
for the full all-countries pull the resume claim implies?"*
**Answer, honestly:** It would complete, but slowly — extrapolating from the measured
per-request timing (not a real end-to-end all-countries run, which wasn't executed in
this session; see the real-numbers table), a full pull across ~217 economies x 7
indicators would plausibly take significantly longer than the 5-country scoped run,
likely tens of minutes. That's an acceptable cost for a batch job that runs
occasionally, not per-request — but it's exactly why `--countries` exists as a
first-class flag rather than an afterthought: fast, scoped iteration for development,
full pulls reserved for an actual scheduled refresh.

**Weakness, stated honestly.** The full `--countries` (all) pull was not run to
completion in this session — the 5-country scoped pull is what's actually verified
end-to-end (245 real rows loaded, visible in `bronze.world_bank_lpi` right now). The
timing extrapolation above is a reasonable estimate, not a measured fact, and should
be re-verified with an actual full run before quoting a specific "full pull takes N
minutes" number anywhere more permanent than this document.

---

### 3. New silver/gold models: making "ingesting" mean something downstream

**What was built.** Two silver models (`silver_comtrade_trade_flows`,
`silver_world_bank_lpi`) and three gold models
(`gold_country_logistics_scorecard`, `gold_trade_balance`, `gold_macro_context`) — the
first time either external data source has been referenced by anything past bronze.

**Why the motCode = '0' filter in `silver_comtrade_trade_flows`, specifically.** The
raw Comtrade response returns one row per (reporter, period, flow) **per mode of
transport** — air, sea, rail, road, etc. — plus one additional row per flow where
`motCode = '0'` ("TOTAL MOT"), which Comtrade itself already computes as the sum
across all modes. Naively summing every row per (reporter, period, flow) would double
every total, since the '0' row already contains the sum of the others. This was
verified directly against the real loaded data before deciding: querying `motCode,
motDesc, fobvalue` for Brazil 2023 showed exactly this structure (see the raw output
captured during this phase), confirming '0' is the correct, sole row to keep for a
"total trade value" metric.

**Why the LPI pivot happens in gold, not silver.** `silver_world_bank_lpi` stays in
the tidy long format the source data naturally has (one row per country/indicator/
year) — consistent with this project's established layering convention (Phase 1,
decision #2): silver preserves natural grain, gold reshapes for consumption. The
pivot into one-row-per-country with all 7 sub-scores as columns
(`gold_country_logistics_scorecard`) is a presentation/consumption decision, which is
gold's job.

**Why each indicator uses its own most-recent year rather than one shared year
across all 7.** The World Bank doesn't necessarily publish every LPI sub-indicator
for every country in the same release cycle. Requiring one common year across all 7
before showing a country's row would silently drop countries (or sub-scores) that
have genuinely current data for most indicators but a one-cycle-old value for one.
Taking each indicator's own latest year maximizes real data shown, at the cost of a
scorecard row technically blending data from up to a couple of different collection
years — a tradeoff made explicit in the model's own header comment, not hidden.

**Why `gold_macro_context` is Brazil-specific rather than a generic multi-country
join.** This project's operational risk layer (Phase 1's `gold_supplier_scorecard`,
etc.) is entirely about Brazilian sellers — Olist is a Brazilian marketplace. A
generic "join LPI to trade balance for every country in the dataset" table would be
more general but less *useful*: nobody asking "how healthy is our supply chain" wants
Argentina's LPI score mixed in. `gold_macro_context` answers one specific, real
question — what's Brazil's macro logistics/trade backdrop, next to the Olist-derived
operational metrics — as a single row, which is also why it's a `FULL OUTER JOIN ...
ON true` rather than a plain join: it should still return a (partially-null) row even
if only one of the two source pulls has been run, rather than silently vanishing.

**Real numbers from these models (5-country scoped pull; see #2 for why not
all-countries):** Germany ranks #1 by LPI overall (4.1) among the 5 loaded countries,
USA #2 (3.8), China #3 (3.7), **Brazil #4 (3.2)**, Argentina #5 (2.8) —
`lpi_rank_in_dataset`, explicitly *not* claimed as a global rank (see the model's own
column-naming rationale). Brazil's 2023 trade balance: $339.7B exports − $240.8B
imports = **$98.9B surplus** (FOB, all modes of transport combined).

**Likely interview question:** *"Why build a whole macro-context gold model instead
of just joining LPI/trade data into the existing executive summary?"*
**Answer:** Grain mismatch — `gold_executive_summary` is a portfolio rollup over
Olist sellers (seller-grain aggregates), while LPI/trade data is country-grain and
conceptually a different kind of fact (macro/national context, not operational
supplier performance). Cramming them into one table would mean either duplicating the
country-level numbers onto every row of a seller-grain rollup (wasteful and
confusing) or inventing an artificial join key that doesn't exist in the data. A
separate, purpose-built table that a dashboard or analyst can pull alongside the
operational metrics is the more honest representation of what these two data sources
actually are to each other: related context, not the same fact table.

**Weakness, stated honestly.** None of `gold_macro_context`,
`gold_country_logistics_scorecard`, or `gold_trade_balance` are wired into the
dashboard UI or the decision agent's system prompt yet — they exist, are tested, and
are queryable, but a user of the dashboard wouldn't discover them without knowing to
query the gold schema directly. Surfacing them in the dashboard (a new section
alongside the existing KPI/risk/concentration ones) is a natural next increment,
explicitly not done in this phase to keep the phase scoped to "make the ingestion
claims true," not "redesign the dashboard again."

---

### 4. FastAPI on Cloud Run: built from zero, validated, not deployed

**What was found.** No GCP artifacts of any kind existed anywhere in this
repository — no Terraform, no `cloudbuild.yaml`, no service account, no mention of
Cloud Run outside the resume itself. Unlike the Comtrade/World Bank findings (broken
or dormant code that needed fixing), there was nothing here to reconcile — only to
build, exactly as Phase 3 built AWS from a comparable starting point.

**What was built.** `gcp/terraform/` (Artifact Registry + a `google_cloud_run_v2_service`
+ the explicit public-access IAM binding Cloud Run requires that App Runner doesn't),
`gcp/deploy.sh`, `gcp/README.md`, and one small but important change to the shared
image: `docker/Dockerfile.api`'s `CMD` now binds to `${PORT:-8000}` instead of a
hardcoded `8000`, because Cloud Run injects `PORT` (default 8080) and requires the
container to listen on it, while App Runner does not set `PORT` at all and needed the
8000 default preserved. One Dockerfile, one image, correct on both clouds via a shell
variable default rather than a build-time branch.

**Why this Cloud Run service reads its data from AWS S3 instead of a separate GCS
bucket.** This is the single most consequential design decision in this phase, worth
walking through carefully. The straightforward option would be: replicate
Phase 3's data-export flow, but to Google Cloud Storage, so each cloud is
self-contained. That was rejected in favor of pointing this Cloud Run service at the
*same* S3 bucket via `DUCKDB_S3_URI` + the existing `docker/fetch_db.py`, unmodified.
Reasoning: a self-contained-per-cloud design means running `scripts/export_to_s3.py`
twice (or writing a second export script for GCS), maintaining two data-freshness
stories, and — most importantly — it would prove nothing about portability, since two
independent single-cloud deployments aren't meaningfully more "multi-cloud" than one.
Deploying the *identical* container image, unmodified, to two different compute
platforms, both fetching from one shared data source, is a real demonstration that
the compute layer doesn't secretly assume anything cloud-specific. The cost, named
directly rather than glossed over: this Cloud Run service depends on AWS credentials
and AWS network reachability even though it's running on GCP infrastructure, which is
an unusual production pattern (most real deployments keep compute and its primary
data store in the same cloud for latency, egress cost, and blast-radius reasons) and
would need real justification — "we're proving cloud portability for this
project" — that wouldn't fly as-is in an actual production system without a better
reason than "the demo wanted to reuse infrastructure."

**Why validated but not applied — and what "validated" actually means here.** Exactly
the same posture as Phase 3's AWS Terraform, for the same reason: no GCP project or
credentials were available in the environment this was built in. `terraform fmt`
(clean, no diff), `terraform init` (successfully downloaded and resolved the real
`hashicorp/google ~> 5.0` provider), and `terraform validate` (passed) were all run —
which confirms the HCL is syntactically valid and every resource's arguments match
the real Google provider's schema (attribute names, required fields, types). It does
**not** confirm the resources would actually provision correctly (IAM propagation
timing, quota limits, an org policy blocking public Cloud Run access, etc.) — that
class of issue only surfaces from a real `terraform apply` against a real project,
which is an action with real cost and real infrastructure consequences that shouldn't
be taken without the project owner's direct authorization and their own GCP
credentials.

**Likely interview question:** *"You're claiming 'FastAPI on Cloud Run' but you just
told me it's not actually deployed — isn't that still not true?"*
**Answer, precisely:** The honest claim is "the Cloud Run deployment is built,
schema-validated against the real GCP provider, and ready to deploy with one command
once pointed at a real project" — which is a materially different and more defensible
claim than either "it's live in production" (false) or "I have some notes about how
I'd do this" (a much weaker claim than what was actually built here). If pressed
further on whether the resume line itself should now say "Cloud Run" unqualified, the
honest answer is: it should say what's true — either update the resume to describe
this as "designed and validated for deployment to Cloud Run" until it's actually
applied, or apply it before claiming it outright. That's a decision for the resume's
owner to make with this document as the accurate reference, not something to paper
over here.

**Weakness, stated honestly, once more directly:** this is infrastructure-as-code
that has never provisioned a single real cloud resource. Terraform's own validation
guarantees are real but bounded — they catch "this configuration is wrong" with high
confidence, not "this configuration will succeed against a live account with its own
quotas, policies, and IAM propagation delays." Anyone relying on this for an actual
deployment should expect to debug at least one small thing `terraform apply` surfaces
that `validate` couldn't have caught — that is completely normal for a first real
apply of any nontrivial Terraform config, not a sign something here is wrong.

---

### 5. Fixing the LangChain / Anthropic-SDK documentation mismatch

**What was found.** `agent/decision_agent.py` itself was already accurate (its own
module docstring correctly says "Uses the Anthropic SDK directly with a manual tool
loop") — the mismatch lived entirely in documentation *about* that file:
`CLAUDE.md`'s Architecture section described `create_openai_functions_agent` with
tools `run_sql` and `list_tables` (neither of which exists — the real tools are
`run_sql` and `get_executive_summary`), `api/routers/decisions.py`'s docstring said
"the LangChain agent," `tests/test_agent.py`'s module docstring said "LangChain/Claude
decision agent," and `.env.example` labeled the Anthropic key "for LangChain agent."
Four separate places, all describing an architecture the code never actually had.

**Why this matters enough to fix as part of "resume reconciliation" even though it
isn't literally in the resume line.** CLAUDE.md exists specifically to be read by
whoever (or whatever) works on this repo next — including future-you preparing for an
interview, cold, exactly as this document's own opening line describes its purpose. A
documentation file describing tools and a framework that don't exist would actively
mislead exactly the reading it's meant to support. This is the same category of gap
as the resume-line claims, just at a different altitude (internal docs vs. external
resume), and this phase's mandate — "ensure the repo matches every claim" — reads
naturally to cover both.

**What was built.** All four references corrected to describe the real architecture
(direct Anthropic SDK, manual tool loop, the two real tool names), with CLAUDE.md's
correction specifically noting *why* the mismatch existed (pointing at this
document) rather than silently rewriting history.

**Likely interview question:** *"Why would a project's own docs describe an
architecture that was never built — did someone plan to use LangChain and pivot?"*
**Answer, honestly:** There's no way to know for certain from the repository alone —
plausible explanations include an initial plan to use LangChain that was replaced
with a simpler direct-SDK implementation without the docs being updated to match, or
documentation written aspirationally/from a template before the actual implementation
diverged. Either is a completely ordinary way for docs to drift from code; the
important part for this phase was finding and closing the gap, not reconstructing
exactly how it happened.

---

### 6. The dashboard's AI assistant was a placeholder — wired for real, plus a discovered bug

**What was found.** `dashboard/app.py`'s "AI Decision Assistant" section took a
question as input but never sent it anywhere — regardless of what was typed, it
displayed a static "LLM integration coming soon" message. This directly contradicted
CLAUDE.md's own Architecture section, which described the dashboard as calling the
FastAPI `/decisions/ask` endpoint. Investigating the API side surfaced a second,
independent bug: `agent/decision_agent.py`'s `ask()` function already computes a full
`{"answer", "sql_used", "action_items"}` result — including running a whole separate
Claude call (`_extract_action_items`'s fallback path) specifically to extract action
items when the main answer doesn't contain `ACTION:`-prefixed lines — but
`run_decision_agent()` (the shim `api/routers/decisions.py` actually calls) discarded
everything except `answer` before it ever reached the API response. The action-item
extraction machinery was real, working code that had never once had its output seen
by anything.

**What was built.** The dashboard's text input now POSTs to
`{API_BASE_URL}/decisions/ask` with a 60-second timeout (LLM tool-calling loops are
slow; a UI-conventional few-second timeout would fail on legitimate, slower answers),
renders the answer, the extracted action items as a bulleted list, and the SQL the
agent ran in a collapsed expander (useful for exactly the kind of "how did it get
this answer" question this document keeps anticipating from interviewers).
`run_decision_agent()` now returns the full dict instead of just `answer`, and
`api/routers/decisions.py`'s `/ask` endpoint passes that dict straight through — the
`action_items`/`sql_used` fields the agent always computed are now actually visible
somewhere. A pre-existing mutable-default-argument bug on the same line
(`context: dict = {}`) was fixed to `dict | None = None` while already editing this
function's signature.

**Why a plain `requests.post` from Streamlit rather than importing and calling the
agent in-process from the dashboard.** The dashboard and API are two separately
deployable services (see `docker-compose.yml`, `aws/`, `gcp/` — the dashboard is
never part of either cloud deployment, only the API is). Importing
`agent.decision_agent` directly into `dashboard/app.py` would work today, since both
happen to run from the same repo checkout locally, but it would silently break the
moment the dashboard is deployed somewhere that doesn't have the agent's dependencies
installed (Streamlit Community Cloud, specifically — its slim `requirements.txt` has
no `anthropic` package by design, see Phase 1 decision #10) or is pointed at a
different API instance than the one running locally. An HTTP call is the only
integration that's correct in every deployment topology this project actually has.

**How this was verified without a real ANTHROPIC_API_KEY.** A real API key wasn't
available in this environment (the `.env` file's key is the literal
`.env.example` placeholder — checked directly, without ever printing the actual
value). The full round trip was still verified end-to-end using Streamlit's `AppTest`
harness driving a real, running `uvicorn` instance: the dashboard correctly
constructed and sent the HTTP request, the FastAPI endpoint received it and invoked
the real agent code, the agent correctly attempted a real call to Anthropic's API,
that call failed with an authentication error (expected, no real key), and the
dashboard caught and displayed that failure cleanly with no unhandled exception
anywhere in the chain. Every piece of this integration is proven correct except the
one step that requires a credential this environment doesn't have — which is a
meaningfully stronger verification than "the code looks right," and the honest
boundary of what could be checked here.

**Likely interview question:** *"So you haven't actually seen a real answer come back
from Claude through this whole pipeline?"*
**Answer:** Correct, and worth being direct about — what's verified is that the
request reaches the agent and the agent reaches Anthropic's API correctly (failing
only on auth, not on malformed requests, wrong URLs, or a code path that never fires).
With a real `ANTHROPIC_API_KEY` in `.env`, the remaining step is Anthropic actually
answering, which is Anthropic's API working as documented, not something this
project's code does anything unusual with.

**Weakness, stated honestly.** The 60-second timeout is a guess calibrated to "LLM
tool-calling loops are slow," not a measured value — this project's agent has never
completed a real end-to-end call in this environment to measure. If real usage shows
the tool loop (which can make multiple sequential Claude calls — one per `run_sql`
tool invocation, potentially several per question) routinely exceeds 60 seconds,
this timeout would need to be measured and adjusted against real observed latency,
the same way the World Bank ingestion timeout was (see #2) — but that measurement
requires the credential this session didn't have.

---

## Phase 4 — real numbers (for citation)

| Metric | Value |
|---|---|
| UN Comtrade rows loaded (Brazil, 2023, imports+exports) | 19 (bronze), 2 (silver, grand-totals only) |
| World Bank LPI rows loaded (5-country scoped pull: BRA, USA, CHN, DEU, ARG) | 245 (bronze and silver — 1:1, no filtering) |
| Brazil 2023 trade balance | $98.9B surplus ($339.7B exports − $240.8B imports, FOB) |
| Brazil's LPI overall score / rank-in-dataset | 3.2 / #4 of 5 loaded countries |
| New dbt models this phase | 5 (2 silver, 3 gold) — total dbt models now 13 (up from 8) |
| New dbt tests this phase | 8 — total dbt tests now 27 (up from 19) |
| New ingestion unit tests (mocked HTTP, no live network) | 9 (5 comtrade, 4 world bank) |
| Total test suite size | 20 tests (excludes the live-LLM `test_agent.py` smoke test, which requires a real `ANTHROPIC_API_KEY`) |
| Real bugs found and fixed in previously-untested code | 3 — Comtrade's 404-causing URL structure, the API response discarding `action_items`/`sql_used`, a mutable-default-argument bug |
| Documentation/code mismatches found and fixed | 5 files (`CLAUDE.md`, `api/routers/decisions.py`, `tests/test_agent.py`, `.env.example`, plus the dashboard placeholder itself) |
| GCP Terraform resources defined | 3 (Artifact Registry repo, Cloud Run v2 service, IAM public-access binding) |
| `terraform validate` (GCP) | Passing (schema-verified against google provider ~> 5.0; not applied — no GCP project/credentials in this environment) |

---

## Phase 5 — Gold Layer Validation: does risk_tier actually predict anything?

Every phase so far built and shipped metrics. None of them checked whether the
headline metric — `gold_supplier_scorecard.risk_tier` — actually predicts the thing
it claims to flag. This phase adds that check: `gold_risk_score_validation.sql`, a
proper out-of-sample backtest, and an honest accounting of what it found — including
the parts that don't flatter the model.

### 1. Why this can't just be "compare risk_tier to late_delivery_rate directly"

**The trap.** The obvious first instinct — group `gold_supplier_scorecard` by
`risk_tier` and check average `late_delivery_rate` per group — will "work" no matter
what, and prove nothing. `late_delivery_rate` is 50% of the formula that produces
`reliability_score`, which is exactly what `risk_tier` thresholds. Asking "do
HIGH-risk sellers have a higher late rate" this way is asking "does a number
correlate with a rounded version of itself" — the answer is trivially yes, by
construction, regardless of whether the underlying scoring logic captures anything
real about supplier behavior.

**The fix.** A risk score is only meaningfully validated against outcomes it did NOT
see when it was computed. `gold_risk_score_validation.sql` recomputes
`gold_supplier_scorecard`'s exact formula (same weights, same 20-day consistency cap,
same 85/70 tier cutoffs — deliberately copy-pasted, not shared via a macro, so the
backtest is provably scoring with the actual production formula) using only orders
placed before 2018-01-01, then checks that training-derived tier against each
seller's actual late-delivery outcomes in orders placed on or after that date. This
is a standard train/holdout split, applied to a risk score instead of an ML model,
for exactly the same reason it's standard practice there: it's the only construction
where a good result is actually evidence of something.

**Why 2018-01-01, and why >=3 training orders / >=1 holdout order.** The split was
chosen for balance (45,430 training orders vs. 54,011 holdout orders, roughly 46/54)
using the full available date range (Sep 2016 - Aug 2018; Sep-Oct 2018's 20 orders
were excluded as too sparse to be a meaningful tail either way) — picked before
looking at any backtest result, not tuned afterward to produce a nicer number. The
order-count minimums (>=3 training, >=1 holdout) exist because a "late rate" computed
from one or two orders is a coin flip, not a rate; 896 of 3,095 sellers (29%) clear
both bars. See #4 below for exactly what that excludes.

**Likely interview question:** *"Why not just hold out a random sample of orders
instead of splitting by time?"*
**Answer:** A random split would leak information: if a seller's Order #47 (randomly
placed in the "holdout" set) is used to help score that same seller's overall risk
tier from their other orders, and the seller's underlying reliability is stable over
time, the holdout order's outcome is correlated with the training orders' outcomes
through the seller's persistent quality — a much weaker test than a genuine time
split. A time-based split asks the harder, more honest question a real deployment
actually faces: does a score built from what's already happened predict what
happens *next* — which is the only way this score would ever actually get used.

---

### 2. The headline result: real, ordinally correct, but weaker than the raw numbers suggest — and one number that looks bad at first glance and isn't wrong, just differently framed

**The result, in full:**

| Training-period risk tier | Sellers | Holdout late rate | Baseline late rate | Rate lift vs. baseline | Seller-selection lift |
|---|---|---|---|---|---|
| HIGH | 76 | 12.67% | 9.37% | **1.35x** | 0.68x |
| MEDIUM | 330 | 9.86% | 9.37% | 1.05x | 1.58x |
| LOW | 490 | 8.36% | 9.37% | 0.89x | 0.66x |

**The core finding: yes, directionally — HIGH-flagged sellers really do fail more
often out of sample.** Holdout late rate is correctly ordered HIGH (12.67%) >
MEDIUM (9.86%) > LOW (8.36%), exactly the ordering the score is supposed to produce,
computed entirely from orders the training-period score never saw. That ordering
matches the training-period ordering too (avg late rate 29.8% / 9.9% / 1.5% for
HIGH/MEDIUM/LOW respectively) — compressed a lot in the holdout period (expected:
regression to the mean is normal for any score validated out of sample, and doubly
expected here given HIGH is only 76 sellers). This is genuine, non-circular evidence
that `reliability_score` captures something real about a seller's future behavior,
not just a restatement of its own inputs.

**The two "lift" numbers diverge, and that divergence is itself the more interesting
finding.** `rate_lift_vs_baseline` (a HIGH-flagged seller's *per-order* risk of a late
delivery, relative to a random order) is 1.35x — a real, if modest, effect.
`seller_selection_lift` (if you could only investigate N sellers, does flagging
HIGH surface more raw late-order *count* than randomly picking N sellers) is 0.68x —
*worse* than random. These look contradictory. They aren't: MEDIUM-tier sellers turn
out to have roughly 3x the average future order volume of HIGH-tier sellers
(65.1 vs. 21.9 orders/seller in the holdout period — see the model's own inline
diagnostic query in this phase's development, reproduced in the real-numbers table
below). A tier full of lower-volume sellers can have a substantially worse per-order
failure rate while still contributing a smaller share of the company's total
late-order count than its seller-count share alone would suggest, purely because it
ships fewer orders overall. Reporting only `seller_selection_lift` (0.68x for HIGH)
would make the score look like it doesn't work; reporting only `rate_lift_vs_baseline`
(1.35x) would hide a real operational fact — that HIGH-flagged sellers are
disproportionately low-volume, so "high risk" and "high failure-count contributor"
are not the same population here. Both numbers are correct; they answer different
questions, and a report that picked only the flattering one would be exactly the kind
of dishonesty this whole validation exercise exists to prevent.

**What % of actual delivery failures came from HIGH-flagged suppliers, answered
directly:** 5.79% of holdout late orders (`pct_of_holdout_late_orders` = 0.0579) came
from sellers flagged HIGH-risk in training, while HIGH-risk sellers were 8.48% of the
validated seller population (`pct_of_sellers` = 0.0848) — i.e., HIGH-risk sellers
under-represent the *raw count* of failures relative to their share of the seller
base (the `seller_selection_lift` = 0.68x restates this same fact as a ratio), even
though each of their orders is individually 35% more likely to be late than a random
order (`rate_lift_vs_baseline` = 1.35x). Both of those sentences are true
simultaneously, for the volume reason above.

**Likely interview question:** *"So does the risk score work or not?"*
**Answer:** It works as a per-order risk signal (1.35x lift on the metric the score
is actually supposed to predict, correctly ordered across all three tiers, out of
sample) and it does **not** work as a "which sellers should I prioritize
investigating to reduce total late-order count" tool, because tier assignment and
future order volume aren't independent in this dataset. Those are two different
claims, and conflating them is exactly the kind of overclaim this backtest was built
to catch. If asked to pick one sentence: *the score has real, modest, out-of-sample
predictive signal on a per-order basis; it is not strong enough, nor is "risk tier"
correlated enough with future order volume, to double as a prioritization tool for
where the most total failures will come from.*

---

### 3. Why the effect is real but modest, stated plainly

**A 1.35x lift is a genuine signal, not a strong one.** For comparison, a
well-tuned fraud or churn model in production commonly reports lift in the 3-10x
range at the top decile. 1.35x means the score meaningfully beats random but leaves
most of the variance in future late-delivery risk unexplained. Three concrete,
named reasons, not a vague "models are imperfect" shrug:

1. **The formula was built to explain the training period's own late rate (50% of
   its weight is literally that number), not to forecast a different period's late
   rate** — a seller's late rate can genuinely shift between periods for reasons the
   score has no way to see (a new warehouse, a bad quarter, a logistics partner
   change), and a formula optimized for retrospective description will always
   underperform out of sample relative to its in-sample fit. This is not a bug in
   the formula; it is the ordinary, expected gap between describing the past and
   predicting the future.
2. **Small HIGH-tier sample (76 sellers, 1,665 holdout orders).** A tier this size
   is more exposed to noise than MEDIUM (330 sellers, 21,495 orders) — the 12.67%
   holdout late rate for HIGH has a wider real confidence interval than the point
   estimate alone suggests, though a formal interval wasn't computed here (see
   weakness below).
3. **The underlying baseline late rate is already low (9.37%)** — Olist's delivered
   orders are late less than 1 time in 10 overall (matching Phase 1's finding of
   7.84% company-wide), which caps how much absolute separation any score can create
   between tiers; a domain with a 40% baseline failure rate has far more room for a
   score to show dramatic lift.

**Likely interview question:** *"If you found the model was this weak, why ship
`risk_tier` at all instead of pulling it?"*
**Answer:** Because 1.35x is a real, positive, out-of-sample, non-circular result —
"weaker than I'd want" is a different finding from "doesn't work." A 35% relative
increase in late-delivery risk for flagged suppliers is operationally actionable (it's
a legitimate input to a sourcing decision, just not a sole determinant of one), and
shipping it *with this validation attached* — rather than either hiding the weak
result or not measuring it at all — is the actually defensible position. The
alternative, quietly shipping a risk score with an implied but unverified predictive
claim, is worse regardless of which way the number would have come out.

---

### 4. What this backtest excludes, and why that's an honest limitation

**71% of sellers (2,199 of 3,095) are excluded from this validation entirely** —
either they don't clear the >=3 training-orders / >=1 holdout-order bar, or they have
no orders at all in one of the two periods. The single largest excluded group is
single-order sellers (571 of them, per Phase 1's own finding) — sellers with no real
track record to score in the first place, not sellers hidden because their inclusion
would look bad. This is worth stating explicitly because it's the kind of exclusion
that *could* be used to cherry-pick a flattering sample, and wasn't: the thresholds
were the same "does this rate mean anything" bar used throughout this project (Phase
1 applied comparable reasoning to the stddev-based consistency component), applied
before looking at what the backtest would say, not after.

**What this means for the 71% not covered:** this backtest says nothing about
whether `risk_tier` predicts anything for low-volume or one-off sellers — which,
per Phase 1's own numbers, describes a large share of the actual seller base. The
validated result applies to the ~900 sellers with enough order history for "risk
tier" to be a meaningful statement about them in the first place; extending any claim
of predictive validity to the long tail of one-and-done sellers would be
unsupported by this analysis.

**No formal confidence intervals or significance testing.** The lift numbers above
are point estimates from a single train/holdout split — no bootstrap, no p-value, no
check of whether 1.35x is statistically distinguishable from 1.0x given the sample
sizes involved. For a score this consequential in a real production sourcing
decision, that would be the natural next increment; it wasn't done here because a
single honestly-reported point estimate with its confounds explained was judged more
valuable, given the time available, than a more statistically rigorous treatment of
a formula that (per #3) is already known to be a hand-weighted heuristic rather than
a fitted model where significance testing carries its usual meaning.

**Likely interview question:** *"With only one train/holdout split, how do you know
this result isn't just luck?"*
**Answer, honestly:** I don't know that with statistical certainty — a single split
is a real limitation, named here rather than glossed over. What raises confidence
short of formal significance testing: the ordering is consistent across all three
tiers (not just HIGH vs. the rest), it matches the training-period ordering
directionally, and the finding survived being computed two different ways (rate-based
and selection-based) that could easily have disagreed in a way that only makes sense
as noise — instead they disagreed in a way that has a clear, verifiable, non-noise
explanation (the volume confound in #2). That's suggestive, not conclusive. A more
rigorous version would run this backtest across multiple split dates (e.g., rolling
3-month holdouts) and check whether 1.35x-ish lift holds up consistently — a natural
next step, not done here.

---

## Phase 5 — real numbers (for citation)

Produced by `dbt build --select gold_risk_score_validation` against the full Olist
dataset. Re-run before citing if the underlying data or the `gold_supplier_scorecard`
formula changes — these numbers are specific to the 2018-01-01 split and the formula
as it exists today.

| Metric | Value |
|---|---|
| Train/holdout split date | 2018-01-01 (45,430 training orders / 54,011 holdout orders) |
| Sellers validated | 896 of 3,095 (29%) — >=3 training orders and >=1 holdout order |
| HIGH-tier holdout late rate | 12.67% (76 sellers, 1,665 holdout orders, 211 late) |
| MEDIUM-tier holdout late rate | 9.86% (330 sellers, 21,495 holdout orders, 2,120 late) |
| LOW-tier holdout late rate | 8.36% (490 sellers, 15,724 holdout orders, 1,314 late) |
| Baseline (all validated sellers) holdout late rate | 9.37% |
| HIGH-tier rate lift vs. baseline | **1.35x** (the headline, per-order signal) |
| HIGH-tier seller-selection lift | 0.68x (volume-confounded — see decision #2) |
| Avg. holdout orders/seller — HIGH / MEDIUM / LOW | 21.9 / 65.1 / 32.1 |
| Training-period avg late rate — HIGH / MEDIUM / LOW | 29.8% / 9.9% / 1.5% |
| Ordinal ranking preserved out of sample? | Yes — HIGH > MEDIUM > LOW in both training and holdout periods |

---

## Phase 6 — Making It Live: a Scheduled Comtrade Pipeline + Trade Concentration Over Time

### 0. A correction, made before anything else in this phase

The task that kicked off this phase said to reuse "the Aptean-style guard" for
dedup, described as an existing pattern in this codebase. It doesn't exist —
checked directly: `grep -rni "aptean"` across every file and the full git history
of this repository returns nothing, and no dedup/upsert/merge pattern of any kind
existed anywhere before this phase (bronze ingestion, until now, was exclusively
full-replace: `DROP TABLE` + `CREATE TABLE AS SELECT`, appropriate for a one-shot
pull, not for a recurring one). Rather than quietly build something and let an
implied "as you already had" stand uncorrected, this is named directly: the dedup
mechanism in this phase (`upsert_bronze()`, decision #3 below) is new work, designed
from scratch for this task, not a reuse of anything pre-existing. This is the same
"don't let an unverified claim about the repo stand" discipline this project has
applied to itself since Phase 4 — it applies equally to claims arriving from a task
description, not just ones already sitting in the code or docs.

### 1. Why UN Comtrade for "making it live" (not World Bank, not a new source)

**What was built.** The "live" pipeline in this phase extends `ingestion/comtrade.py`
(Phase 4) rather than `ingestion/world_bank_lpi.py` or a new data source.

**Why.** Three concrete reasons. First, Comtrade genuinely has fresh, frequently-
revised data — customs statistics get updated as countries submit or correct
figures, which is a real reason to poll on a schedule; World Bank LPI is a biennial
survey (new values appear roughly every two years, not weekly), which would make
"build a weekly pipeline for it" a much harder claim to justify honestly. Second,
Comtrade's free tier needs no signup or key to produce real, live data (verified
directly in Phase 4 and again here) — a scheduled pipeline that requires a credential
nobody reading this repo can obtain would undercut its own "genuinely live" claim.
Third, and specific to what this phase adds: Comtrade's data is naturally
partner-country-disaggregated (which country sold what to whom), which is exactly
the shape a *concentration* analysis needs; World Bank LPI is a per-country score
with no natural "concentration of what" axis to compute at all.

**Likely interview question:** *"Trade statistics are annual — what does 'live'
even mean here?"*
**Answer:** "Live" means the pipeline polls a real external API on a schedule and
the gold layer reflects whatever it finds, not that the underlying phenomenon
changes weekly — see decision #2 for the honest version of this distinction, which
matters enough to deserve its own section.

---

### 2. Why weekly, for data that updates roughly annually

**The honest tension, stated directly.** A weekly Airflow schedule polling a data
source that substantively changes about once a year looks, at first glance, like a
mismatched cadence — and mostly, on any given week, it is: the pipeline will fetch
the same period Comtrade already had last week and upsert it right back, changing
nothing. That is not a wasted run; it's the correct behavior of a well-designed
poller, for three reasons that together justify weekly specifically (not daily, not
monthly):

1. **Comtrade doesn't announce when it revises a period's data.** A country can
   resubmit corrected customs figures for an already-published period at any time,
   with no public calendar. The only way to catch a revision promptly without a
   human remembering to check is to poll — and the polling interval is a tradeoff
   between staleness (too infrequent) and load on a free, rate-limited public API
   (too frequent). Weekly is a deliberately conservative middle point: daily would
   be 7x the API load for a source that realistically doesn't revise that often;
   monthly risks sitting on stale data for weeks after a real revision lands.
2. **The dedup mechanism (#3) makes "poll and find nothing new" a genuinely free
   operation, correctness-wise** — a no-op re-pull doesn't corrupt anything or
   create duplicates, so there's no downside to polling "too often" relative to how
   fast the source actually changes, only the API-load cost already addressed above.
3. **Operationally, this is what a real production data-freshness SLA looks like**
   for a slowly-changing external source: you don't try to detect the exact moment
   new data appears, you poll on a cadence cheap enough to run indefinitely and
   accept a bounded staleness window (here, up to a week) as the cost of not needing
   a push notification from a source that doesn't offer one.

**Likely interview question:** *"So most weeks this DAG does nothing useful — isn't
that a sign it's scheduled wrong?"*
**Answer, precisely:** Most weeks it does nothing *new*, which is different from
nothing *useful* — confirming the current period's figures haven't silently changed
is the useful work, even when the answer is "no change." The alternative — not
polling at all, and hoping someone notices a Comtrade revision manually — is strictly
worse, not more efficient. If pushed further: yes, for this specific source, monthly
would probably be operationally sufficient too, and weekly is a deliberately
conservative choice favoring freshness over minimizing API calls, made explicit here
rather than asserted without a reason.

---

### 3. Dedup design: delete-then-insert on a natural key, not a native UPSERT

**What was built.** `ingestion/comtrade.upsert_bronze()` deletes any existing rows
matching the incoming batch's natural key
(`typeCode, freqCode, period, reporterCode, flowCode, partnerCode, partner2Code,
cmdCode, customsCode, motCode` — every dimension/grouping field Comtrade returns,
deliberately excluding every measure column so a revised *value* for the same
underlying fact is recognized as "the same row, updated," not "a new row"), then
inserts the new batch. Verified directly (not just reasoned about) with three
scenarios in `tests/test_comtrade.py`: a genuinely new partner is added without
disturbing existing rows, a revised value for an existing key replaces the old value
without duplicating the row, and re-running the identical pull twice (simulating an
Airflow task retry) leaves the row count unchanged.

**Why delete-then-insert instead of DuckDB's native `INSERT ... ON CONFLICT`.**
`ON CONFLICT` needs a declared unique constraint or primary key to detect
conflicts against, and this project's bronze tables have never had one — bronze is
built throughout this project (Phases 1-5) to mirror source data as directly as
possible, without imposing constraints the source itself doesn't guarantee (Comtrade
doesn't publish a formal uniqueness contract on this key; it's this project's own,
reasonable inference from inspecting real responses, not a documented guarantee).
Adding a constraint purely to unlock `ON CONFLICT` syntax would mean asserting a
stronger guarantee than actually verified, for a syntactic convenience. Delete-then-
insert needs no schema change, is straightforward to read as "remove what's about to
be replaced, then add the replacement," and — this is the property that actually
matters for the task's own framing — is naturally idempotent under retry: it doesn't
need special-case retry logic on top of it to be safe.

**A real bug this surfaced during development, not hypothetically.** The very first
live run of `upsert_bronze()` against this project's actual working database failed
with `table comtrade_trade_flows has 48 columns but 49 values were supplied`. The
table already existed — populated by Phase 4's `load_bronze()` full-replace CLI,
which never added a `_run_id` column, only `_loaded_at`. The fix
(`_table_columns()` comparing the existing table's schema against the incoming
DataFrame's columns, rebuilding fresh on any mismatch rather than assuming
compatibility from row count alone) is now load-bearing production logic, not a
hypothetical edge case — it's exactly what happened the first time this code ran for
real. This is worth knowing cold: an interviewer asking "what broke when you first
ran this" has a concrete, true answer, not a hedge.

**Likely interview question:** *"What happens if two overlapping runs try to write
at the same time — does the dedup logic handle concurrency?"*
**Answer, honestly:** No, and it isn't meant to. Delete-then-insert makes repeated
*sequential* writes (a retry, an overlapping manual + scheduled run one after
another) safe — it does not make *concurrent* writes to the same DuckDB file safe,
because DuckDB is a single embedded file, not a client-server database with its own
transaction isolation across processes. That's exactly why the DAG sets
`max_active_runs=1` (decision #7) — concurrency is prevented at the orchestration
layer, not solved at the storage layer, because the storage layer (an embedded
file) isn't the right place to solve it.

---

### 4. Rate limits: characterized empirically, not assumed from documentation

**What was found.** Comtrade's free preview API has no published rate-limit
number. Direct testing in this phase found genuinely inconsistent behavior: six
rapid sequential calls with identical parameters all succeeded cleanly in one test
run; in Phase 4's earlier testing, the *second* call in quick succession had
returned a 429. There is no discoverable pattern (burst limit, per-minute quota,
per-parameter-combination throttling) that explains both observations — the honest
conclusion is that the limit is real but its exact shape is unknown and possibly
not even deterministic (shared infrastructure, load-dependent throttling, etc.).

**What was built in response.** Two independent layers, because they mitigate
different things: (1) a proactive 1.5-second pacing delay between every API call in
`comtrade_pipeline.py`, regardless of whether a prior call was throttled — cheap
insurance against a limit that might be time-window-based; (2) the existing
reactive retry-with-backoff in `fetch_trade_flows()` (Phase 4, extended in #6 below)
for when pacing alone isn't enough. Neither is presented as a guarantee, because
none is possible against an undocumented limit — both are documented explicitly as
pragmatic mitigations against an empirically-observed-but-unspecified constraint.

**Likely interview question:** *"How do you know your rate-limit handling is
actually sufficient?"*
**Answer, honestly:** I don't know that with certainty, and said so directly rather
than asserting confidence the testing doesn't support. What's actually verified: a
real run of the full weekly pull (2 reporters x 3 commodities = 6 partner-breakdown
calls plus 1 period-detection probe) completed successfully end to end, including
correctly retrying through at least one real 429 encountered during this phase's own
testing. That's evidence the current pacing+retry combination works for this
pipeline's actual call volume — it is not proof it would hold at 10x the volume
against a limit whose exact shape remains unknown.

---

### 5. Pagination: the free tier doesn't have any — detection instead of a page loop

**What was found, and why "handle pagination" doesn't mean what it might sound
like here.** Comtrade's free preview API caps results at exactly 500 records per
call, confirmed directly: a narrow query (one HS code, one partner) returned 244
rows with `count` matching `len(data)` exactly (nothing withheld); a broad query
(HS chapter 85, all partners) returned exactly 500 for `count` and `len(data)` both
— the textbook signature of a hard cap, not a coincidence. Critically, there is no
`page`, `offset`, or continuation token anywhere in the response — nothing to page
*with*. "Handle pagination" for this specific API, honestly, means "detect and
report truncation," not "loop through pages," because the free tier offers no
mechanism to retrieve rows beyond the cap at all.

**What was built.** `comtrade_pipeline.run_weekly_pull()` checks
`len(records) >= PREVIEW_ROW_CAP` (500) after every fetch and logs a structured
warning identifying exactly which (reporter, commodity, period) hit the cap,
surfaced in the run's summary dict (and therefore in Airflow's XCom / task logs).
This actually fired on real data in this phase: Brazil's HS-85 (electronics)
2024 and 2025 pulls both hit the 500-row cap and were flagged. That specific
combination was deliberately kept in the default config (rather than narrowed to a
sub-code that would dodge the cap) specifically so this detection path gets
exercised on real data, not just in a unit test.

**What "truncated" means for the resulting concentration numbers, honestly.** A
truncated pull is missing an unknown number of the *smallest* partner-country
rows (Comtrade doesn't document a truncation ordering, but the observed data is
consistent with largest-value-first). For a concentration metric, this biases the
computed HHI **downward** in the direction of undercounting the group total (missing
partners' small values aren't in the denominator either), but the effect on the
*top-partner* rankings and *dominant-partner* share — the numbers this project's
gold layer and dashboard actually foreground — is small, since by construction the
missing rows are the small ones. Still named as a real, quantifiable limitation, not
waved away: Brazil's HS-85 HHI (~2,949-3,331 across the two periods pulled) should be
read as "at least this concentrated," not as an exact figure.

**Likely interview question:** *"Why not just narrow every commodity code to avoid
ever hitting the cap?"*
**Answer:** Because that would optimize for a clean-looking demo over an honest one.
HS chapter 85 (electronics) is a real, broad, economically meaningful Olist-aligned
category — narrowing it to a sub-heading specifically to dodge a real API limit
would hide exactly the kind of constraint a production version of this pipeline
would have to handle regardless. Demonstrating the detection working on genuine
truncated data is more defensible than a demo that never has to.

---

### 6. Hardening `fetch_trade_flows()`: retrying network errors, not just 429s

**What was found.** Phase 4's retry loop only handled HTTP 429 responses; a
connection timeout or reset (a different, also-observed failure mode for this API —
see Phase 4's own DECISIONS entry on the World Bank API's similar flakiness from
this environment's network) would have propagated as an unhandled exception,
killing the whole weekly pull over one transient network blip.

**What was built.** `fetch_trade_flows()`'s retry loop now wraps the request itself
in a `try/except requests.exceptions.RequestException`, retrying transient network
failures with the same exponential backoff already used for 429s, re-raising only
after the final attempt. This mirrors the pattern already established for the World
Bank ingestion script (Phase 4's `_get_with_retry`) — the same class of problem
(an external API that's sometimes slow or briefly unreachable from this specific
environment) getting the same kind of fix, applied consistently across both
ingestion modules rather than solved once and left unfixed in the other.

---

### 7. The partner-country reference data: verified against Comtrade's own source, not memory — and one wrong assumption caught before shipping

**What was built.** `dbt/seeds/comtrade_partner_areas.csv` — all 310 rows of
Comtrade's own partner-country reference data, fetched directly from
`https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json` and loaded as
a dbt seed, joined in `silver_comtrade_partner_flows.sql` to attach real country
names to partner codes.

**Why fetched, not hand-typed.** Country-code-to-name mappings are exactly the kind
of fact that's easy to get almost-entirely-right from memory and subtly wrong in a
few spots (a transposed digit, a superseded code) — which is a much worse failure
mode than "just re-derive it from data" for a project whose whole DECISIONS.md
practice is "verify before citing." Comtrade publishes this mapping at a stable,
documented URL specifically so consumers don't have to guess it, and using it is
strictly more defensible than a hand-typed list, however careful.

**A wrong assumption, caught before it shipped.** The seed's own `isGroup` field
looked, from its name, like exactly what was needed to exclude non-country
pseudo-partners (regional aggregates like "Other Asia, nes") from a per-country
concentration calculation — counting a regional bucket alongside its own real member
countries would double-count value that's already attributed to at least one real
country elsewhere in the same result set. Filtering on `isGroup = false` was the
first implementation. It produced visibly wrong output: "Other Asia, nes" appeared
as the **5th-largest source country** for Brazil's electronics imports in the first
real run of `gold_trade_concentration`. Checking the reference data directly showed
`isGroup = False` for every one of the ~20 actual aggregate/residual entries
("Caribbean, nes," "Bunkers," "Free Zones," etc.) — the field does not mean what its
name suggests, at least not for this purpose. The eventual fix — matching the
literal `", nes"` suffix Comtrade itself uses for "not elsewhere specified" residual
codes, plus three explicitly-named non-country codes (`Bunkers`, `Free Zones`,
`Special Categories`) that don't share that suffix — was itself verified against
all 310 reference rows before shipping: a naive `LIKE '%nes%'` substring match was
tried first and rejected because it incorrectly caught five real countries whose
names happen to contain "nes" as a substring (Indo**nes**ia, Philippi**nes**,
Micro**nes**ia, French Poly**nes**ia, and Saint Vincent and the Gre**nes**... — actually
Grenadines — every one confirmed by direct inspection of the query result, not
assumed from the pattern alone).

**Likely interview question:** *"How did you catch this — did you write a test for
it first?"*
**Answer, honestly:** No — it was caught by looking at real output and noticing
something implausible ("Other Asia, nes" ranked ahead of Germany and Japan as an
electronics source for Brazil doesn't pass a sanity check), not by a test written in
advance that anticipated this specific failure mode. The test suite does now lock in
the *correct* behavior (there's no automated regression test proving `isGroup` is
misleading, only tests proving the final `', nes'`-suffix filter produces the right
partner list on real data) — a fair thing to name if asked "so this is only caught
by manual inspection": yes, for this specific class of reference-data-semantics bug,
that's what caught it.

---

### 8. Concentration and shift metric definitions

**HHI methodology: reused, not reinvented.** `gold_trade_concentration`'s HHI
(sum of squared partner-share percentages, 0-10,000 scale, DOJ/FTC merger-guideline
interpretation bands) is the identical formula and identical interpretation
thresholds as Phase 1's `gold_concentration_risk` (supplier revenue concentration).
This is a deliberate, load-bearing consistency choice: this project uses one
concentration index throughout, applied to three different populations (suppliers,
geography, now trade partners) — not three different ad-hoc formulas that happen to
share a name. An interviewer asking "why HHI again here" gets the same answer as
Phase 1's: it's a named, standard index, not a bespoke score whose meaning has to be
re-explained from scratch every time it's used.

**"Prior period" is defined per-partner via `LAG()`, not as a fixed N-1-years
lookback — and why that distinction matters.** `gold_trade_concentration_shift`
compares each partner's current share to *that partner's own* most recent prior
appearance in the data (`LAG(...) OVER (PARTITION BY ... partner_code ORDER BY
period)`), not to "the same metric exactly one calendar period earlier." With
exactly two dense periods pulled so far (2024, 2025, every partner present in both),
these two definitions happen to coincide — but they would diverge the moment
Comtrade has a reporting gap for some partner in some year, which does happen in
real customs data. The `LAG()`-based definition is the version that stays correct
once that happens; a fixed-offset definition would silently compare a partner's 2025
share to a NULL 2024 value instead of correctly falling back to, say, their 2022
figure. Documented explicitly in the model's own header comment so this doesn't
read as an oversight if a future reporting gap changes the observed behavior.

**Why HS chapters 85/33/94, specifically.** Chosen to line up with product
categories Phase 1's `gold_sourcing_cost_drivers` already analyzes at the
transaction level (electronics, health_beauty/cosmetics, furniture) — not because
these are Brazil's largest trade categories in absolute terms, but so the same
product families are visible through both lenses this project now has: micro
(individual Olist transactions and their freight cost) and macro (national import
concentration by source country). HS 85 was deliberately kept as a broad 2-digit
chapter rather than narrowed to dodge the preview API's row cap — see decision #5.

**Real finding, stated plainly (this is a genuinely interesting result, not a
placeholder metric):** Brazil's electronics imports (HS 85) are **highly
concentrated** — HHI 2,949 (2025) and 3,331 (2024), both above the 2,500 "highly
concentrated" DOJ/FTC threshold, with a single partner (China) alone accounting for
~53% of import value in 2025. Brazil's cosmetics imports (HS 33) are
**unconcentrated** — HHI ~1,005-1,019, no single dominant source. Furniture (HS 94)
is highly concentrated (HHI ~3,400-3,600). This is exactly the kind of product-by-
product variation a concentration metric should reveal — a single company-wide
"our sourcing is/isn't concentrated" number would have hidden this entirely.

---

### 9. The Airflow DAG: thin orchestration, and how it was actually validated

**Design: business logic in plain functions, the DAG file is glue.** All pull,
pagination, rate-limit, and dedup logic lives in `ingestion/comtrade.py` and
`ingestion/comtrade_pipeline.py` as ordinary Python, independently tested with
`pytest` and mocked HTTP (`tests/test_comtrade_pipeline.py`) with zero dependency on
Airflow being installed. `dags/comtrade_weekly_dag.py` imports and calls that code
from two `@task`-decorated functions. This is the same separation this project
established for its ingestion CLIs back in Phase 4 (a plain, testable `main()`
function, not logic embedded in argument parsing) — applied here to a different
orchestrator (Airflow instead of a CLI entrypoint) for the same reason: the actual
logic shouldn't need the orchestration layer running to be tested.

**`catchup=False` and `max_active_runs=1`, explained rather than left as defaults.**
`catchup=False` because backfilling every missed weekly run since a fixed start
date against a source that updates roughly annually would mean dozens of redundant,
rate-limit-risking calls fetching identical data — the opposite of what catchup
exists to help with (catching up on genuinely missed *distinct* work).
`max_active_runs=1` because DuckDB is a single embedded file with no
concurrent-writer support of its own (see decision #3's answer on concurrency) —
this is where that constraint is actually enforced, at the orchestration layer, not
assumed away.

**Two retry layers, not one, and why both are needed.** Airflow's own task-level
`retries=2` (in `default_args`) handles a task failing catastrophically — an
unhandled exception, the worker process dying, a container restart mid-run. The
per-HTTP-call retry-with-backoff inside `fetch_trade_flows()` (decision #6) handles
a single request failing transiently. Neither substitutes for the other: an
Airflow-level retry re-runs the *entire* task from scratch (re-fetching everything,
which the dedup logic makes safe but which is more redone work than necessary for a
single flaky call); a request-level retry can't recover from the task process itself
crashing.

**How this DAG was actually validated — this is the strongest infrastructure
validation done anywhere in this project.** Every prior phase's cloud/orchestration
artifact (AWS and GCP Terraform in Phases 3-4) was validated with `terraform
validate` — confirming syntax and schema correctness against the real provider, but
never actually executed. This phase went further: Apache Airflow 3.3.0 was
installed directly (in an isolated venv, not the project's main one) — the first
Airflow release with Python 3.13 wheels, matching this project's actual Python
version, confirmed by checking Airflow's published per-version constraints files
before choosing a version, not by trial and error. The DAG was then not just parsed
(`DagBag`, zero import errors, both tasks correctly registered, correct
`pull_comtrade_data -> refresh_dbt_models` dependency, `catchup`/`max_active_runs`
both confirmed set correctly) but **actually executed**, task by task, via
`airflow tasks test`: `pull_comtrade_data` ran for real against the live Comtrade
API, correctly re-detected 2025 as the latest period, correctly hit and logged the
HS-85 truncation warning, and correctly upserted with zero net row change (2,762
rows before and after — proving the dedup guarantee holds through Airflow's actual
task-execution path, not just in isolated unit tests) and pushed its summary to
XCom; `refresh_dbt_models` then ran a real `dbt build` subprocess, all 57
model/test checks passing. Both tasks completed with `state=success`.

**Likely interview question:** *"You went to the trouble of actually running this
in Airflow — why didn't earlier phases do the same for the AWS/GCP Terraform?"*
**Answer:** Different constraints, not different diligence. Terraform's `validate`
genuinely cannot run further without real cloud credentials and a real account —
`plan`/`apply` create actual billable resources, which this project has consistently
declined to do without the account owner's explicit authorization (stated plainly in
Phases 3 and 4). Airflow has no equivalent barrier: it's software that installs and
runs entirely locally, with no cloud account, no billing, and no resource-creation
side effects — there was no reason *not* to actually run it once it was clear
Python 3.13 support existed. The gap between "validated" and "actually executed" in
this project has always tracked exactly this distinction: whether running it further
required an external, consequential, billable action, or just installing a package.

**One compatibility choice worth naming: `airflow.decorators`, not `airflow.sdk`.**
Airflow 3.x deprecates `airflow.decorators.dag`/`.task` in favor of `airflow.sdk`,
and importing from `airflow.decorators` prints a deprecation warning under 3.3.0
(observed directly during the validation above). `airflow.sdk` doesn't exist at all
on Airflow 2.x, which is still the more commonly deployed major version in the wild.
Since this project's DAG was written to be usable regardless of which major version
a reader/deployer actually has, `airflow.decorators` was kept deliberately — a
cosmetic warning on 3.x is a strictly better tradeoff than an outright `ImportError`
on 2.x.

---

### 10. Deployment: self-hosted Airflow via docker-compose, not a managed-service Terraform buildout

**What was built.** `docker/Dockerfile.airflow` (extends the official
`apache/airflow:3.3.0-python3.13` image with this project's own ingestion/dbt code
and dependencies) and `docker/docker-compose.airflow.yml` (Postgres metadata DB +
Airflow scheduler + API server, LocalExecutor, `dags/` and `data/` bind-mounted).

**Why docker-compose instead of a full Terraform buildout for a managed Airflow
service (AWS MWAA or GCP Cloud Composer), matching the pattern of Phases 3-4's AWS
App Runner / GCP Cloud Run Terraform.** A deliberate scope decision, not an
oversight. MWAA specifically is one of the most infrastructure-heavy AWS services to
provision correctly via Terraform — it requires a VPC with at least two private
subnets across different availability zones, a NAT gateway, a specifically-
structured S3 bucket for DAG storage, and a dedicated execution role, none of which
this project's existing AWS footprint (Phase 3: an S3 bucket and an App Runner
service, no VPC at all) currently has any of. Building that out would have been a
substantially larger addition than the AWS/GCP compute Terraform in Phases 3-4
combined, for a task that asked to "keep it deployable" — which a genuinely
runnable, standard `docker compose up` satisfies honestly, without requiring a new
VPC topology to be designed and validated in the same phase as the pipeline itself.
Cloud Composer (GCP's managed option) is comparably heavy. The honest next step,
named rather than silently deferred: a managed Airflow service is the natural
production upgrade path from this docker-compose setup, and would warrant its own
phase with the same Terraform-plus-validation treatment Phases 3-4 gave AWS/GCP
compute.

**What's validated about the deployment packaging, and what isn't.** The DAG's
actual task logic was executed for real (decision #9). The docker-compose file
itself — the Postgres wiring, the Airflow image build, the volume mounts — was
validated only as far as YAML syntax correctness (parsed cleanly with `PyYAML`,
merge-key anchors resolve correctly, all four services and both named volumes
present). `docker compose up` was not run against it, because Docker was not
available in the environment this was built in (the same disclosed limitation as
Phase 3's `docker-compose.yml`). This is a meaningfully smaller unverified surface
than earlier phases' Docker work, precisely because the thing that actually matters
— does the DAG's code work — was proven independently of the container packaging
around it.

---

### 11. What actually refreshes when the pipeline runs, and — critically — what "hosted dashboard" does and doesn't mean here

This is the section to read before claiming this project has "a scheduled pipeline
feeding a live hosted dashboard," because that sentence is true for one deployment
topology and false for the one that's actually publicly live today. Both are stated
plainly below, not left to be assumed.

**If the Airflow stack (`docker-compose.airflow.yml`) and the API/dashboard stack
(`docker-compose.yml`) are run together, on the same host, sharing the same `data/`
directory** — both compose files bind-mount `../data` to their respective
containers — then yes, this is a genuinely coherent, wired-together system: the
weekly DAG's pull lands in the shared DuckDB file, `dbt build` (run by the DAG's
second task) refreshes `gold_trade_concentration`/`gold_trade_concentration_shift`
in that same file, and the dashboard container (reading the same bind-mounted file)
reflects it on next page load — no restart, no redeploy, no code change. This was
demonstrated concretely in this phase: two real pulls (2024, then 2025) landed in
the actual project database, `dbt build` picked up both, and the dashboard's
selectbox-driven `AppTest` runs (decision below) showed different, period-correct
HHI values and interpretations for each product queried, live from that same file.

**The Streamlit Community Cloud deployment — the one with an actual public
URL — does NOT and structurally CANNOT receive this pipeline's output, as
currently architected.** Per Phase 1's own decision record, `streamlit_app.py`
bootstraps a synthetic, self-contained database via `data/sample_data.py` on Cloud,
because Cloud has no access to this project's local filesystem, the Airflow
container's volume, or any of this project's other infrastructure. This phase's
`sample_data.py` update (a small, hand-picked two-commodity synthetic dataset
modeled on this phase's real findings — one HIGHLY_CONCENTRATED product, one
UNCONCENTRATED one) exists specifically so the Cloud dashboard's new
"Trade-Partner Concentration" section has something real-shaped to render instead of
an empty state — but it is a static snapshot generated once, at Cloud cold-start,
never updated by this phase's weekly DAG, ever, under the current architecture. This
is the same category of limitation Phase 1 already disclosed for the rest of the
Cloud dashboard (the Olist-derived sections are equally static there) — this phase
doesn't introduce a new kind of gap, it extends an existing, already-disclosed one
to a new section.

**What it would actually take to make the public Cloud dashboard live too — named,
not built.** The DAG (or a scheduled export step after it) would need to push its
DuckDB snapshot or Parquet exports to a location Streamlit Cloud can reach over the
network — which is exactly what `scripts/export_to_s3.py` (Phase 3) already does,
and exactly the mechanism `docker/fetch_db.py` (Phase 3) already uses for the AWS/GCP
API deployments. Wiring the *dashboard* (not just the API) to fetch from S3 on
startup, instead of always bootstrapping synthetic data, is the concrete next step —
not done in this phase, because it changes Phase 1's `streamlit_app.py` bootstrap
logic and deserves its own explicit decision (and a real redeploy to verify) rather
than a drive-by change bundled into a data-pipeline phase.

**One-sentence version, for a fast answer under interview pressure:** *the scheduled
pipeline and the dashboard genuinely talk to each other when run together locally or
on one self-hosted server sharing storage; the separate, already-public Streamlit
Cloud dashboard is intentionally isolated (per Phase 1) and shows a static synthetic
example of this phase's metrics, not this pipeline's live output, until the export-
to-S3 mechanism already built in Phase 3 is wired into the dashboard's own
bootstrap — which it currently is not.*

---

## Phase 6 — real numbers (for citation)

Produced by real pulls against the live Comtrade API in this phase (2 reporters —
Brazil 76, Argentina 32 — x 3 commodities x 2 periods), plus `dbt build`.

| Metric | Value |
|---|---|
| Bronze rows landed (`comtrade_trade_flows`) | 2,762 (across 2024 + 2025, 2 reporters, 3 commodities) |
| Silver partner-level rows (`silver_comtrade_partner_flows`) | 1,287 (aggregate/residual pseudo-partners excluded) |
| Gold concentration rows (`gold_trade_concentration`) | 1,287 |
| Gold shift rows (`gold_trade_concentration_shift`) | 573 (only pairs with a genuine prior period) |
| Brazil electronics (HS 85) HHI, 2025 | 2,948.7 — HIGHLY_CONCENTRATED (China ~52.8% of import value) |
| Brazil electronics (HS 85) HHI, 2024 | 3,331.1 — HIGHLY_CONCENTRATED |
| Brazil cosmetics (HS 33) HHI, 2025 | 1,009.1 — UNCONCENTRATED |
| Brazil furniture (HS 94) HHI, 2025 | 3,412.0 — HIGHLY_CONCENTRATED |
| Truncated pulls detected (real, not simulated) | 1 (Brazil, HS 85, both 2024 and 2025 hit the 500-row preview cap) |
| Comtrade partner reference seed | 310 rows, fetched from Comtrade's own reference endpoint |
| New dbt models this phase | 3 (1 silver, 2 gold) + 1 seed — total dbt models now 20 (17 models + 1 seed + 2 shift/concentration), 57 total model+test checks |
| New ingestion/pipeline tests | 14 (9 comtrade.py incl. upsert dedup, 5 comtrade_pipeline.py incl. live-pipeline idempotency) |
| Total test suite size | 29 (excludes the live-LLM `test_agent.py` smoke test) |
| Airflow version validated against | 3.3.0 (first release with Python 3.13 wheels) |
| Airflow validation method | Real execution via `airflow tasks test` for both tasks — not just `DagBag` parsing |
| Real bugs found and fixed in this phase | 3 — the bronze schema-compatibility crash on first live upsert run, the wrong `isGroup` assumption (verified against real reference data, corrected to a `', nes'`-suffix filter), a false "Aptean-style guard" premise corrected before building anything on top of it |

---

## Phase 7 — CI/CD: GitHub Actions

Every prior phase's tests, dbt checks, and Docker builds were run by hand, by
whoever happened to be sitting at the keyboard, whenever they remembered to. This
phase closes that gap: a GitHub Actions workflow now runs the real test suite,
the real dbt build, and a real Docker image build automatically on every push and
every PR to `main` — and, unlike every cloud deployment artifact in Phases 3, 4,
and 6, this one was not just written and validated locally. It was triggered for
real, on GitHub's own infrastructure, and the actual run output is quoted below,
not summarized from memory.

### 1. Why GitHub Actions, and why this needed almost no new infrastructure decision

**What was built.** `.github/workflows/ci.yml` — two jobs, triggered on
`push`/`pull_request` to `main`, no path filters.

**Why GitHub Actions specifically, briefly.** This wasn't a real build-vs-buy
decision the way App Runner-vs-Fargate (Phase 3) or self-hosted-vs-managed Airflow
(Phase 6) were — the repo is already hosted on GitHub, GitHub Actions is free for
public repositories, and it requires zero new accounts, credentials, or
infrastructure beyond a YAML file in the repo itself. Every other CI option
(CircleCI, Jenkins, a self-hosted runner) would add an external account and a
second place for configuration to live, for a project that has no other reason to
leave GitHub. The interesting decisions in this phase are all about what runs
inside the workflow, not about which CI product hosts it.

**Why no path filters (`paths:` / `paths-ignore:`).** A change to almost any file
in this repo can affect the checks that matter: an ingestion script change affects
what bronze looks like, a dbt model change affects the gold layer, a
`requirements-api.txt` change affects whether the Docker image builds. Scoping the
trigger to "only run when `dbt/**` changes" would require this project to
correctly declare its own internal dependency graph in the workflow file — a
second, easier-to-forget place for that graph to live, separate from dbt's own
`ref()`/`source()` graph, which is the one that's actually authoritative. Running
the full gate on every push is slightly wasteful on GitHub's free compute; it is
never wrong.

---

### 2. The real gap this phase found: dbt tests have nothing to run against in a fresh CI checkout

**What was found.** `dbt build` (which runs models, then tests) needs a populated
`bronze` schema to do anything — every silver model selects from
`{{ source('bronze', '...') }}`. A fresh `git clone` in a CI runner has an empty
DuckDB file. Populating bronze the way local development does requires either the
real Olist CSVs (gitignored, ~120MB, Kaggle-licensed — not something this repo can
or should commit) or live calls to the UN Comtrade and World Bank APIs (both
already documented in Phases 4 and 6 as rate-limited and, in this project's own
testing, occasionally flaky from this environment's network). Neither is
acceptable for a CI gate whose entire purpose is "tell me with certainty whether
my code change broke something" — a gate that can fail because Kaggle wasn't
reachable, or because Comtrade rate-limited a request, is a gate nobody will trust
after the second false alarm.

**What was built instead.** `scripts/build_ci_fixture_db.py` — a small, fully
synthetic bronze layer, covering exactly the 8 bronze tables any dbt model
actually references (checked directly with
`grep -rho "source('bronze', '...')" dbt/models`, not assumed from the 11 tables
`sources.yml` declares — 3 of those 11, `customers`, `geolocation`, and
`order_payments`, are declared but never selected by any current model and
correctly don't need a fixture row).

**Why a hand-built fixture instead of, say, checking a small real Olist sample
into the repo.** A hand-built fixture can be deliberately shaped to exercise
specific branches in the SQL — a category that clears
`gold_sourcing_cost_drivers`' `>= 30`-item threshold and one that deliberately
doesn't, two Comtrade periods so `gold_trade_concentration_shift`'s `LAG()` logic
has something to compare against, a seller cohort that straddles the
`gold_risk_score_validation` backtest's 2018-01-01 split — in a way that a random
100-row slice of real Olist data would not reliably do (real data has no
obligation to happen to contain a seller with exactly the right order-count
distribution on both sides of an arbitrary date). It is also unambiguous about its
own nature: nobody reading `build_ci_fixture_db.py` could mistake its output for
real customer data, whereas a committed "small real sample" invites exactly that
confusion later.

**Likely interview question:** *"Doesn't a synthetic fixture mean CI never
actually validates against real data quality issues — a genuine null spike in a
new Olist export, a Comtrade schema change?"*
**Answer, honestly:** Correct, and that's a real, named boundary, not an
oversight. This CI gate answers "is the SQL logic correct" — do the joins,
aggregations, and thresholds behave as intended given data shaped like the real
thing. It does not and cannot answer "did today's real data quietly change shape
in a way the models don't handle" — that's a data-observability problem (schema
drift detection, volume anomaly alerts on the real pipeline), a different and
complementary concern from what a code-correctness CI gate is built to catch. A
production version of this project would want both: this CI gate for every code
change, plus monitoring on the real, scheduled pipeline runs (Phase 6's Airflow
DAG) for data-shape drift that no fixture could ever anticipate.

---

### 3. Validating the fixture locally, first — and the real bug it caught before CI ever saw it

**What was done.** Before writing a single line of the GitHub Actions workflow,
the fixture generator was run locally and its output fed through a real
`dbt build`. The first attempt failed:

```
Runtime Error in model silver_world_bank_lpi (models/silver/silver_world_bank_lpi.sql)
Binder Error: Referenced column "_loaded_at" not found in FROM clause!
```

The fixture's `world_bank_lpi` table was missing a `_loaded_at` column that
`silver_world_bank_lpi.sql` selects — a real gap in the fixture script, caught by
actually running it, not by re-reading the SQL more carefully. Fixed, re-run:
all 57 checks (17 models, 39 tests, 1 seed) passed clean, with zero models
skipped (the first failed run had silently skipped 7 downstream checks —
`gold_country_logistics_scorecard` and `gold_macro_context` among them — because
dbt skips anything depending on a model that errored; a failure log that only
shows one red error line while several other checks quietly never ran is easy to
misread as "one small thing broke" when the real blast radius is larger).

**Why this matters enough to write down.** This is the same discipline this
project applied to the Comtrade partner-code filter in Phase 6 (verify against
real output, not just against the code reading correctly) and to every cloud
Terraform config in Phases 3, 4, and 6 (`validate` first, and be explicit about
what `validate` does and doesn't prove) — applied here to the CI fixture itself.
A CI workflow that had been pushed without this local check first would have
produced this exact same failure, just on GitHub's infrastructure instead of a
laptop, several minutes slower to discover and with a live push already
consuming one of the "show me it really works" attempts this task explicitly
asked for.

---

### 4. Job structure: two parallel jobs, no suppressed exit codes, no `continue-on-error`

**What was built.** Two independent jobs — `dbt + pytest` (dependency install →
fixture build → `dbt build` → `pytest`) and `Docker build (API image)` (checkout →
`docker build -f docker/Dockerfile.api`). Neither job sets `continue-on-error`
anywhere, and no step pipes its exit code through anything that would swallow a
failure (no `|| true`, no `; exit 0`).

**Why two jobs instead of one linear job.** They test genuinely independent
things — dbt/pytest correctness has nothing to do with whether the Docker image
builds, and a broken `requirements-api.txt` shouldn't block seeing whether the
data-layer tests passed, or vice versa. Splitting them also means GitHub Actions
runs them in parallel by default, and a failure in one names itself clearly in
the UI without being buried in a single long combined log.

**Why `requirements-dev.txt` and not the root `requirements.txt`.** This project
has four requirements files for four different deploy targets (`requirements.txt`
root — Streamlit Community Cloud, deliberately slim, Phase 1 decision #10;
`requirements-api.txt` — the AWS/GCP container; `requirements-airflow.txt` — the
self-hosted Airflow image; `requirements-dev.txt` — full local development). Only
`requirements-dev.txt` contains `dbt-core`, `dbt-duckdb`, and `pytest` at all —
using the root file here wouldn't produce a slower CI run, it would produce a CI
run where steps 2 and 3 of the task's own requirements (dbt tests, pytest) are
literally impossible to execute, failing with `command not found` rather than a
real test result. Picking the right one of four files each with a genuinely
different purpose is exactly the kind of thing worth being deliberate about
rather than defaulting to the first `requirements*.txt` a directory listing
shows.

**Why the Docker job builds `Dockerfile.api` only, not `Dockerfile.dashboard` or
`Dockerfile.airflow` too.** `Dockerfile.api` is the one image actually deployed
anywhere real (AWS App Runner, GCP Cloud Run — Phases 3 and 4) — it's the image
whose breakage has an actual production consequence. This is a scope decision
worth stating honestly as a real, current gap rather than silently covering only
part of the ask and calling it done: `Dockerfile.dashboard` and
`Dockerfile.airflow` could silently stop building and this CI workflow would not
notice. Extending the same job to build all three is a small, mechanical addition
(one more `docker build -f ... .` line per image) and is the natural next
increment, not done here to keep this phase's Docker check matched to what's
actually deployed today.

---

### 5. Verifying the Docker build step actually works, before trusting GitHub's runner to prove it

**What was found.** No prior phase in this project ever had a working Docker
daemon available — Phase 3's `Dockerfile.api` correctness was verified by copying
its exact file set into an isolated directory and running the resulting Python
directly, an honest but real proxy for "the container would work," not the
container itself.

**What changed this phase.** Docker was installed for real — Colima (a
lightweight, CLI-only Docker daemon backed by a small Lima VM) plus the Docker
CLI, both via Homebrew, since no Docker Desktop was present and a VM-based daemon
was the option that didn't require GUI interaction to set up. `docker build -f
docker/Dockerfile.api -t ci-test-api:local .` was run for the first time ever
against this project's real Dockerfile, and it succeeded. The resulting image was
then actually run (`docker run`, bind-mounting the real project `data/` directory)
and hit with real HTTP requests: `/health` returned `{"status":"ok","db":"ok"}`
and `/suppliers/?limit=1` returned real supplier-scorecard JSON. This is the first
point in this project's entire history that `docker/Dockerfile.api` has been
proven to actually build and run a working container, rather than proven correct
by simulation.

**Likely interview question:** *"If this had never been tested with a real Docker
daemon before, how confident were you that Phase 3's simulation-based validation
was actually equivalent?"*
**Answer, honestly:** Confident but not certain, and this phase is the resolution
of that uncertainty, not a restatement of it. The simulation (copying the exact
file set, installing only `requirements-api.txt` into a clean venv, running
`uvicorn` directly) was a genuinely strong proxy — it would have caught a missing
dependency or a wrong import path — but it could not have caught anything
specific to the container runtime itself (a base-image quirk, a `COPY` path that
resolves differently under Docker's build context than under a plain file copy).
Nothing in that category *did* turn up when the real build finally ran, which is
a good outcome, but the honest framing is "the simulation turned out to be
accurate," not "the simulation made real verification unnecessary."

**The workflow YAML itself was also checked with a purpose-built tool, not just
read carefully:** `actionlint` (installed via Homebrew, the standard GitHub
Actions workflow linter — it understands the `on:`/`jobs:`/`steps:` schema and
common mistakes specific to Actions syntax, which a generic YAML parser doesn't)
ran against `.github/workflows/ci.yml` with zero findings before this workflow
was ever committed.

---

### 6. The push itself was blocked twice, by two different, real GitHub permission gates — worked through, not routed around

**What happened, in order.** The first `git push` of the new workflow file was
rejected outright:

```
! [remote rejected] main -> main (refusing to allow a Personal Access Token
to create or update workflow `.github/workflows/ci.yml` without `workflow` scope)
```

This is a GitHub server-side rule, specific to paths under `.github/workflows/`:
a token needs the `workflow` OAuth scope explicitly, regardless of the
repository's own permission settings — a `repo`-scoped token that can push
anywhere else in the same repository is still rejected for this one path. The
cached credential (a plain PAT, from earlier phases' pushes) had never needed
this scope before, because no earlier phase had touched `.github/`.

The fix was not immediate, either: authenticating fresh via `gh auth login`'s
device-code flow (browser approval, no password/token typed anywhere) produced a
token scoped to `gist`, `read:org`, `repo` — **`gh`'s own default login scopes do
not include `workflow`** unless requested explicitly. A second device-code
approval, this time via `gh auth refresh --scopes workflow`, was required before
`gh auth status` showed the `workflow` scope actually present. Only then did
`gh auth setup-git` (wiring the now-correctly-scoped credential into git's own
credential helper) let the push through.

**Why this is worth documenting instead of treating as a boring auth hiccup.**
Two real, independent permission gates blocked this task, back to back, each with
a different specific fix, and both were surfaced to the project owner directly
rather than worked around — no attempt was made to, say, strip the workflow file
down to something that wouldn't trigger the path-based restriction, or to encode
the file differently to dodge the check. That restraint is the same standard this
project has held cloud deployment to since Phase 3 (stop and report a blocker
rather than silently substitute a different architecture to route around it) —
applied here to a GitHub permissions quirk instead of an AWS account
subscription gate, but the same principle.

**Likely interview question:** *"Why didn't the first `gh auth login` just work —
isn't requesting broad scopes the default for a CLI tool like this?"*
**Answer:** No, and that's a deliberate, documented design choice by the `gh` CLI
itself, not a bug — it requests a conservative default scope set and expects
callers who need something more specific (like `workflow`, which grants the
ability to modify CI/CD definitions — a meaningfully more sensitive permission
than reading issues or pushing to a branch) to ask for it explicitly via
`gh auth refresh --scopes ...` or `gh auth login --scopes ...`. Least-privilege
by default, with an explicit escalation step, is the correct instinct for a tool
that manages credentials — it just meant this task needed two rounds of user
approval instead of one, which is worth naming plainly rather than glossing over
as "logged in, moved on."

---

### 7. The real, triggered run — quoted, not summarized

**What was verified.** After the push succeeded (commit `53da4b6`, on top of
`0146cf1`), the GitHub Actions REST API — polled unauthenticated at first, since
this repository is public, and confirmed reachable that way before any `gh` auth
existed this session — showed a run start within seconds of the push landing.
Polled to completion:

```
Run 33985360545 — trigger: push, commit 53da4b6 — status: completed, conclusion: success
```

Job and step-level results, pulled from the GitHub API directly (not retyped from
watching a browser):

| Job | Step | Result | Duration |
|---|---|---|---|
| dbt + pytest | Install dependencies | success | 38s |
| dbt + pytest | Build CI fixture database | success | 2s |
| dbt + pytest | dbt build (models + data-quality tests) | success | 7s |
| dbt + pytest | pytest | success | 6s |
| Docker build (API image) | Build API image | success | 20s |

And the actual log text, pulled via `gh run view --log` (not paraphrased):

```
Done. PASS=57 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=57
============================== 29 passed in 5.10s ==============================
```

**Why quoting the raw log line matters here specifically.** This project's whole
documentation practice has been "show the real number, not a description of the
real number" — the same standard applied to Phase 6's actual Comtrade API
responses and Phase 5's actual backtest output applies here: a CI run that's
merely described as "passing" is a claim; a CI run whose actual `PASS=57 ERROR=0`
log line is quoted, from a run whose ID and commit SHA are both stated, is
verifiable by anyone who opens the same URL.

---

### 8. What this CI gate does not cover — stated plainly, matching this project's own standard for itself

**`test_agent.py` has zero CI coverage — the LLM decision agent
(`agent/decision_agent.py`) is real code with no automated regression protection
at all.** This is exactly why the README and this document's summary sections were
updated (post-Phase-7) to stop presenting the agent as a proven, working part of
the pipeline: unverified code with zero CI coverage doesn't belong in a "what this
system does" headline, regardless of how central it was to the original framing.
It's excluded from CI deliberately (a live smoke test against the real Anthropic API,
requiring `ANTHROPIC_API_KEY` — not configured as a repository secret, and
intentionally not added as one in this phase, since wiring a real LLM credential
into a CI system that runs on every PR — including PRs from forks, which get a
read-only token by default but this repo has no branch-protection rules
configured yet to enforce that distinction — is a decision that deserves its own
explicit review, not a drive-by addition alongside a CI/CD phase). The honest
next step, named rather than done here: a version of `agent/decision_agent.py`'s
tool-calling loop tested against a *mocked* Anthropic client (the same `responses`
library already used to mock HTTP calls to Comtrade and World Bank in Phases 4
and 6) would give real coverage of the tool-dispatch logic without ever touching
a live API key — that's buildable without secrets and isn't in this workflow yet.

**This is CI, not CD — the workflow builds the Docker image, it never pushes it
anywhere or triggers a deploy.** That precisely matches what this phase was asked
to build (install deps, dbt test, pytest, "attempt to build the Docker image to
confirm it doesn't break") — but it's worth being explicit that "build" and
"deploy" are different verbs, and nothing in this workflow updates the AWS ECR
image, App Runner service, or GCP Artifact Registry from Phases 3/4. Extending
this workflow to push to ECR/Artifact Registry on a merge to `main` (using
OIDC-federated cloud credentials rather than long-lived keys, ideally) is the
natural next phase, in the same spirit as Phase 6's own "no CI/CD pipeline yet"
admission about the Airflow deploy story.

**The fixture and the workflow's Python version were validated in two different
Python environments, not one.** Local validation of the fixture script and the
resulting `dbt build`/`pytest` output ran on Python 3.13.5 (this project's local
`.venv`, matched throughout Phases 1-6). The workflow itself pins Python 3.12 —
a deliberately more conservative, broadly-compatible choice for a CI runner than
matching the exact local version — and the *first* time this project's dbt/pytest
suite ever ran on 3.12 specifically was the real GitHub Actions run itself, not a
local dry run. It passed. That's a genuinely good outcome, but the honest framing
again is "it happened to pass," not "it was pre-validated on that exact version" —
a subtle distinction worth being able to name if asked directly whether the local
validation and the CI environment were identical (they were not, on this one
axis).

**The fixture needs manual maintenance as the dbt project grows.** If a future
model adds a new bronze column reference, `dbt build` will fail loudly in CI with
a clear "column not found" compile error (a feature — see #3 above, this is
exactly the failure mode the local validation step caught) — but someone has to
notice that failure and update `build_ci_fixture_db.py` to match, by hand. There
is no automated check that the fixture stays in sync with the dbt project's
actual source references; it's re-derived by inspection each time, the same way
it was built in this phase.

---

## Phase 7 — real numbers (for citation)

All numbers below are quoted directly from a real, completed GitHub Actions run —
re-open `https://github.com/akhil-partheeban/supply-chain-decision-engine/actions/runs/33985360545`
to see the same output live, or trigger a fresh run for current numbers.

| Metric | Value |
|---|---|
| Workflow trigger verified | Real `git push` to `main`, commit `53da4b6` |
| Run ID | 33985360545 |
| Run conclusion | success |
| dbt checks (models + tests + seed) | PASS=57, WARN=0, ERROR=0, SKIP=0, NO-OP=0, TOTAL=57 |
| pytest results | 29 passed, 0 failed, in 5.10s |
| Docker image build | success, 20s (`docker/Dockerfile.api`, the image actually deployed to AWS/GCP) |
| Dependency install time | 38s (`pip install -r requirements-dev.txt`) |
| Bronze tables in the CI fixture | 8 (every source any current dbt model references — 3 of 11 declared sources are unused and correctly excluded) |
| Real bug the fixture caught locally, pre-push | 1 (missing `_loaded_at` column on the synthetic `world_bank_lpi` table — a `dbt build` Binder Error, caught before ever reaching CI) |
| Real permission gates encountered and resolved | 2 (missing `workflow` OAuth scope on the cached PAT; `gh`'s own default login scopes also excluding `workflow`, requiring a second explicit `gh auth refresh --scopes workflow`) |
| Tools installed this phase to make verification real (not simulated) | Docker + Colima (real container builds/runs, first time in this project's history), GitHub CLI (`gh`), `actionlint` |
| CI coverage gaps stated honestly | `test_agent.py` (live-LLM smoke test) has zero CI coverage; the workflow builds `Dockerfile.api` only, not `Dockerfile.dashboard`/`Dockerfile.airflow`; this is CI (build/test) with no CD (deploy) step; local fixture validation ran on Python 3.13, CI pins 3.12 |

---

*This document now covers all seven phases. Any further work on this project should
add a new dated section here rather than editing the phase sections above — those are
a historical record of what was decided and why, not a living spec.*
