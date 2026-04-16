# CamOnAirFlow

> **"CamOnAirFlow"** — a name that implies a slick, production-grade Apache Airflow orchestration platform.
> In reality, Airflow got quietly dropped somewhere along the way and replaced with **dlt** pipelines run via github schedules.
> The name stays. The Airflow dropped. I move on.

---

A personal sandpit / dev playground for ingesting, transforming, and analysing public open-source data across weather, snow, skiing, satellite imagery, and air quality. No business objective. No stakeholders. Just questions worth answering and tools worth learning.

---

## What is this?

This project is a self-contained analytics engineering playground. It uses real public data to explore things like:

- How much snow fell at a ski field this season vs last?
- Which NZ ski runs are steepest, most technical, or take longest to ride?
- Are ice climbing conditions forming at Wye Creek right now?
- What does the satellite spectral signature of an ice formation look like over time?
- How does air quality in European cities vary seasonally?

It is deliberately over-engineered for a hobby project, because the point is to get reps with the tooling.

---

## Stack

| Layer | Tool |
|---|---|
| Ingestion | [dlt](https://dlthub.com/) (Data Load Tool) |
| Transformation | [dbt](https://www.getdbt.com/) (DuckDB adapter) |
| Warehouse | [MotherDuck](https://motherduck.com/) (cloud DuckDB) |
| Satellite data | [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/) via `stackstac` / `xarray` |
| Geo data | [GeoNames API](https://www.geonames.org/), [Google Elevation API](https://developers.google.com/maps/documentation/elevation) |
| Visualisation | `plotnine`, `matplotlib`, `scipy` |
| CI / Scheduling | ~~Apache Airflow~~ GitHub Actions |

---

## Data Sources

| Source | Description | Pipeline |
|---|---|---|
| [Open-Meteo](https://open-meteo.com/) | Free weather API — hourly temperature, wind, and humidity for mountain weather analysis | `ice_climbing_hourly.py` |
| [OpenAQ](https://openaq.org/) | Open air quality sensor data across European cities | `openaq_dlt.py` |
| [GeoNames](https://www.geonames.org/) | Geographic metadata — cities, elevations, regions | `GeoAPI.py` |
| [OpenStreetMap](https://www.openstreetmap.org/) (via Overpass) | Ski runs, lifts, pistes — geometry, tags, gradients | `Ski_runs.py` |
| [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/) | Sentinel-2 multispectral satellite imagery | `Spectral_Analysis.py` |

---

## Project Structure

```
CamOnAirFlow/
├── pipelines/          # dlt ingestion pipelines (raw → MotherDuck)
│   ├── ice_climbing_hourly.py  # Hourly weather at NZ ice climbing spots
│   ├── openaq_dlt.py           # Air quality sensors across European cities
│   ├── GeoAPI.py               # GeoNames geographic data
│   ├── Ski_runs.py             # OSM ski runs, lifts, piste geometry
│   └── Spectral_Analysis.py    # Sentinel-2 satellite spectral features
│
├── dbt/                # dbt project (MotherDuck via DuckDB adapter)
│   ├── models/
│   │   ├── base/       # Cleaned, typed source models
│   │   ├── staging/    # Business logic & feature engineering
│   │   ├── common/     # Shared dimensions (date, city, country)
│   │   └── analysis/   # Views for exploration & visualisation
│   └── macros/         # Custom dbt macros (audit log, graph weights, etc.)
│
├── scripts/            # Standalone analysis & visualisation scripts
│   ├── ski_paths.py                    # Shortest/optimal ski run paths
│   ├── ski_paths_matrix.py             # Route matrix between ski runs
│   ├── steepest_ski_paths_viz.py       # Gradient visualisation
│   ├── ski_path_elevation_viz.py       # Elevation profiles
│   ├── Ice_Climbing_Visual.py          # Ice climbing condition charts
│   ├── Cumulative_snow.py              # Cumulative snowfall over season
│   ├── seasonal_var_daily.py           # Seasonal variance analysis
│   ├── staging_nz_ice_quality_visual.py # NZ ice quality estimates
│   ├── sentinel_visual.py              # Satellite band visualisation
│   └── plot_gradient_distribution.py   # Run gradient distributions
│
├── charts/             # Output chart artefacts
├── post_run.py         # Post-pipeline dbt run hook
├── project_path.py     # Path resolver & env bootstrapping
├── requirements.txt    # Python dependencies
└── Dockerfile          # Dev container definition
```

---

## Getting Started

### Prerequisites

- Docker / VS Code Dev Container (recommended)
- Python 3.11+
- A `.env` file at the project root (see below)

### Environment Variables

Create a `.env` file in the project root:

```env
GEONAMES_USERNAME=your_geonames_username
GOOGLE_API_KEY=your_google_api_key
```

Some pipelines (OpenAQ, Open-Meteo, Planetary Computer) use free/open APIs with no key required.

### Install dependencies

```bash
pip install -r requirements.txt
cd dbt && dbt deps
```

### Run a pipeline

```bash
# Ingest ice climbing weather
python pipelines/ice_climbing_hourly.py

# Ingest air quality
python pipelines/openaq_dlt.py

# Ingest satellite spectral data
python pipelines/Spectral_Analysis.py

# Ingest ski run geometry from OSM
python pipelines/Ski_runs.py
```

### Run dbt transforms

```bash
cd dbt
dbt run
dbt test
```

### Run analysis scripts

```bash
python scripts/ski_paths.py
python scripts/Ice_Climbing_Visual.py
python scripts/Cumulative_snow.py
```

---

## dbt Model Layers

| Layer | Schema | Purpose |
|---|---|---|
| `base` | `base` | Filtered, typed raw data — one row per source record |
| `staging` | `staging` | Feature engineering, business logic, derived metrics |
| `common` | `common` | Shared dimensions: `dim_date`, `dim_city`, `dim_country` |
| `analysis` | `analysis` | Lightweight views for exploration and ad-hoc queries |

An audit log is written on every model run via a pre/post hook defined in `dbt_project.yml`.

---

## CI / Automation

All automation is handled via GitHub Actions — no Airflow, no orchestrator, no regrets.

### dbt Slim CI (`.github/workflows/ci.yml`)

Triggered on push or PR to `main` when any file under `dbt/models/` changes.

- Connects to **MotherDuck** using a repository secret (`MOTHERDUCK`) as the DuckDB path
- Uses **dbt slim CI** (`state:modified+`) — only builds models that changed and their downstream dependents, not the full project
- The previous `manifest.json` is stored on a dedicated `dbt-manifest-state` branch and restored at the start of each run
- Falls back to a full `dbt build` if no previous manifest exists (e.g. first run)
- After a successful build, compiles a fresh manifest and pushes it back to `dbt-manifest-state` for the next run

### Python Linting (`.github/workflows/linting.yml`)

Triggered on push or PR when any `.py` file changes. Runs `flake8` across the whole repo.

### Dependency Audit (`.github/workflows/deps.yml`)

Scheduled weekly — **Thursdays at 9:00 pm Melbourne time** (AEST, `0 11 * * 4` UTC). Installs `requirements.txt` and reports any outdated packages. Also runnable manually via `workflow_dispatch`.

---

## Example Questions This Explores

- **"What's the steepest legal ski run in the South Island?"** — `Ski_runs.py` → `base_ski_run_gradients` → `staging_ski_gradient_distribution`
- **"Are ice climbing routes at Wye Creek likely to be in condition?"** — `ice_climbing_hourly.py` → `base_weather_daily_ice_features` → `staging_nz_ice_quality_estimate`
- **"How do Sentinel-2 spectral bands change at ice climbing locations over winter?"** — `Spectral_Analysis.py` → `base_spectral_daily_features`
- **"How long does it take to ride every lift at Coronet Peak?"** — `Ski_runs.py` → `base_ski_lift_times` → `staging_ski_time_estimate`

---

## Notes

- Data is stored in **MotherDuck** (cloud-hosted DuckDB). The connection string is kept in a `.env` file locally and as a GitHub Actions secret for CI.
- Pipeline state is managed by dlt (stored in `pipelines/.dlt/`).
- Some pipelines support incremental loading and will resume from the last known state.
- The `request_cache/` directory caches expensive API responses between runs.
- This is a personal learning project. Production-readiness is not a goal. Breaking changes are frequent and guilt-free.
