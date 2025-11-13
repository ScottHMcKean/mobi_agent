# Mobi Vancouver Bike Share Hackathon

This repository gives hackathon teams a practical toolkit for exploring Vancouver's Mobi by Rogers bike share system. It bundles data acquisition, clean-room processing, feature engineering, and starter machine learning workflows designed to run quickly on Databricks serverless clusters.

Your mission: build an agentic system on Databricks that can reason about rider behaviors, station operations, or supporting content using the datasets assembled here. The playbooks in this repo give you the raw ingredients so you can focus on end-to-end automation, orchestration, and decision making.

## Highlights

- Historical trip pipeline: scrape and download the official monthly CSVs, keep a raw archive, and merge them into a bronze Parquet table ready for Unity Catalog.
- Real time station coverage: pull station metadata and availability with a lightweight GBFS client, then persist snapshots as Parquet or Delta.
- Exploration notebooks: run `01_data.ipynb` to build bronze and silver trip tables, `02_scrape_mobi.ipynb` to capture marketing copy, and `03_vector_search.ipynb` or `02_tools.ipynb` for additional feature tooling.
- Forecasting starter: `05_ml.py` walks through next-day station demand forecasting with scikit-learn and Spark interoperability.
- Extensible scraper: reuse `src/mobi/basic_site_scraper.py` to capture first party content that can enrich features or power LLM prompts.

## What Will You Do With This Data

- Build an autonomous planner that ingests trip history and proposes dispatch actions for high demand stations.
- Surface ride patterns for specific neighborhoods or events, then let an LLM powered assistant explain the insights to non technical partners.
- Prototype a rider concierge that predicts availability, recommends nearby stations, and answers FAQs using scraped site content.
- Generate story driven dashboards that guide city stakeholders while an agent keeps the metrics refreshed and annotated.
- Combine weather, holidays, and marketing copy to let an orchestrated workflow test hypotheses and report outcomes automatically.

## Repository Guide

- `src/mobi`: production ready Python helpers for downloading trip CSVs, processing them into Parquet, calling the GBFS API, and working with station level datasets.
- `01_data.ipynb`: Databricks notebook that automates the trip data bronze ingestion and silver standardization workflow.
- `02_scrape_mobi.ipynb` and `02_tools.ipynb`: utilities for web scraping, prompt generation, and feature engineering support.
- `03_vector_search.ipynb`: example retrieval workflow that can power question answering over scraped content.
- `05_ml.py`: Databricks notebook script for training, evaluating, and saving baseline demand forecasts.
- `config.yaml`: central configuration for Unity Catalog catalog.schema settings and scraper options.
- `data/`: optional local cache of raw trip CSVs and processed Parquet files.
- `tests/`: pytest suite with integration coverage for the GBFS client against the live API.

## Data Sources

- Trip history: CSV files published at https://www.mobibikes.ca/en/system-data dating back to January 2018.
- Station metadata and status: GBFS feeds exposed by Fifteen's Mobi endpoints at https://gbfs.kappa.fifteen.eu/gbfs/2.2/mobi/en/.
- Optional site content: HTML scraped from https://www.mobibikes.ca/ for marketing copy or FAQ context.

## Getting Started Locally

1. Install Python 3.9 or later and create a virtual environment.
2. Run `uv sync` or `pip install -e ".[dev]"` from the project root to install dependencies.
3. Copy `config.yaml` if you need different Unity Catalog targets or scraper settings.
4. Use the `mobi.data_downloader` and `mobi.data_processor` modules inside Python or notebooks to pull and combine data. Each function has docstrings with parameter defaults.

## Running in Databricks

- Import the notebooks into your Databricks workspace or attach the repo as a project.
- Create or select a Unity Catalog catalog and schema, then update `config.yaml` or the Databricks widgets in each notebook.
- Execute `01_data.ipynb` end to end to populate bronze and silver tables such as `silver_trips` and `bronze_stations`.
- Optionally run `05_ml.py` to build `silver_station_daily` aggregates and write `silver_station_daily_predictions`.

## Testing

Run `pytest` from the project root to exercise the GBFS integration tests. Set a higher timeout if your network is slow by exporting `PYTEST_ADDOPTS="--timeout=30"`.

## Next Ideas

- Blend in weather, events, or elevation features to improve demand forecasts.
- Swap scikit-learn baselines with AutoML or Delta Live Tables to productionize the pipeline.
- Package helper modules into a reusable wheel for future transportation hackathons.
