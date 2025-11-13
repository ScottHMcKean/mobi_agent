![Mobi Vancouver Bike Share banner](img/header.png)

# 🚴‍♂️ Mobi Vancouver Bike Share — Databricks Hackathon Toolkit

This repository provides a lightweight toolkit to help hackathon teams explore and build with Vancouver’s **Mobi by Rogers** bike share data. Everything here is designed for **Databricks Serverless**, **Unity Catalog**, and **Databricks Vector Search**, with strong emphasis on **Genie rooms** as your team’s primary agentic interface.

Your goal:
**Build an intelligent, agent-powered solution that reasons about bike trips, stations, or site content using Databricks-native tools.**
The repo gives you the raw ingredients — you bring the orchestration, automation, and machine learning.

---

# 🔑 What’s Included

### **Data ingestion + standardization (Notebook: 01_data.ipynb)**
Pulls historical monthly trip CSVs from Mobi’s public site, writes them into UC Volumes, and creates **bronze and silver Delta tables**. This notebook gives each team a consistent, queryable base dataset.

### **Utility tools + feature preparation (Notebook: 02_tools.ipynb)**
Includes helper functions, small utilities, scraping support, and the building blocks for generating embeddings, text features, or structured metadata.

### **Retrieval workflows + Vector Search (Notebook: 03_vector_search.ipynb)**
Demonstrates how to work with Databricks Vector Search, create an index from scraped Mobi content, and issue semantic searches. This directly supports Genie room workflows and Q&A agents.

---

# 🧭 How Hackathon Teams Should Use This Repo

This repository is intentionally minimal. It provides:

- historical trip data ingestion
- simple station metadata access via GBFS
- helpers for scraping Mobi’s public website
- utilities for populating vector search indexes
- unified configuration via `config.yaml`
- example workflows you can adapt, extend, or replace

You will build your **own machine learning, ranking, forecasting, or agentic logic** on top of this foundation.

Data for this project was pulled from the public Mobi Vancouver GBFS feeds; for full feed definitions and additional fields you can leverage, check out the official GBFS specification: https://github.com/MobilityData/gbfs

---

# 📚 Repository Overview

### Notebooks
- **`01_data.ipynb`**
  Build bronze and silver trip tables from raw CSVs stored in a UC Volume.
- **`02_tools.ipynb`**
  Shared utilities for scraping, feature prep, data helpers, and prompt-ready transformations.
- **`03_vector_search.ipynb`**
  Create and query a Vector Search index backed by Mobi site content for downstream agent or chatbot use.

### Source code
- **`src/mobi`**
  Python helpers for downloading trip data, processing CSVs to Parquet, scraping Mobi pages, managing configurations, and calling the GBFS API.

### Configuration
- **`config.yaml`**
  Set your Unity Catalog `catalog.schema`, volume names, scraper options, and concurrency settings.

### Documentation
- **`README.md`**
  Lightweight guidance for hackathon participants.

---

# 🔗 Data Sources

- **Trip history CSVs:**
  https://www.mobibikes.ca/en/system-data

- **Station metadata + availability:**
  https://gbfs.kappa.fifteen.eu/gbfs/2.2/mobi/en/

- **Optional web content (scraping):**
  https://www.mobibikes.ca/

---

# 🚀 Suggested Databricks Workflow

1. **Clone or import this repo** into your Databricks workspace.
2. Update `config.yaml` with your **Unity Catalog catalog + schema**.
3. Run **01_data.ipynb** to ingest and standardize trip data.
4. Scrape or import Mobi page content for semantic search.
5. Build a **Vector Search index** using 03_vector_search.ipynb.
6. Create a **Genie room** that:
   - indexes your Delta tables
   - uses your Vector Search endpoint
   - calls Databricks functions/agents/tools you develop
7. Build your ML models, planners, Q&A tools, or automations on top.

---

# 🧩 What You Build Is Up To You

This repo provides the foundation — your team is responsible for:

- intelligent ML models
- retrieval-augmented workflows
- autonomous or agentic flows
- dashboards, apps, planning systems, or orchestrations
- creativity in applying bike-share insights to real-world challenges

---

# ⚙️ Configuration Reminder

Update the following fields in `config.yaml` before running the notebooks:
- `catalog`
- `schema`
- `raw_data_vol`
- scraper settings (optional)
