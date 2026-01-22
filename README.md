# 🗺️  GeoMarket Intelligence Pipeline

[![Streamlit App](https://img.shields.io/badge/Streamlit-Live%20App-FF4B4B?logo=streamlit&logoColor=white)](https://geomarket-intelligence-pipeline.streamlit.app/)
[![Database](https://img.shields.io/badge/Database-PostgreSQL-blue)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Infrastructure-Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Automation](https://img.shields.io/badge/Automation-GitHub%20Actions-green)](https://github.com/features/actions)

## 🌟 Overview
This project is a production-grade **Real Estate Intelligence System** designed to automate the collection and analysis of property market data. Unlike basic scrapers, this system implements a professional **Normalized Data Warehouse** architecture to track price fluctuations over time and visualize geographic "Hot-Zones."

The system identifies market opportunities by calculating **Price per Square Foot** trends and automatically segmenting properties into **Luxury, Premium, and Standard** categories based on real-time market distributions.

---

## 🏗️ Architecture
The system utilizes a decoupled, cloud-native architecture:

1.  **Extraction:** Python-based ingestion from the `Realty-in-US` API (RapidAPI) with built-in **Rate-Limiting** and **Pagination** logic.
2.  **Transformation:** Data cleaning and **Feature Engineering** using Pandas. Key metrics include:
    *   **Geo-Clustering:** Grouping properties by coordinate rounding to identify micro-neighborhood trends.
    *   **Market Segmentation:** Algorithmic classification of listings into price-tiers.
3.  **Storage:** A normalized **PostgreSQL** schema (hosted on Supabase) utilizing **SQLAlchemy**. The model separates static property attributes from temporal price history to ensure data integrity.
4.  **Orchestration:** Fully automated via **GitHub Actions** using CRON scheduling to ensure weekly market updates.
5.  **Visualization:** A high-performance **Streamlit** dashboard featuring **PyDeck** geospatial 3D mapping and interactive filtering.

---

## 🚀 Key Features
*   **Relational Database Normalization:** Designed a two-table system (`properties` and `price_history`) with Foreign Key constraints to track historical price changes without duplicating static house data.
*   **Idempotent Upsert Logic:** Implemented `ON CONFLICT` SQL logic to maintain a "Single Source of Truth," allowing the pipeline to update existing listings while appending new price points.
*   **Geospatial Intelligence:** Developed a **Scatterplot Mapping Layer** using PyDeck to visualize property density and color-code market segments across metropolitan areas.
*   **Environment Parity (Docker):** Containerized the local development environment using **Docker Compose** to ensure seamless migration between local testing and cloud production.
*   **Defensive Engineering:** Implemented robust schema validation and `.get()` based data fetching to prevent pipeline failures due to third-party API schema drift.

---

## 🛠️ Tech Stack
*   **Language:** Python 3.10+
*   **Data Analysis:** Pandas, NumPy
*   **Database:** PostgreSQL (Supabase), SQLAlchemy (ORM)
*   **DevOps:** Docker, Docker Compose, GitHub Actions (CI/CD), YAML
*   **Visualization:** Streamlit, PyDeck (Geospatial), Plotly

---

## 📊 Live Dashboard
Access the interactive real estate analytics here: 
👉 **[GeoMarket Intelligence App](https://geomarket-intelligence-pipeline.streamlit.app/)**

---

## 📂 Project Structure
```text
├── .github/workflows/
│   └── main.yml         # GitHub Actions Automation logic
├── main.py              # ETL Script (Ingestion & Normalization)
├── app.py               # Streamlit Dashboard & PyDeck Mapping
├── docker-compose.yml   # Local Infrastructure as Code
├── requirements.txt     # Dependency Management
└── README.md            # Documentation
```

---

## ⚙️ Local Setup
1. **Clone the repo:**
   ```bash
   git clone https://github.com/MANOJ-1410/GeoMarket-Intelligence-Pipeline.git
   ```
2. **Infrastructure (Docker):**
   ```bash
   docker-compose up -d
   ```
3. **Environment Variables:**
   Create a `.env` file with your `RAPIDAPI_KEY` and `DB_URL`.
4. **Run the Dashboard:**
   ```bash
   streamlit run app.py
   ```
