# Blog & SEO Content Performance Tracker

## Purpose
This project helps to answer the question:
**Which blog posts and landing pages drive the most traffic, engagement and client interest?**

By tracking visitor sessions, blog metadata, and keyword relevance, this system reveals which types of content convert best, which need improvement, and what new topics are worth creating next.

The goal is to empower Kristen Elizabeth Photography — a high-end family and senior photography business — with the ability to make data-driven decisions about blogging, SEO, and client acquisition. By identifying top-performing content and high-demand search gaps, the business can focus marketing efforts where they’ll have the most impact.

## Datasets and Technologies

### Datasets

| Source               | Description                                                                 | Format                              | Phase              |
|----------------------|-----------------------------------------------------------------------------|-------------------------------------|--------------------|
| **Google Analytics** | Live pageview/session data with traffic source, device info, and engagement | API-connected or BigQuery export    | AE (Live)          |
| **Semrush Keywords** | Keyword search volume, difficulty, and intent                               | CSV                                 | AE                 |
| **Web Metadata**     | Scraped blog titles, publish dates, meta descriptions, and word count       | Python script output (JSON or CSV)  | AE                 |
| **Google Keyword Planner** | Live Keyword volume                        | CSV or export                       | *Planned for DE Phase*            | *Planned for DE*   |
| **Form Submissions** | Inquiry data (e.g., from HoneyBook or Squarespace)                          | *Planned for DE Phase*                       | *Planned for DE*   |

### Technologies

| Tool          | Role                                                                 |
|---------------|----------------------------------------------------------------------|
| **Iceberg**   | Raw ingestion layer for scalable event and metadata storage         |
| **Snowflake** | Clean, modeled warehouse layer for metrics and dimensional modeling |
| **dbt**       | Build fact/dim models, SCD tracking, windowed metrics                |
| **Airflow**   | Schedule and orchestrate ingestion, scraping, and enrichment tasks  |
| **Spark**     | (Optional) Enrich page metadata or process large keyword joins      |

## Workflow
**1.** Live Google Analytics data ingestion daily into Iceberg
**2.** Page metadata scrapped and enriched using Python or Spark
**3.** Semrush keywords loaded and matched to blog metadata
**4.** dbt builds final models
    `fact_page_views`, `dim_page_content`, `dim_keywords`, `dim_device`, `dim_traffic_source`
**5.** KPIs exposed via dbt models and visualized in a BI tool (not sure which yet.)

Note. The I have joined the Data Engineering bootcamp that follows this and will add inquiry tracking and keyword gap analysis using Google Keyword Planner data.

## Challenges Faced:
- Building a live GA data flow requires setting up authenticated API access and schema parsing
- Matching Semrush keywords to blog posts needed a mix of fuzzy logic and manual tagging
- Tracking blog updates accross time

## Future Enhancements
- Add form submission ingestion and join it to session data
- Integrate Google Keyword Planner API for real time keyword opportunity analysis
- Use Search Console data for impression and ranking validation
- Build a lightweight dashboard for editorial content planning
- Predict decaying blog content and suggest updates using ML models