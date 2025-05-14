# Blog & SEO Content Performance Tracker

## Purpose
This project helps to answer the question:
**Which blog posts and landing pages drive the most traffic, engagement and client interest?**

By tracking visitor sessions, page metadata and keyword relevance, this system reveals which types of content convert best, which need improvement and what new topics are worth creating next.

The goal is to empower Kristen Elizabeth Photography, a high end family and senior photography business in the Twin Cities and my wife, with the ability to make data driven decisions about blogging, SEO and lead generation. By idendifiying top-performing content and content gaps, the business can focus efforts on the blog topics and SEO strategies that are most likely to attract and convert high-value clients.

## Datasets and Technologies

### Datasets

| Source               | Description                                                                 | Format                              | Phase              |
|----------------------|-----------------------------------------------------------------------------|-------------------------------------|--------------------|
| **Google Analytics** | Live pageview/session data with traffic source, device info, and engagement | API-connected or BigQuery export    | AE (Live)          |
| **Semrush Keywords** | Keyword search volume, difficulty, and intent                               | CSV                                 | AE                 |
| **Web Metadata**     | Scraped blog titles, publish dates, meta descriptions, and word count       | Python script output (JSON or CSV)  | AE                 |
| **Google Keyword Planner** | Live SEO keyword                        | CSV or export                       | *Planned for DE Phase*            | *Planned for DE*   |
| **Form Submissions** | Inquiry data (e.g., from HoneyBook or Squarespace)                          | *Planned for DE Phase*                       | *Planned for DE*   |

### Technologies

| Tool          | Role                                                                 |
|---------------|----------------------------------------------------------------------|
| **Iceberg**   | Raw ingestion layer for scalable event and metadata storage         |
| **Snowflake** | Clean, modeled warehouse layer for metrics and dimensional modeling |
| **dbt**       | Build fact/dim models, SCD tracking, windowed metrics                |
| **Airflow**   | Schedule and orchestrate ingestion, scraping, and enrichment tasks  |
| **Spark**     | (Optional) Enrich page metadata or process large keyword joins      |
