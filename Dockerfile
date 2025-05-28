FROM quay.io/astronomer/astro-runtime:11.3.0

# USER root
# COPY ./dbt_project ./dbt_project
# COPY --chown=astro:0 . .


# Set environment variables for dbt
ENV DBT_SCHEMA=alexperrine
ENV DBT_PROFILES_DIR=.
ENV DBT_PROJECT_DIR=.