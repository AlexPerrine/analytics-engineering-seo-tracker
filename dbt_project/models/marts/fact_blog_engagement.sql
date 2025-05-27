with daily_rollup as (
    select *
    from {{ ref('int_blog_page_daily_rollup') }}
)

select
    pageview_date,
    pagepath,
    pagetitle,
    referrer_platform,
    pagetype,
    total_pageviews,
    total_engaged_sessions,
    avg_engagement_duration,
    bounce_rate,
    most_common_time_bucket,
    most_common_source,
    most_common_pagetype,
    total_pageviews * total_engaged_sessions as engagement_score
from daily_rollup
where total_pageviews < 100