with pageviews as (
    select
        pageview_date,
        pagepath,
        pagereferrer,
        pagetitle,
        sessionmedium,
        sessionsource,
        referrer_platform,
        pagetype,
        screenpageviews,
        totalusers,
        newusers
    from {{ ref('stg_ga_pageviews') }}
),

engagement as (
    select
        event_date,
        pagepath,
        eventtimebucket,
        eventcount,
        session_channel,
        engagedsessions,
        averagesessionduration,
        bouncerate
    from {{ ref('stg_ga_engagement') }}
)

select
    p.pageview_date,
    p.pagepath,
    p.pagetitle,
    p.pagereferrer,
    p.referrer_platform,
    p.pagetype,
    sum(p.screenpageviews) as total_pageviews,
    sum(e.engagedsessions) as total_engaged_sessions,
    avg(e.averagesessionduration) as avg_engagement_duration,
    avg(e.bouncerate) as bounce_rate,
    any_value(e.eventtimebucket) as most_common_time_bucket,
    any_value(e.session_channel) as most_common_source,
    any_value(p.pagetype) as most_common_pagetype
from pageviews as p
left join engagement as e
    on p.pagepath = e.pagepath
    and p.pageview_date = e.event_date

group by
    p.pageview_date,
    p.pagepath,
    p.pagereferrer,
    p.pagetitle,
    p.referrer_platform,
    p.pagetype