with users as (
    select
        eventdate as session_date,
        city,
        region,
        devicecategory,
        device_type,
        operatingsystem,
        platform,
        sessionsperuser,
        engagementrate,
        engagement_tier,
        totalusers,
        activeusers,
        user_recency_segment
    from {{ ref('stg_ga_users') }}
),

pageviews as (
    select
        pageview_date,
        sessionmedium,
        sessionsource,
        session_quality,
    from {{ ref('stg_ga_pageviews') }}
        group by
        pageview_date,
        sessionmedium,
        sessionsource,
        session_quality
),

combined as (
    select
        u.session_date,
        u.city,
        u.region,
        u.devicecategory,
        u.device_type,
        u.operatingsystem,
        u.platform,
        u.sessionsperuser,
        u.engagementrate,
        u.engagement_tier,
        u.totalusers,
        u.activeusers,
        u.user_recency_segment,
        any_value(p.session_quality) as top_session_quality
    from users as u
    left join pageviews as p
        on u.session_date = p.pageview_date
    group by
        u.session_date,
        u.city,
        u.region,
        u.devicecategory,
        u.operatingSystem,
        u.platform,
        u.device_type,
        u.sessionsperuser,
        u.engagementrate,
        u.engagement_tier,
        u.totalusers,
        u.activeusers,
        u.user_recency_segment
)

select * from combined