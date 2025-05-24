with events as (
    select
        event_date,
        substr(eventtime, 1, 2)::integer as event_hour,
        pagepath,
        pagetitle,
        eventtype,
        eventcount,
        engagedsessions,
        userengagementduration,
        bouncerate,
        sessionmedium,
        sessionsource,
        session_channel,
        eventtimebucket,
    from {{ ref('stg_ga_engagement') }}
)

select
    event_date,
    event_hour,
    pagepath,
    pagetitle,
    eventtype,
    session_channel,
    eventtimebucket,
    sum(eventcount) as total_events,
    sum(engagedsessions) as total_engaged_sessions,
    avg(userengagementduration) as avg_engagement_duration,
    avg(bouncerate) as bounce_rate,
    any_value(session_channel) as top_source_channel,
from events
group by
    event_date,
    event_hour,
    pagepath,
    pagetitle,
    eventtype,
    session_channel,
    eventtimebucket