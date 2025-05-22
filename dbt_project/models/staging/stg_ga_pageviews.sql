with source as (
    select * from {{ source("alexperrine", "raw_ga_pageviews")}}
),

renamed as (
    select
        date as pageview_date,
        landingpage,
        landingpageplusquerystring,
        newusers,
        pagepath,
        pagereferrer,
        pagetitle,
        screenpageviews,
        sessionmedium,
        sessions,
        sessionsource,
        totalusers,
        userengagementduration,
        loaded_date,
        source_system
    from source
)

select * from renamed