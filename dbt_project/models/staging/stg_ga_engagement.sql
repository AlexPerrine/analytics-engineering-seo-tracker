with source as (
    select * from {{ source("alexperrine", "raw_ga_engagement")}}
),

renamed as (
    select
        date as event_date,
        substr(datehourminute, 9, 2) || ':' || substr(datehourminute, 11, 2) as event_time,
        case 
            when cast(substr(datehourminute, 9, 2) as integer) between 0 and 5 then 'early_morning'
            when cast(substr(datehourminute, 9, 2) as integer) between 6 and 11 then 'morning'
            when cast(substr(datehourminute, 9, 2) as integer) between 12 and 17 then 'afternoon'
            when cast(substr(datehourminute, 9, 2) as integer) between 18 and 21 then 'evening'
            when cast(substr(datehourminute, 9, 2) as integer) between 22 and 23 then 'late_night'
            else 'unknown'
        end as event_time_bucket,
        pagepath,
        pagetitle,
        sessioncampaignid,
        eventname,
        sessionmedium,
        sessionsource,
        eventcount,
        engagedsessions,
        bouncerate,
        averagesessionduration,
        userengagementduration,
        loaded_date,
        source_system
    from source
        
)

select * from renamed