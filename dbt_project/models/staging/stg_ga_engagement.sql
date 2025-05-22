with source as (
    select * from {{ source("alexperrine", "raw_ga_engagement")}}
),

renamed as (
    select
        date as event_date,
        substr(datehourminute, 9, 2) || ':' || substr(datehourminute, 11, 2) as eventtime,
        case 
            when cast(substr(datehourminute, 9, 2) as integer) between 0 and 5 then 'early_morning'
            when cast(substr(datehourminute, 9, 2) as integer) between 6 and 11 then 'morning'
            when cast(substr(datehourminute, 9, 2) as integer) between 12 and 17 then 'afternoon'
            when cast(substr(datehourminute, 9, 2) as integer) between 18 and 21 then 'evening'
            when cast(substr(datehourminute, 9, 2) as integer) between 22 and 23 then 'late_night'
            else 'unknown'
        end as eventtimebucket,
        pagepath,
        pagetitle,
        sessioncampaignid,
        eventname,
        case 
            when lower(eventname) in ('page_view', 'scroll', 'view_item') then 'passive'
            when lower(eventname) in ('click', 'form_submit', 'sign_up') then 'active'
            when lower(eventname) like '%conversion%' then 'conversion'
            else 'other'
        end as eventtype,
        sessionmedium,
        sessionsource,
        case 
            when lower(sessionmedium) = 'organic' then 'organic_search'
            when lower(sessionmedium) in ('cpc', 'ppc', 'paid') then 'paid_search'
            when lower(sessionmedium) in ('email') then 'email'
            when lower(sessionmedium) = 'referral' then 'referral'
            when lower(sessionmedium) = 'social' then 'social'
            else 'other'
        end as session_channel,
        eventcount,
        engagedsessions,
        case 
            when engagedsessions > 0 then true
            else false
        end as is_engaged_session,
        bouncerate,
        case 
            when bouncerate = 100 then true 
            else false 
        end as is_bounce,
        averagesessionduration,
        userengagementduration,
        case 
            when userengagementduration >= 60 then 'high'
            when userengagementduration >= 30 then 'medium'
            else 'low'
        end as engagementdurationtier,
        loaded_date,
        source_system
    from source
        
)

select * from renamed