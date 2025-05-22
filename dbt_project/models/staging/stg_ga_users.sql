with source as (
    select * from {{ source("alexperrine", "raw_ga_users")}}
),

renamed as(
    select
        date as eventdate,
        firstsessiondate,
        datediff(day, firstsessiondate, date) as user_days_active,
        case 
            when datediff(day, firstsessiondate, date) = 0 then 'new'
            when datediff(day, firstsessiondate, date) <= 7 then 'returning_week'
            when datediff(day, firstsessiondate, date) <= 28 then 'returning_month'
            else 'repeat_long_term'
        end as user_recency_segment,
        city,
        region,
        browser,
        devicecategory,
        case 
            when lower(devicecategory) in ('mobile', 'tablet') then 'mobile'
            when lower(devicecategory) = 'desktop' then 'desktop'
            else 'other'
        end as device_type,
        operatingsystem,
        platform,
        case
            when lower(platform) like '%web%' then 'web'
            when lower(platform) like '%amp%' then 'amp'
            when lower(platform) like '%app%' then 'app'
            else 'other'
        end as platform_group,
        searchterm,
        sessionsperuser,
        engagementrate,
        case
            when engagementrate >= 0.8 then 'high'
            when engagementrate >= 0.4 then 'medium'
            else 'low'
        end as engagement_tier,
        totalusers,
        activeusers,
        loaded_date,
        source_system
    from source
)

select * from renamed