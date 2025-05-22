with source as (
    select * from {{ source("alexperrine", "raw_ga_users")}}
),

renamed as(
    select
        date as eventdate,
        firstsessiondate,
        city,
        region,
        browser,
        devicecategory,
        operatingsystem,
        platform,
        searchterm,
        sessionsperuser,
        engagementrate,
        totalusers,
        activeusers,
        loaded_date,
        source_system
    from source
)

select * from renamed