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
        case
            when pagereferrer like '%google.com%' then 'google_search'
            when lower(pagereferrer) like '%gclid=%' then 'paid_google_search'
            when pagereferrer like '%instagram.com%' then 'instagram'
            when lower(pagereferrer) like '%featured-links%' then 'instagram'
            when lower(pagereferrer) like '%quick-links%' then 'instagram'
            when pagereferrer like '%facebook.com%' then 'facebook'
            when lower(pagereferrer) like '%fbclid=%' then 'facebook'
            when pagereferrer like '%pinterest.com%' then 'pinterest'
            when pagereferrer like '%linkedn.com%' then 'linkedin'
            when pagereferrer like '%yelp.com%' then 'yelp'
            when pagereferrer like '%stan.store%' then 'product_sales'
            when pagereferrer like '%semrush.com%' then 'site_development'
            when pagereferrer like '%showit%' then 'site_development'
            when pagereferrer like '%pic-time%' then 'site_development'
            else 'other'
        end as referrer_platform,
        pagetitle,
        case
            when pagepath = '/' then 'home_page'
            when lower(pagetitle) like  '%blog%' then 'blog'
            when pagepath like '/home' then 'home_page'
            when pagepath like '/about' then 'about_page'
            when pagepath like '/all-about-me/' then 'about_page'
            when pagepath like '/contact' then 'contact_page'
            when pagepath like '/family-photography' then 'family_page'
            when pagepath like '/family-and-children-portraits/' then 'family_page'
            when pagepath like '/headshot-and-branding-photography' then 'branding_page'
            when pagepath like '/maternity-and-newborn-photography'then 'maternity_page'
            when pagepath like '/maternity/' then 'maternity_page'
            when pagepath like '/mini-sessions' then 'minis_page'
            when pagepath like '/senior-pictures' then 'seniors_page'
            when pagepath like '/seinor-pictures' then 'seniors_page'
            when pagepath like '/senior-rep-team' then 'seniors_page'
            when pagepath like '/senior-testimonials' then 'seniors_page'
            when pagepath like '/for-photographers' then 'photograhers_page'
            when pagepath like '/frequently-asked-questions/' then 'faq_page'
            else 'other'
        end as pagetype,
        screenpageviews,
        case 
            when screenpageviews >= 3 then 'engaged'
            when screenpageviews = 1 then 'bounce'
            else 'light'
        end as session_quality,
        sessionmedium,
        sessions,
        sessionsource,
        totalusers,
        userengagementduration,
        loaded_date,
        source_system
    from source
    WHERE sessionSource != 'leadsgo.io' OR userEngagementDuration < 3600
)

select * from renamed