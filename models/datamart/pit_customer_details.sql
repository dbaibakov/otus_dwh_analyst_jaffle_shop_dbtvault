{{
    config(
        materialized='view'
    )
    
}}

with raw as (
    select
        first_name
        , last_name
        , email
        , null as country
        , null as age
        , effective_from
        , customer_pk
    from {{ ref('sat_customer_details') }}
    union all
    select
        null as first_name
        , null as last_name
        , null as email
        , country
        , age
        , effective_from
        , customer_pk
    from {{ ref('sat_customer_details_crm') }}
)
, collapsed as (
    select
        max(first_name) as first_name
        , max(last_name) as last_name
        , max(email) as email
        , max(country) as country
        , max(age) as age
        , effective_from
        , customer_pk
    from raw
    group by effective_from, customer_pk
)

select
    customer_pk
    , coalesce(first_name, lag(first_name) over (partition by customer_pk order by effective_from)) as first_name
    , coalesce(last_name, lag(last_name) over (partition by customer_pk order by effective_from)) as last_name
    , coalesce(email, lag(email) over (partition by customer_pk order by effective_from)) as email
    , coalesce(country, lag(country) over (partition by customer_pk order by effective_from)) as country
    , coalesce(age, lag(age) over (partition by customer_pk order by effective_from)) as age
    , effective_from
    , coalesce(lead(effective_from) over (partition by customer_pk order by effective_from), '99991231') as effective_to
from collapsed