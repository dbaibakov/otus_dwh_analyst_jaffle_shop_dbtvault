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
        , 1 as source
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
        , 2 as source
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
        , bit_or(source) as source
    from raw
    group by effective_from, customer_pk
)

select
    customer_pk
    , coalesce(first_name, case when source & 1 = 0 then lag(first_name) over (partition by customer_pk order by effective_from) end) as first_name
    , coalesce(last_name, case when source & 1 = 0 then lag(last_name) over (partition by customer_pk order by effective_from) end) as last_name
    , coalesce(email, case when source & 1 = 0 then lag(email) over (partition by customer_pk order by effective_from) end) as email
    , coalesce(country, case when source & 2 = 0 then lag(country) over (partition by customer_pk order by effective_from) end) as country
    , coalesce(age, case when source & 2 = 0 then lag(age) over (partition by customer_pk order by effective_from) end) as age
    , effective_from
    , coalesce(lead(effective_from) over (partition by customer_pk order by effective_from), '99991231') as effective_to
from collapsed