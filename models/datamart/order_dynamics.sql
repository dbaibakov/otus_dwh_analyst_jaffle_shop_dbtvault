with recursive week as (select min(order_date)::date                       as date_start,
                               (min(order_date) + interval '1 week')::date as date_end
                        from {{ ref('sat_order_details') }}
                        union all
                        select (prev.date_start + interval '1 week')::date, (prev.date_end + interval '1 week')::date
                        from week as prev
                        where prev.date_start < (select max(order_date) from {{ ref('sat_order_details') }}))
select week.date_start
     , ord.status
     , count(1) as count
from week
         join {{ ref('sat_order_details') }} ord
              on ord.order_date >= week.date_start and ord.order_date < week.date_end
group by week.date_start
       , ord.status
