-- [Unit Test] Scalar Functions
with a as (
    select date '2022-05-16' as d
)
-- Expected Output:
-- 0.47325298871196086
-- 1
-- 2022-05-01
-- 2022-05-31
-- 2022-05-15
-- 2022-05-15 23:59:59.999 +08:00
-- 2022-05-16 23:59:59.999 +08:00
-- 2022-05-16 23:59:59.999 +08:00
select
    rand('123') as rand,
    array_max_count_element(array['1', '2', '1']) as array_max_count_element,
    first_day(d) as first_day,
    last_day(d) as last_day,
    yesterday() as yesterday,
    yesterday_last_second() as yesterday_last_second,
    last_second(d) as last_second,
    to_datetime(d, '23:59:59.999999') as to_datetime
from a;


-- [Unit Test] Aggregate Functions
with a as (
    select 'guangzhou' as city, 'tianhe'  as district, array[1,2,3] as item_ids
    union all
    select 'guangzhou' as city, 'baiyun'  as district, array[2,3,4] as item_ids
    union all
    select 'guangzhou' as city, 'baiyun'  as district, null as item_ids
    union all
    select 'shenzhen'  as city, 'futian'  as district, array[1,2] as item_ids
    union all
    select 'shenzhen'  as city, 'nanshan' as district, array[1,3,3] as item_ids
)
-- Expected Output:
-- guangzhou baiyun 4
-- shenzhen  futian 3
select
    city,
    max_count_element(district),
    array_agg_distinct_integer(item_ids) as sku_cnt
from a
group by city
order by city;
