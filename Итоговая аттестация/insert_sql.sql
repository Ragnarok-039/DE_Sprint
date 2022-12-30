insert into company_rates_showcase select * from (
with open_table as (select * from
(select *,
row_number() over(partition by symbol, date(time_series_30min) order by time_series_30min) as num
from company_rates) as temp
where temp.num = 1),

close_table as (select * from
(select *,
row_number() over(partition by symbol, date(time_series_30min) order by time_series_30min desc) as num
from company_rates) as temp
where temp.num = 1),

total_volume as (select symbol, date(time_series_30min) as date, sum(volume) as total_volume
from company_rates
group by date(time_series_30min), symbol),

max_volume as (select * from
(select *,
row_number() over(partition by symbol, date(time_series_30min) order by volume desc) as num
from company_rates) as temp
where temp.num = 1),

max_rate as (select * from
(select *,
row_number() over(partition by symbol, date(time_series_30min) order by high desc) as num
from company_rates) as temp
where temp.num = 1),

min_rate as (select * from
(select *,
row_number() over(partition by symbol, date(time_series_30min) order by low) as num
from company_rates) as temp
where temp.num = 1)

select md5(concat(ot.symbol, date(ot.time_series_30min))) as key,
ot.symbol,
date(ot.time_series_30min) as date,
tv.total_volume,
ot.open,
ct.close,
ot.open - ct.close as difference,
round((ct.close * 100) / ot.open, 2) as difference_percent,
mv.time_series_30min as largest_trading_volume,
mr.time_series_30min as max_rate,
min_r.time_series_30min as min_rate
from open_table as ot
left join close_table as ct
on ot.symbol = ct.symbol and date(ot.time_series_30min) = date(ct.time_series_30min)
left join total_volume as tv
on ot.symbol = tv.symbol and date(ot.time_series_30min) = tv.date
left join max_volume as mv
on ot.symbol = mv.symbol and date(ot.time_series_30min) = date(mv.time_series_30min)
left join max_rate as mr
on ot.symbol = mr.symbol and date(ot.time_series_30min) = date(mr.time_series_30min)
left join min_rate as min_r
on ot.symbol = min_r.symbol and date(ot.time_series_30min) = date(min_r.time_series_30min)
where date(ot.time_series_30min) = date(now()) - interval '1 day') as total;
