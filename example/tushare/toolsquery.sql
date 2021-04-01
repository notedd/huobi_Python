

select a.ts_code,b.name,a.trade_date,a.close,a.turnover_rate,volume_ratio,pe,round(a.total_mv/10000,0) as total_mv,round(a.circ_mv/10000,0) as circ_mv,
b.market,b.area,b.industry,b.list_date from stock_daily_basic a inner join stock_base_info b on a.ts_code=b.ts_code
where trade_date='20210330' and list_date<'20210230' order by turnover_rate desc limit 20;

select a.ts_code,b.name,a.trade_date,a.close,a.turnover_rate,volume_ratio,pe,round(a.total_mv/10000,0) as total_mv,round(a.circ_mv/10000,0) as circ_mv,
b.market,b.area,b.industry,b.list_date from stock_daily_basic a inner join stock_base_info b on a.ts_code=b.ts_code
where a.ts_code='605050.SH';

select a.ts_code,b.name,a.trade_date,a.close,a.turnover_rate,volume_ratio,pe,round(a.total_mv/10000,0) as total_mv,round(a.circ_mv/10000,0) as circ_mv,
b.market,b.area,b.industry,b.list_date from stock_daily_basic a inner join stock_base_info b on a.ts_code=b.ts_code
where trade_date='20210330' and b.industry='医疗保健' order by turnover_rate desc limit 20;

select distinct industry from stock_base_info;

select ts_code,name from stock_base_info where area='湖南';


select a.ts_code,b.name,a.trade_date,a.close,a.turnover_rate,volume_ratio,pe,round(a.total_mv/10000,0) as total_mv,round(a.circ_mv/10000,0) as circ_mv,
b.market,b.area,b.industry,b.list_date from stock_daily_basic a inner join stock_base_info b on a.ts_code=b.ts_code
where trade_date between '20210326' and '20210330' and list_date<'20210230' limit 10


select a.ts_code,b.name,a.trade_date,a.close,a.turnover_rate,volume_ratio,pe,round(a.total_mv/10000,0) as total_mv,round(a.circ_mv/10000,0) as circ_mv,
b.market,b.area,b.industry,b.list_date from stock_daily_basic a inner join stock_base_info b on a.ts_code=b.ts_code
where trade_date='20210330' and area='湖南' order by total_mv desc ;

