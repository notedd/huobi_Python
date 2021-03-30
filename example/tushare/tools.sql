DROP TABLE IF EXISTS stock_base_info;

CREATE TABLE stock_base_info (
     id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
     ts_code varchar(255) DEFAULT NULL COMMENT '股票标识',
     symbol varchar(255) DEFAULT NULL COMMENT '股票代码',
     name varchar(255) DEFAULT NULL COMMENT '股票名称',
     area varchar(255) DEFAULT NULL COMMENT '所在地区',
     industry varchar(255) DEFAULT NULL COMMENT '所属行业',
     fullname varchar(255) DEFAULT NULL COMMENT '股票全称',
     enname varchar(255) DEFAULT NULL COMMENT '英文全称',
     market varchar(255) DEFAULT NULL COMMENT '市场类型',
     exchange varchar(255) DEFAULT NULL COMMENT '交易所代码',
     curr_type varchar(255) DEFAULT NULL COMMENT '交易货币',
     list_date varchar(255) DEFAULT NULL COMMENT '上市时间',
     list_status varchar(255) DEFAULT NULL COMMENT '上市状态',
     delist_date varchar(255) DEFAULT NULL COMMENT '退市时间',
     is_hs varchar(255) DEFAULT NULL COMMENT '是否沪深港通标的',
     update_time timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (id)
) ENGINE = InnoDB;
alter table stock_base_info add unique key stock_base_info_uniq(ts_code);


DROP TABLE IF EXISTS stock_daily_basic;
CREATE TABLE stock_daily_basic (
    id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
	ts_code varchar(20) COMMENT 'TS股票代码',
	trade_date date COMMENT '交易日期',
	close float COMMENT '当日收盘价',
	turnover_rate float COMMENT '换手率（%）',
	turnover_rate_f float COMMENT '换手率（自由流通股）',
	volume_ratio float COMMENT '量比',
	pe float COMMENT '市盈率（总市值/净利润）',
	pe_ttm float COMMENT '市盈率（TTM）',
	pb float COMMENT '市净率（总市值/净资产）',
	ps float COMMENT '市销率',
	ps_ttm float COMMENT '市销率（TTM）',
	dv_ratio float COMMENT '股息率',
    dv_ttm float COMMENT '股息率（TTM）',
	total_share float COMMENT '总股本 （万股）',
	float_share float COMMENT '流通股本 （万股）',
	free_share float COMMENT '自由流通股本 （万）  ',
	total_mv float COMMENT '总市值 （万元）',
	circ_mv float COMMENT '流通市值（万元）',
	PRIMARY KEY (id)
) ENGINE = InnoDB COMMENT '全部股票每日重要的基本面指标';
alter table stock_daily_basic add unique key stock_daily_basic_uniq(ts_code, trade_date);

DROP TABLE IF EXISTS stock_daily;
CREATE TABLE stock_daily (
   id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
   ts_code varchar(20) COMMENT 'TS股票代码',
   trade_date date COMMENT '交易日期',
   open float COMMENT '开盘价',
   high float COMMENT '最高价',
   low float COMMENT '最低价',
   close float COMMENT '收盘价',
   pre_close float COMMENT '昨收价',
   `change` float COMMENT '涨跌额',
   pct_chg float COMMENT '涨跌幅',
   vol float COMMENT '成交量手',
   amount float COMMENT '成交额',
   adj_factor float COMMENT '复权因子',
   PRIMARY KEY (id)
) ENGINE = InnoDB COMMENT '股票每日行情';
alter table stock_daily add unique key stock_daily_uniq(ts_code, trade_date);

DROP TABLE IF EXISTS stock_weekly;
CREATE TABLE stock_weekly (
     id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
     ts_code varchar(20) COMMENT 'TS股票代码',
     trade_date date COMMENT '交易日期',
     open float COMMENT '开盘价',
     high float COMMENT '最高价',
     low float COMMENT '最低价',
     close float COMMENT '收盘价',
     pre_close float COMMENT '昨收价',
     `change` float COMMENT '涨跌额',
     pct_chg float COMMENT '涨跌幅',
     vol float COMMENT '成交量手',
     amount float COMMENT '成交额',
     adj_factor float COMMENT '复权因子',
     PRIMARY KEY (id)
) ENGINE = InnoDB COMMENT '股票每周行情';
alter table stock_weekly add unique key stock_weekly_uniq(ts_code, trade_date);

DROP TABLE IF EXISTS stock_monthly;
CREATE TABLE stock_monthly (
      id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
      ts_code varchar(20) COMMENT 'TS股票代码',
      trade_date date COMMENT '交易日期',
      open float COMMENT '开盘价',
      high float COMMENT '最高价',
      low float COMMENT '最低价',
      close float COMMENT '收盘价',
      pre_close float COMMENT '昨收价',
      `change` float COMMENT '涨跌额',
      pct_chg float COMMENT '涨跌幅',
      vol float COMMENT '成交量手',
      amount float COMMENT '成交额',
      adj_factor float COMMENT '复权因子',
      PRIMARY KEY (id)
) ENGINE = InnoDB COMMENT '股票每月行情';
alter table stock_monthly add unique key stock_monthly_uniq(ts_code, trade_date);

DROP TABLE IF EXISTS index_daily;
CREATE TABLE index_daily (
     id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
     ts_code varchar(20) COMMENT 'TS股票代码',
     trade_date date COMMENT '交易日期',
     open float COMMENT '开盘价',
     high float COMMENT '最高价',
     low float COMMENT '最低价',
     close float COMMENT '收盘价',
     pre_close float COMMENT '昨收价',
     `change` float COMMENT '涨跌额',
     pct_chg float COMMENT '涨跌幅',
     vol float COMMENT '成交量手',
     amount float COMMENT '成交额',
     PRIMARY KEY (id)
) ENGINE = InnoDB COMMENT '指数每日行情';
alter table index_daily add unique key index_daily_uniq(ts_code, trade_date);
