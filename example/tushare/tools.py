import tushare as ts
from sqlalchemy import create_engine
import pymysql
import constant
import time
import numpy as np
import pandas as pd
import datetime


def get_ts():
    ts.set_token(constant.g_token)
    return ts


def get_conn():
    conn = create_engine(
        "mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format("test", "12345678", "127.0.0.1", "3306", "test"))
    return conn


def get_db():
    return pymysql.connect(host=constant.g_host, user=constant.g_user, password=constant.g_password,
                           database=constant.g_database)


def produce_stock_base_info():
    pro = get_ts().pro_api()
    data = pro.stock_basic(
        fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
    # print(data)
    db = get_db()
    cursor = db.cursor()
    cursor.execute("truncate table stock_base_info")
    db.commit()
    conn = get_conn()
    data.to_sql('stock_base_info', con=conn, if_exists='append', index=False)


def get_all_stock_base_info():
    db = get_db()

    cursor = db.cursor()

    cursor.execute(
        "select ts_code,symbol,name,area,industry,fullname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,update_time from stock_base_info ")
    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchall()
    db.close()
    return data


# 导入数据库数据 有长度限制
def produce_kline_info(freq, start_date, end_date, ts_codes):
    if freq == 'D':
        table = "stock_daily"
    elif freq == 'W':
        table = "stock_weekly"
    elif freq == 'M':
        table = "stock_monthly"
    else:
        return
    print(table)

    ts_codes_tushare = ""
    ts_codes_db = ""

    for tscode in ts_codes:
        ts_codes_tushare = ts_codes_tushare + "," + tscode
        ts_codes_db = ts_codes_db + "'" + tscode + "',"

    print(ts_codes_tushare[1:])
    print(ts_codes_db[0:-1])

    # 查询数据
    df = get_ts().pro_bar(ts_code=ts_codes_tushare[1:], freq=freq, start_date=start_date, end_date=end_date)

    print(df)

    # 先删除已有的数据
    db = get_db()
    cursor = db.cursor()
    sqlo = "delete from {}  where trade_date between '{}' and '{}' and ts_code in ({})"
    sql = sqlo.format(table, start_date, end_date, ts_codes_db[0:-1])
    # print(sql)
    cursor.execute(sql)
    db.commit()

    # 导入数据
    conn = get_conn()
    df.to_sql(table, con=conn, if_exists='append', index=False)


def product_kline_info_all(freq, start_date, end_date, size):
    data = get_all_stock_base_info()
    ts_codes = []
    for obj in data:
        ts_codes.append(obj[0])

    for ts_codes_temp in partition(ts_codes, size):
        print("#####################################################################################")
        if freq == 'M':
            time.sleep(1)
        produce_kline_info(freq=freq, start_date=start_date, end_date=end_date, ts_codes=ts_codes_temp)


def partition(ls, size):
    """
    Returns a new list with elements
    of which is a list of certain size.
        >>> partition([1, 2, 3, 4], 3)
        [[1, 2, 3], [4]]
    """
    return [ls[i:i + size] for i in range(0, len(ls), size)]


def produce_daily_basic(trade_date, ts_codes):
    ts_codes_tushare = ""
    ts_codes_db = ""

    for tscode in ts_codes:
        ts_codes_tushare = ts_codes_tushare + "," + tscode
        ts_codes_db = ts_codes_db + "'" + tscode + "',"

    print(ts_codes_tushare[1:])
    print(ts_codes_db[0:-1])

    # 查询数据
    df = get_ts().pro_api().daily_basic(ts_code=ts_codes_tushare[1:], trade_date=trade_date,
                                        fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv')

    print(df)

    # 先删除已有的数据
    db = get_db()
    cursor = db.cursor()
    sqlo = "delete from stock_daily_basic where trade_date between '{}' and '{}' and ts_code in ({})"
    sql = sqlo.format(trade_date, trade_date, ts_codes_db[0:-1])
    # print(sql)
    cursor.execute(sql)
    db.commit()

    # 导入数据
    conn = get_conn()
    df.to_sql('stock_daily_basic', con=conn, if_exists='append', index=False)


def produce_daily_basic_all(trade_date, size):
    data = get_all_stock_base_info()
    ts_codes = []
    for obj in data:
        ts_codes.append(obj[0])

    for ts_codes_temp in partition(ts_codes, size):
        print("#####################################################################################")
        produce_daily_basic(trade_date, ts_codes=ts_codes_temp)


def testnp():
    #换手率
    min_turnover_rate = 10
    max_turnover_rate = 100

    #量比
    min_volume_ratio = 2
    max_volume_ratio = 100

    #市盈率
    min_pe = 0
    max_pe =100

    #总市值
    min_total_mv = 10
    max_total_mv = 5000

    today = datetime.datetime.now()
    yestoday = today + datetime.timedelta(days=-1)
    day = yestoday.strftime('%Y%m%d')
    list_date = day

    db = get_db()
    cursor = db.cursor()
    sqlo = """select a.ts_code,b.name,a.trade_date,a.close,a.turnover_rate,volume_ratio,pe,round(a.total_mv/10000,0) as total_mv,round(a.circ_mv/10000,0) as circ_mv,
            b.market,b.area,b.industry,b.list_date from stock_daily_basic a inner join stock_base_info b on a.ts_code=b.ts_code
            where trade_date between '{}' and '{}' and list_date<='{}' """
    sql = sqlo.format(day, day, list_date)
    cursor.execute(sql)
    data = cursor.fetchall()
    db.close()

    df = pd.DataFrame(list(data))

    df = df[(df[4] >= min_turnover_rate) & (df[4] <= max_turnover_rate) & (df[5] >= min_volume_ratio) & (df[5] <= max_volume_ratio) & (
                        df[6] >= min_pe) & (df[7] <= max_pe) & (df[7] >= min_total_mv) & (df[7] <= max_total_mv)]

    df = df.sort_values(by=[7], ascending=False)

    print(df)


# testnp()
# produce_stock_base_info()
# produce_kline_info(freq='D', start_date='20210326', end_date='20210329', ts_codes=['000001.SZ','000004.SZ'])
# product_kline_info_all('D', "20210324", "20210324",100)
# product_kline_info_all('M', "20210129", "20210129",1)
# produce_daily_basic_all(trade_date='20210324', size=100)
