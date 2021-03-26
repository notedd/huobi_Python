import tushare as ts
from sqlalchemy import create_engine
import pymysql
import constant


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
    data = pro.stock_basic()
    print(data)
    db = get_db()
    cursor = db.cursor()
    cursor.execute("truncate table stock_base_info")
    db.commit()
    conn = get_conn()
    data.to_sql('stock_base_info', con=conn, if_exists='append', index=False)


def produce_stock_info(freq, start_date, end_date, ts_codes):
    if freq == 'D':
        table = "stock_daily"
    elif freq == 'W':
        table = "stock_weekly"
    elif freq == 'M':
        table = "stock_monthly"
    else:
        return
    print(table)
    df = get_ts().pro_bar(ts_code=ts_codes, freq=freq, start_date=start_date, end_date=end_date)
    print(df)
    conn = get_conn()
    df.to_sql(table, con=conn, if_exists='append', index=False)


def get_all_stock_base_info():
    db = get_db()
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    # 使用 execute()  方法执行 SQL 查询
    count = cursor.execute("select ts_code from stock_base_info ")
    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchall()
    db.close()
    return data


def product_daily_info(start_date, end_date):
    data = get_all_stock_base_info()
    ts_code = ""
    for obj in data:
        ts_code = ts_code + "," + obj[0]
    print(ts_code)

    db = get_db()
    cursor = db.cursor()
    sqlo = "delete from stock_daily  where trade_date between '{}' and '{}'"
    sql = sqlo.format(start_date, end_date)
    cursor.execute(sql)
    db.commit()

    produce_stock_info(freq='D', start_date=start_date, end_date=end_date, ts_codes=ts_code)


def product_monthly_info(start_date, end_date):
    data = get_all_stock_base_info()
    ts_code = ""
    count = 0
    for obj in data:
        count = count + 1
        ts_code = ts_code + "," + obj[0]

    db = get_db()
    cursor = db.cursor()
    sqlo = "delete from stock_monthly  where trade_date between '{}' and '{}'"
    sql = sqlo.format(start_date, end_date)
    cursor.execute(sql)
    db.commit()

    produce_stock_info(freq='M', start_date=start_date, end_date=end_date, ts_codes=ts_code)


# produce_stock_base_info()
# produce_stock_info(freq='M', start_date='20210201', end_date='20210331', ts_codes='000004.SZ')
# product_monthly_info("20210101", "20210301")
