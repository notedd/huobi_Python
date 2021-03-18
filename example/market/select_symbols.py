import time
from huobi.client.market import MarketClient
from huobi.constant import *
from huobi.utils import *
import pymysql


def get_db():
    return pymysql.connect(host=g_host, user=g_user, password=g_password, database=g_database)


def get_select_symbols_fromdb():
    try:
        select_symbols = {}
        db = get_db()
        cursor = db.cursor()
        day = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        sqlo = """select symbol,day,zhiying_scale,zhisun_scale,create_time,update_time
                  from select_symbols where day = '{}' """
        sql = sqlo.format(day)
        print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        for obj in data:
            record = {}
            record['symbol'] = obj[0]
            record['zhiying_scale'] = obj[2]
            record['zhisun_scale'] = obj[3]
            select_symbols[obj[0]] = record
        db.commit()
        return select_symbols
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


# 从数据库查询kline聚合信息
def get_symbol_klines_fromdb():
    try:
        symbol_klines = {}
        db = get_db()
        cursor = db.cursor()
        sql = """select symbol,create_time,time_type,times,max_rise_percent,max_shake_percent,
                              min_rise_percent,min_shake_percent,avg_rise_percent,avg_shake_percent
                  from symbol_klines where create_time > (now()-6000000) """
        print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        for obj in data:
            record = {}
            record['symbol'] = obj[0]
            record['time_type'] = obj[2]
            record['max_rise_percent'] = obj[4]
            record['max_shake_percent'] = obj[5]
            record['min_rise_percent'] = obj[6]
            record['min_shake_percent'] = obj[7]
            record['avg_rise_percent'] = obj[8]
            record['avg_shake_percent'] = obj[9]
            symbol_klines[obj[0]] = record
        db.commit()
        return symbol_klines
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


print(get_symbol_klines_fromdb())
