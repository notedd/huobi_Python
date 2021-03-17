import time
from huobi.client.market import MarketClient
from huobi.constant import *
from huobi.utils import *
import pymysql


def get_db():
    return pymysql.connect(host=g_host, user=g_user, password=g_password, database=g_database)


# 获取最近24小时所有交易对的ticker信息入库
def produce_last24_tickers():
    market_client = MarketClient(init_log=True)
    list_obj = market_client.get_market_tickers();

    db = get_db()
    if list_obj and len(list_obj):

        cursor = db.cursor()
        cursor.execute("truncate table tickers")
        db.commit()

        for obj in list_obj:
            try:
                rise_percent = 0;
                shake_percent = 0;
                rise = obj.close - obj.open
                shake = obj.high - obj.low
                if (abs(rise) > 1.0e-16):
                    rise_percent = round(rise / obj.open, 4)
                if (abs(shake) > 1.0e-16):
                    shake_percent = round(shake / obj.high, 4)

                sqlo = """INSERT INTO tickers(symbol,high,low,open,close,count,amount,volume,rise,rise_percent,shake,shake_percent)
                         VALUES ('{}',{},{},{},{},{},{},{},{},{},{},{})"""
                sql = sqlo.format(obj.symbol, obj.high, obj.low, obj.open, obj.close, obj.count, obj.amount, obj.vol,
                                  rise,
                                  rise_percent, shake, shake_percent)
                print(sql)
                cursor = db.cursor()
                cursor.execute(sql)
                db.commit()
            except Exception as e:
                print(e)
                db.rollback()
        db.close()


# 获取某个交易对kline数据入库
def produce_symbol_kline(symbol, type, size):
    market_client = MarketClient(init_log=True)
    interval = type
    list_obj = market_client.get_candlestick(symbol, interval, size)

    if list_obj and len(list_obj):
        db = get_db()
        for obj in list_obj:
            rise_percent = 0;
            shake_percent = 0;
            rise = obj.close - obj.open
            shake = obj.high - obj.low
            if (abs(rise) > 1.0e-16):
                rise_percent = round(rise / obj.open, 4)
            if (abs(shake) > 1.0e-16):
                shake_percent = round(shake / obj.high, 4)

            sqlo = """INSERT INTO kline(time_id,time_type,symbol,high,low,open,close,count,amount,volume,rise,rise_percent,shake,shake_percent)
                     VALUES ({}, '{}', '{}', {},{},{},{},{},{},{},{},{},{},{})"""
            sql = sqlo.format(obj.id, type, symbol, obj.high, obj.low, obj.open, obj.close, obj.count, obj.amount,
                              obj.vol, rise,
                              rise_percent, shake, shake_percent)
            cursor = db.cursor()
            try:
                if (list_obj.index(obj) == 0):
                    print('跳过', sql)
                    continue
                print(sql)
                cursor.execute(sql)
                db.commit()
            except Exception as e:
                print(e)
                db.rollback()
        db.close()


# 从数据里面根据条件查询交易对
def get_symbol_fromdb(symbol, vol, rise_percent, size):
    try:
        db = get_db()
        cursor = db.cursor()
        sqlo = """select symbol,volume,rise_percent from tickers where symbol like '{}' and volume>{} and rise_percent > {} order by volume desc limit {}"""
        sql = sqlo.format(symbol, vol, rise_percent, size)
        print(sql)
        count = cursor.execute(sql)
        data = cursor.fetchall()
        db.commit()
        return data
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


# 根据条件选中交易对生成klines数据
def produce_all_klines(min1, min15, min60, day1):
    produce_last24_tickers()
    symbols = get_symbol_fromdb('%usdt', 10000000, 0.01, 20)
    for obj in symbols:
        symbol = obj[0]
        produce_symbol_kline(symbol, CandlestickInterval.MIN1, min1)
        produce_symbol_kline(symbol, CandlestickInterval.MIN15, min15)
        produce_symbol_kline(symbol, CandlestickInterval.MIN60, min60)
        produce_symbol_kline(symbol, CandlestickInterval.DAY1, day1)


# 从数据库查询kline聚合信息
def get_symbol_klineinfos_fromdb(symbol, time_type, lastcount):
    try:
        db = get_db()
        cursor = db.cursor()
        sqlo = """select max(rise_percent) as max_rise_percent,min(rise_percent) as min_rise_percent,
                         max(shake_percent) as max_shake_percent,min(shake_percent) as min_shake_percent,
                         avg(rise_percent) as avg_rise_percent,avg(shake_percent) as avg_shake_percent
                  from kline where symbol = '{}' and time_type='{}' order by time_id desc limit {}"""
        sql = sqlo.format(symbol, time_type, lastcount)
        # print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        db.commit()
        return data
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


# 产生分析数据 交易对的聚合情况
def produce_symbol_klines(min1, min15, min60, day1):
    symbols = get_symbol_fromdb('%usdt', 10000000, 0.01, 20)
    data = {}
    times = {}

    db = get_db()
    cursor = db.cursor()
    cursor.execute("truncate table symbol_klines")
    db.commit()

    for obj in symbols:
        symbol = obj[0]
        data[CandlestickInterval.MIN1] = get_symbol_klineinfos_fromdb(symbol, CandlestickInterval.MIN1, min1)
        times[CandlestickInterval.MIN1] = min1;
        data[CandlestickInterval.MIN15] = get_symbol_klineinfos_fromdb(symbol, CandlestickInterval.MIN15, min15)
        times[CandlestickInterval.MIN15] = min15;
        data[CandlestickInterval.MIN60] = get_symbol_klineinfos_fromdb(symbol, CandlestickInterval.MIN60, min60)
        times[CandlestickInterval.MIN60] = min60;
        data[CandlestickInterval.DAY1] = get_symbol_klineinfos_fromdb(symbol, CandlestickInterval.MIN1, day1)
        times[CandlestickInterval.DAY1] = day1;

        print(symbol)

        for objo in data:
            objone = data[objo]
            print(objo)
            print(objone)
            time = times[objo]
            print(time)

            if objone:

                try:
                    cursor = db.cursor()
                    sqlo = """INSERT INTO symbol_klines(symbol,time_type,times,max_rise_percent,max_shake_percent,
                              min_rise_percent,min_shake_percent,avg_rise_percent,avg_shake_percent)
                                 VALUES ('{}','{}',{},{},{},{},{},{},{})"""
                    sql = sqlo.format(symbol, objo, time, objone[0][0], objone[0][2], objone[0][1], objone[0][3],
                                      objone[0][4], objone[0][5])
                    print(sql)
                    cursor = db.cursor()
                    cursor.execute(sql)
                    db.commit()
                except Exception as e:
                    print("error")
                    print(e)
                    db.rollback()
        db.close



#produce_symbol_klines(0, 12, 3, 0)

