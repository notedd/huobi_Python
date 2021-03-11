import time
from huobi.client.market import MarketClient
from huobi.constant import *
from huobi.utils import *
import pymysql


def produce_tickers():
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print('开始定时拉取任务 {}'.format(t))
    market_client = MarketClient(init_log=True)
    list_obj = market_client.get_market_tickers();

    if list_obj and len(list_obj):
        db = pymysql.connect(host="127.0.0.1", user="test", password="12345678", database="test")
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


def produce_kline(symbol, type, size):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print('{} - {} - {}'.format(symbol, type, t))
    market_client = MarketClient(init_log=True)
    interval = type

    list_obj = market_client.get_candlestick(symbol, interval, size)

    if list_obj and len(list_obj):
        db = pymysql.connect(host="127.0.0.1", user="test", password="12345678", database="test")
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


def select_kline_symbol(symbol, vol, rise_percent, size):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print('开始选取拉取kline交易对 {}'.format(t))
    try:
        db = pymysql.connect(host="127.0.0.1", user="test", password="12345678", database="test")
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


def produce_klines(min1, min15, min60, day1):
    produce_tickers()
    symbols = select_kline_symbol('%usdt', 10000000, 0.01, 20)
    for obj in symbols:
        symbol = obj[0]
        produce_kline(symbol, CandlestickInterval.MIN1, min1)
        produce_kline(symbol, CandlestickInterval.MIN15, min15)
        produce_kline(symbol, CandlestickInterval.MIN60, min60)
        produce_kline(symbol, CandlestickInterval.DAY1, day1)


#produce_klines(60, 96, 24, 30);

#produce_klines(5,2,2,2);

# select_kline_symbol('%usdt', 10000000, 0.01, 20)
