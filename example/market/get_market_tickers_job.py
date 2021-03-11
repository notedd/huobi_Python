import time
from huobi.client.market import MarketClient
from huobi.constant import *
from huobi.utils import *
import pymysql


def job():
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

    print(len(list_obj))


job()
