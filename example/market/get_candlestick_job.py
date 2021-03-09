import time
from apscheduler.schedulers.blocking import BlockingScheduler
from huobi.client.market import MarketClient
from huobi.constant import *
from huobi.utils import *
import pymysql

def job(symbol,type,size):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print('{} - {} - {}'.format(symbol,type,t))
    market_client = MarketClient(init_log=True)
    interval = type

    list_obj = market_client.get_candlestick(symbol, interval, size)

    if list_obj and len(list_obj):
        db = pymysql.connect(host="127.0.0.1",user="test",password="12345678",database="test")
        for obj in list_obj:
            sqlo = """INSERT INTO kline(time_id,time_type,symbol,high,low,open,close,count,amount,volume)
                     VALUES ({}, '{}', '{}', {},{},{},{},{},{},{})"""
            sql = sqlo.format(obj.id,type,symbol,obj.high,obj.low,obj.open,obj.close,obj.count,obj.amount,obj.vol)
            cursor = db.cursor()
            try:
                if(list_obj.index(obj)==0) :
                    print('跳过',sql)
                    continue
                print(sql)
                cursor.execute(sql)
                db.commit()
            except Exception as e:
                print(e)
                db.rollback()
        db.close()

#job('htusdt',CandlestickInterval.MIN1,2)

scheduler = BlockingScheduler()
scheduler.add_job(job, 'cron', minute='*/1', args=['htusdt',CandlestickInterval.MIN1 ,2])
scheduler.add_job(job, 'cron', minute='*/10', args=['htusdt',CandlestickInterval.MIN15 ,2])
scheduler.add_job(job, 'cron', minute='*/30', args=['htusdt',CandlestickInterval.MIN60 ,2])
scheduler.add_job(job, 'cron', hour='*/12', args=['htusdt',CandlestickInterval.DAY1 ,2])
scheduler.start()
