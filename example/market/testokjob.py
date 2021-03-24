import time
from huobi.client.market import MarketClient
from huobi.constant import *
from huobi.utils import *
import pymysql
import produce_info_job


def get_db():
    return pymysql.connect(host=g_host, user=g_user, password=g_password, database=g_database)


def get_order_fromdb():
    try:
        orders = []
        db = get_db()
        cursor = db.cursor()
        sql = """select id,symbol,price,round(create_time/1000,0) as create_time,test_type,test_yin,test_sun,result_type
                  from `order` where result_type='none' and test_type='test' """
        # print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        for obj in data:
            record = {}
            record['id'] = obj[0]
            record['symbol'] = obj[1]
            record['create_time'] = obj[3]
            record['test_yin'] = obj[5]
            record['test_sun'] = obj[6]
            orders.append(record)
        db.commit()
        return orders
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


def update_order(id, symbol, result_type, result_time):
    try:
        db = get_db()
        cursor = db.cursor()
        sqlo = """update `order` set result_type='{}',result_time={} where id={} and symbol='{}'"""
        sql = sqlo.format(result_type, result_time, id, symbol)
        print(sql)
        cursor.execute(sql)
        db.commit()
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


def get_last_zhiyintime(symbol, time_type, time_id, test_yin):
    try:
        db = get_db()
        cursor = db.cursor()
        sqlo = """select from_unixtime(time_id,'%Y-%m-%d %H:%i:%s') as time,time_id,high,open,low,close
                    from kline where symbol='{}' and time_type='{}' and high>{} and time_id>{} order by time_id limit 1;"""
        sql = sqlo.format(symbol, time_type, test_yin, time_id)

        cursor.execute(sql)
        data = cursor.fetchall()
        record = {}
        for obj in data:
            record = {}
            record['time'] = obj[0]
            record['time_id'] = obj[1]
            record['high'] = obj[2]
        db.commit()
        return record
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


def get_last_zhisuntime(symbol, time_type, time_id, test_sun):
    try:
        db = get_db()
        cursor = db.cursor()
        sqlo = """select from_unixtime(time_id,'%Y-%m-%d %H:%i:%s') as time,time_id,high,open,low,close
                    from kline where symbol='{}' and time_type='{}' and low<{} and time_id>{} order by time_id limit 1;"""
        sql = sqlo.format(symbol, time_type, test_sun, time_id)

        cursor.execute(sql)
        data = cursor.fetchall()
        record = {}
        for obj in data:
            record = {}
            record['time'] = obj[0]
            record['time_id'] = obj[1]
            record['low'] = obj[2]
        db.commit()
        return record
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


def test():
    orders = get_order_fromdb()
    for obj in orders:
        id = obj['id']
        symbol = obj['symbol']
        time_type = '60min'
        size = 24
        time_id = obj['create_time']
        test_sun = obj['test_sun']
        test_yin = obj['test_yin']

        produce_info_job.produce_symbol_kline(symbol, time_type, size)
        zhiyin = get_last_zhiyintime(symbol, time_type, time_id, test_yin)
        zhisun = get_last_zhisuntime(symbol, time_type, time_id, test_sun)

        print("{},zhiyin={},zhisun={}".format(symbol, zhiyin, zhisun))

        if (zhiyin and zhisun):
            yin_time = zhiyin['time_id']
            sun_time = zhisun['time_id']
            if (yin_time < sun_time):
                update_order(id, symbol, result_type='yin', result_time=zhiyin['time_id'])
            elif (yin_time > sun_time):
                update_order(id, symbol, result_type='sun', result_time=zhisun['time_id'])
            else:
                # 同一个小时出现当作止损人工分析
                update_order(id, symbol, result_type='sun', result_time=zhisun['time_id'])
        elif (zhiyin):
            update_order(id, symbol, result_type='yin', result_time=zhiyin['time_id'])
        elif (zhisun):
            update_order(id, symbol, result_type='sun', result_time=zhisun['time_id'])
        else:
            print("no")


# print(get_last_zhisuntime('htusdt','60min',0,16.84))
# print(get_order_fromdb())

test()
