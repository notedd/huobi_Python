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
        # print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        for obj in data:
            record = {}
            record['symbol'] = obj[0]
            record['day'] = obj[1]
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
def get_symbol_klines_fromdb(time_type):
    try:
        symbol_klines = {}
        db = get_db()
        cursor = db.cursor()
        sqlo = """select symbol,create_time,time_type,times,max_rise_percent,max_shake_percent,
                              min_rise_percent,min_shake_percent,avg_rise_percent,avg_shake_percent
                  from symbol_klines where create_time > (now()-60000) and time_type='{}' order by avg_rise_percent desc , max_rise_percent desc"""
        sql = sqlo.format(time_type)
        # print(sql)
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


def produce_select_symbols():
    select_symbols_old = get_select_symbols_fromdb()
    symbol_klines_db_15min = get_symbol_klines_fromdb(time_type='15min')
    symbol_klines_db_60min = get_symbol_klines_fromdb(time_type='60min')

    # 更新或者删除已有的
    for symbol in select_symbols_old:
        try:
            db = get_db()
            print("循环处理已有交易对{}".format(symbol))
            if (symbol_klines_db_15min.get(symbol)):
                record = symbol_klines_db_15min.get(symbol)
                record60 = symbol_klines_db_60min.get(symbol)
                zhiying_scale = record['max_rise_percent']
                zhisun_scale = record60['max_shake_percent']
                day = select_symbols_old.get(symbol)['day']
                print("需要更新或者删除交易对 symbol={} zhiying={} zhisun={} day={}".format(symbol, zhiying_scale, zhisun_scale,
                                                                                 day))

                if (zhiying_scale < 0.01 or zhisun_scale > 0.6 or zhisun_scale < zhiying_scale):
                    print("删除交易对{}".format(symbol))
                    # 删除数据库里面的也删除
                    cursor = db.cursor()
                    sqlo = """delete from select_symbols where symbol='{}' and day='{}'"""
                    sql = sqlo.format(symbol, day)
                    print(sql)
                    cursor.execute(sql)
                    db.commit

                    continue

                # 更新
                print("更新交易对{}".format(symbol))
                cursor = db.cursor()
                sqlo = """update select_symbols set zhiying_scale={},zhisun_scale={} where symbol='{}' and day='{}'"""
                sql = sqlo.format(zhiying_scale, zhisun_scale, symbol, day)
                print(sql)
                cursor.execute(sql)
                db.commit()

                # 删除已经存在的
                symbol_klines_db_15min.pop(symbol)
                symbol_klines_db_60min.pop(symbol)

        except Exception as e:
            print(e)
            db.rollback()
        finally:
            db.close()

    # 判断目前的数量是否达到上线
    count = len(select_symbols_old)
    if (count > 5):
        print("超过过当日最大数退出")
        return

    # 加入新的交易对
    left = 5 - count
    day = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    for symbol in symbol_klines_db_15min:
        try:
            db = get_db()
            print("循环处理新的交易对{}".format(symbol))
            record = symbol_klines_db_15min.get(symbol)
            record60 = symbol_klines_db_60min.get(symbol)
            zhiying_scale = record['max_rise_percent']
            zhisun_scale = record60['max_shake_percent']
            print("需要插入的交易对 symbol={} zhiying={} zhisun={} day={}".format(symbol, zhiying_scale, zhisun_scale, day))

            if (zhiying_scale > 0.01 and zhisun_scale < 0.6 and zhisun_scale > zhiying_scale):
                left = left - 1
                if (left < 0):
                    return

                print("确定插入交易对{}".format(symbol))
                cursor = db.cursor()
                sqlo = """insert into select_symbols(symbol,day,zhiying_scale,zhisun_scale) values ('{}','{}',{},{})"""
                sql = sqlo.format(symbol, day, zhiying_scale, zhisun_scale)
                print(sql)
                cursor.execute(sql)
                db.commit()

        except Exception as e:
            print(e)
            db.rollback()
        finally:
            db.close()


produce_select_symbols()
# print(get_symbol_klines_fromdb())
