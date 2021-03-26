import time
from huobi.client.market import MarketClient
from huobi.constant import *
from huobi.utils import *
import pymysql
import constant

def get_db():
    return pymysql.connect(host=constant.g_host, user=constant.g_user, password=constant.g_password, database=constant.g_database)

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


# 获取某个交易对的某种维度最近多少条kline数据入库
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


# 生成每日选中的交易对 每日追加进去
def produce_select_tickers(symbol, vol, rise_percent, size):
    db = get_db()
    cursor = db.cursor()
    sqlo = """select symbol,volume,rise_percent from tickers where symbol like '{}' and volume>{} and rise_percent > {} order by volume desc limit {}"""
    sql = sqlo.format(symbol, vol, rise_percent, size)
    print(sql)
    cursor.execute(sql)
    data = cursor.fetchall()
    day = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    db = get_db()
    for obj in data:
        try:
            cursor = db.cursor()
            sqlo = """INSERT INTO select_tickers(symbol,day,volume,rise_percent) VALUES ('{}','{}',{},{})"""
            sql = sqlo.format(obj[0], day, obj[1], obj[2])
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
        cursor.execute(sql)
        data = cursor.fetchall()
        db.commit()
        return data
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


# 查询当天选中的交易对
def get_select_tickers_fromdb():
    try:
        day = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        db = get_db()
        cursor = db.cursor()
        sqlo = """select symbol,volume,rise_percent from select_tickers where day='{}'"""
        sql = sqlo.format(day)
        print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        db.commit()
        return data
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


# 生成选中交易对的最近多少条的kline数据
def produce_all_klines(min1, min15, min60, day1):
    symbols = get_select_tickers_fromdb()
    for obj in symbols:
        symbol = obj[0]
        produce_symbol_kline(symbol, CandlestickInterval.MIN1, min1)
        produce_symbol_kline(symbol, CandlestickInterval.MIN15, min15)
        produce_symbol_kline(symbol, CandlestickInterval.MIN60, min60)
        produce_symbol_kline(symbol, CandlestickInterval.DAY1, day1)


# 从数据库查询kline聚合信息 时间大于seconds秒之后的数据
def get_symbol_klineinfos_fromdb(symbol, time_type, seconds):
    try:
        db = get_db()
        cursor = db.cursor()
        sqlo = """select max(rise_percent) as max_rise_percent,min(rise_percent) as min_rise_percent,
                         max(shake_percent) as max_shake_percent,min(shake_percent) as min_shake_percent,
                         avg(rise_percent) as avg_rise_percent,avg(shake_percent) as avg_shake_percent
                  from kline where symbol = '{}' and time_type='{}' and time_id > (UNIX_TIMESTAMP()-{} )"""
        sql = sqlo.format(symbol, time_type, seconds)
        print(sql)
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
    symbols = get_select_tickers_fromdb()
    data = {}
    times = {}

    db = get_db()
    cursor = db.cursor()
    cursor.execute("truncate table symbol_klines")
    db.commit()

    for obj in symbols:
        symbol = obj[0]
        # data[CandlestickInterval.MIN1] = get_symbol_klineinfos_fromdb(symbol, CandlestickInterval.MIN1, min1)
        # times[CandlestickInterval.MIN1] = min1;
        data[CandlestickInterval.MIN15] = get_symbol_klineinfos_fromdb(symbol, CandlestickInterval.MIN15, min15)
        times[CandlestickInterval.MIN15] = min15;
        data[CandlestickInterval.MIN60] = get_symbol_klineinfos_fromdb(symbol, CandlestickInterval.MIN60, min60)
        times[CandlestickInterval.MIN60] = min60;
        # data[CandlestickInterval.DAY1] = get_symbol_klineinfos_fromdb(symbol, CandlestickInterval.MIN1, day1)
        # times[CandlestickInterval.DAY1] = day1;

        print(symbol)

        for objo in data:
            objone = data[objo]
            print(objo)
            print(objone)
            time = times[objo]
            print(time)

            if objone:
                try:
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
                  from symbol_klines where create_time > (now()-600) and time_type='{}' order by avg_rise_percent desc , max_rise_percent desc"""
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

def testYinOrSun():
    orders = get_order_fromdb()
    for obj in orders:
        id = obj['id']
        symbol = obj['symbol']
        time_type = '60min'
        size = 24
        time_id = obj['create_time']
        test_sun = obj['test_sun']
        test_yin = obj['test_yin']

        produce_symbol_kline(symbol, time_type, size)
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
