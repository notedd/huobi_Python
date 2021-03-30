import tools
import datetime

def job():
    today = datetime.datetime.now()
    yestoday = today + datetime.timedelta(days=-1)
    day = yestoday.strftime('%Y%m%d')
    print(day)
    tools.product_kline_info_all(freq='D', start_date=day, end_date=day, size=100)

job()
