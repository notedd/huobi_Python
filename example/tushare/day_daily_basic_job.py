import tools
import datetime

def job():
    today = datetime.datetime.now()
    yestoday = today + datetime.timedelta(days=-1)
    day = yestoday.strftime('%Y%m%d')
    print(day)
    tools.produce_daily_basic_all(trade_date=day, size=100)

job()
