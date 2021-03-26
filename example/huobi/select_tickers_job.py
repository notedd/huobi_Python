import tools

def job():
    tools.produce_last24_tickers()
    tools.produce_select_tickers('%usdt', 10000000, 0.01, 20)

job()