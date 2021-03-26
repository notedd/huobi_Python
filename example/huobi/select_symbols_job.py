import tools

def job():

    tools.produce_symbol_klines(0, 10800, 10800, 0)
    tools.produce_select_symbols()

job()