import tools

def job():

    tools.produce_symbol_klines(0, 43200, 43200, 0)
    tools.produce_select_symbols()

job()