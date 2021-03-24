import produce_info_job

def job15():
    produce_info_job.produce_all_klines(16,3,2,2)
    produce_info_job.produce_symbol_klines(0, 10800, 10800, 0)

job15()