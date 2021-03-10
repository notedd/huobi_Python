import time
from apscheduler.schedulers.blocking import BlockingScheduler

def job(text):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print('{} --- {}'.format(text, t))

scheduler = BlockingScheduler()
# 在每天22点，每隔 1分钟 运行一次 job 方法
scheduler.add_job(job, 'cron', hour='14', minute='*/1', args=['job1'])
# 在每天22和23点的25分，运行一次 job 方法
scheduler.add_job(job, 'cron', hour='22-23', minute='25', args=['job2'])
scheduler.start()




