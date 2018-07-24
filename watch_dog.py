import sched
import time
from Historian import Historian
from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import math
import configparser
import os

s = sched.scheduler(time.time, time.sleep)
model_name_list = ['model_C12', 'model_C3', 'model_iC4', 'model_iC5', 'model_nC4', 'model_nC5', 'model_sumC6']
h = Historian()
logger = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
DELTA = 10
HOME = ''

def read_pid():
    fd = open('{}/{}/va_pid'.format(HOME,'misc'),'r')
    str = fd.readline()
    id  = int(str)
    return id


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    config = configparser.ConfigParser()
    config.read('va_config.ini')
    global HOME
    HOME = config['DEFAULT']['HOME']

    #main_loop()

    s.enter(DELTA, 1, main_loop, ())
    s.run()


def main_loop():
    token = h.get_auth_token()
    end = datetime.utcnow()
    start = end - timedelta(minutes=DELTA)
    df = h.get_historian_data_batch({'Authorization': 'Bearer ' + token},
                                    model_name_list,
                                    start.strftime(DATE_FORMAT),
                                    end.strftime(DATE_FORMAT))
    ts = df.index.max()


    ts_end = pd.Timestamp(end).tz_localize('UTC')

    if df.empty == False:
        print(ts_end - ts)
        if (ts_end - ts) < timedelta(minutes=DELTA):
            logging.info('Seems that analyzer alive')
            return

    logging.info('Restart virtual analyzer')
    kill_python_task()
    start_process()



def kill_python_task():
    pid = read_pid()
    os.system('echo killing va')
    os.system('taskkill /F /PID {}'.format(pid))

def start_process():
    os.system('cd {}'.format(HOME))
    os.system('echo runing va')
    os.system('start {}/va.bat'.format(HOME))


main()
