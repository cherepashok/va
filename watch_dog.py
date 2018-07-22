import sched
import time
from Historian import Historian
from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import math

s = sched.scheduler(time.time, time.sleep)
model_name_list = ['model_C12', 'model_C3', 'model_iC4', 'model_iC5', 'model_nC4', 'model_nC5', 'model_sumC6']
h = Historian()
logger = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
DELTA = 20


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main_loop()

    # s.enter(300, 1, main_loop, (s, dc, h,))
    # s.run()


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
    pass


def start_process():
    pass


main()
