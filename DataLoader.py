import pandas as pd
from datetime import datetime
from Historian import Historian
from datetime import timedelta
import logging


class CircularDataFrame:
    df = pd.DataFrame()
    tag_list = []
    h = Historian()
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
    hours = 5
    # zulu_shift = 3
    end = datetime.now()

    def __init__(self, tag_list):
        self.tag_list = tag_list
        self.token = self.h.get_auth_token()
        self.load_data()

    def load_data(self):
        start = datetime.utcnow() - timedelta(hours=(self.hours))
        logging.debug('Loading data starting at - ' + str(start))

        self.end = (start + timedelta(hours=self.hours))
        # self.end.minute
        logging.debug('Loading data ending at - ' + str(self.end))

        self.df = self.h.get_historian_data({'Authorization': 'Bearer ' + self.token},
                                            self.tag_list,
                                            start.strftime(self.DATE_FORMAT),
                                            self.end.strftime(self.DATE_FORMAT))

    def add_interval_data(self):
        self.df = self.df.drop([self.df.index.min()])

        start = self.end
        self.end = datetime.utcnow()  # - timedelta(hours = (self.zulu_shift))

        n_df = self.h.get_historian_data_batch({'Authorization': 'Bearer ' + self.token},
                                               self.tag_list,
                                               start.strftime(self.DATE_FORMAT),
                                               self.end.strftime(self.DATE_FORMAT))

        'If we got empty n_df will duplicate it from last raw'
        if True == n_df.empty:
            last_raw = self.df[self.df.index.max():]
            last_raw.index = last_raw.index + timedelta(minutes=1)
            n_df = last_raw

        self.df = self.df.append(n_df, sort=True)
        self.df = self.h.purge(self.df)

    def get_data_frame(self):
        return self.df
