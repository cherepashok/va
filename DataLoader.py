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
    # To convert to zulu time
    zulu_shift = 3
    end = datetime.now()


    def __init__(self,tag_list):
        self.tag_list = tag_list
        self.load_data()

    def load_data(self):


        start = datetime.now() - timedelta(hours = (self.hours + self.zulu_shift))
        logging.debug('Loading data starting at - '+str(start))

        self.end = (start + timedelta(hours = self.hours))
        logging.debug('Loading data ending at - '+str(self.end))

        token = self.h.get_auth_token()
        self.df = self.h.get_historian_data({'Authorization': 'Bearer ' + token}, self.tag_list,
                                       start.strftime(self.DATE_FORMAT), self.end.strftime(self.DATE_FORMAT))


    def add_interval_data(self):
        self.df = self.df.drop([self.df.index.min()])

        start = self.end
        self.end = datetime.now() - timedelta(hours = (self.zulu_shift))

        logging.debug('Loading data starting at - '+str(start))
        logging.debug('Loading data ending   at - '+str(self.end))

        token = self.h.get_auth_token()
        n_df = self.h.get_historian_data_batch({'Authorization': 'Bearer ' + token}, self.tag_list,
                                       start.strftime(self.DATE_FORMAT), self.end.strftime(self.DATE_FORMAT))
        self.df = self.df.append(n_df, sort=True)
        logging.debug(self.df.index.min())
        logging.debug(self.df.index.max())

    def get_data_frame(self):
        return self.df
