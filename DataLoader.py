import pandas as pd
from datetime import datetime
from Historian import Historian
from datetime import timedelta
import logging

class CircularDataFrame:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    df = pd.DataFrame()
    tag_list = []
    h = Historian()
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
    hours = 5
    # To convert to zulu time
    zulu_shift = 4
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




# class CircularDataContainer:
#     tags_ts = {}
#
#     m_interval = 300
#     def __init__(self,minutes_interval):
#         self.m_interval = minutes_interval
#
#     def add_elem(self,tag,value):
#         if tag not in self.tags_ts:
#             self.tags_ts[tag] = collections.deque(maxlen=self.m_interval)
#         self.tags_ts[tag].append(value)
#
#
#     def load_from_csv(self, path, tagname):
#         print("Reading - ",tagname)
#         tag_data = pd.read_csv("{}.csv".format(os.path.join(path, tagname)),names=['tag','time','value','quality'])
#         for row in tag_data.values:
#             tag  = row[0]
#             time = row[1]
#             value = row[2] + 1
#             self.add_elem(tag,(time,value))
#
#     def get_df(self):
#
#         df = pd.DataFrame.from_dict(self.tags_ts , orient='columns')
#         return df


# class HeapDataContainer:
#     '''Class loads & stores data from json , csv sources'''
#
#     tagsTS = {}
#
#     DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
#
#     def readCSV(self, path, tagname):
#         print("Reading - ",tagname)
#
#
#         #s = datetime.now()
#         tag_data = pd.read_csv("{}.csv".format(os.path.join(path, tagname)))
#         #e = datetime.now()
#
#         #print(e-s)
#
#
#         #ss = datetime.now()
#         for row in tag_data.values:
#             tag  = row[0]
#             time = row[1]
#             value = row[2] + 1
#             self.addElem(tag,time,value)
#         #ee = datetime.now()
#         # print(ee-ss)
#
#     def getDF(self):
#         self.normalize()
#         td = pd.DataFrame.from_dict(self.tagsTS,orient="columns")
#         return td
#
#
#     def addElem(self,tag,time,value):
#         if tag not in self.tagsTS:
#             self.tagsTS[tag] = []
#         heappush(self.tagsTS[tag], (time,value))
#
#     def pushElem(self,tag,time,value):
#         heapreplace(self.tagsTS[tag],(time,value))
#
#     def heapsort(self, h):
#         return [heappop(h) for i in range(len(h))]
#
#     def normalize(self):
#         for tag in self.tagsTS:
#             self.tagsTS[tag] = self.heapsort(self.tagsTS[tag])
#
#
#     def print(self):
#         self.normalize()
#         for tag in self.tagsTS:
#             for elem in self.tagsTS[tag]:
#                 print(tag,elem[0],elem[1])
