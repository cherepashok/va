import pandas as pd
import subprocess
import requests
import json
import logging
import configparser
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.exceptions import InsecureRequestWarning

class Historian:
    #TAGS_FILE = '{}/misc/feature_tags.csv'
    url       =  'https://{}:{}/historian-rest-api/v1/datapoints/raw'
    username  = 'admin'
    password  = 'root2018'
    domain    = '91.202.222.202'
    port      = '8443'
    HOME      = ''

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('va_config.ini')
        self.HOME      = config['DEFAULT']['HOME']
        self.username  = config['HISTORIAN']['username']
        self.password  = config['HISTORIAN']['password']
        self.domain    = config['HISTORIAN']['domain']
        self.port      = config['HISTORIAN']['port']
        #self.TAGS_FILE = self.TAGS_FILE.format(self.HOME)
        self.url       = self.url.format(self.domain,self.port)

    def get_auth_token(self):
        auth_url = 'https://{}:{}'.format(self.domain,self.port)
        auth_url = '/'.join([auth_url, 'uaa/oauth/token'])

        auth = HTTPBasicAuth(self.username, self.password)
        data = {'grant_type': 'client_credentials'}
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        r = requests.post(auth_url, data=data, auth=auth, verify=False)
        token = r.json()['access_token']
        return token

    def get_historian_data_batch(self, auth_token, tag_list, start, end):
        df = pd.DataFrame()
        json_data = self.get_json(auth_token, end, start, tag_list)
        #logging.debug(json_data)
        # try:
        if 'Data' in json_data:
            for tag_data in json_data['Data']:
                values = self.get_tag_series(tag_data)
                df = self.add_values_to(df, tag_data, values)
            if df.empty != True:
                df = self.purge(df)
        else:
            logging.debug(json_data)
        # except Exception as e:
        #     logging.error(e)
        #     logging.error(df.head())
        #     logging.error(json_data)
        return df

    def get_historian_data(self, auth_token, tag_list, start, end):
        df = pd.DataFrame()
        batch = []

        for tag in tag_list:
            #logging.info('Loading %s data',tag)
            batch.append(tag)
            # if tag != tag_list[-1] and len(batch) < batch_size:
            #     continue
            json_data = self.get_json(auth_token, end, start, batch)

            if 'Data' in json_data:
                for tag_data in json_data['Data']:
                    #logging.debug(tag_data)
                    values = self.get_tag_series(tag_data)
                    df = self.add_values_to(df, tag_data, values)
            else:
                logging.debug(json_data)

            batch.clear()

        if df.empty != True:
            df = self.purge(df)
        return df


    def add_values_to(self, df, tag_data, values):
        d = pd.DataFrame(pd.Series(values), columns=[tag_data['TagName']])
        df = pd.concat([df, d], axis=1, sort=True, join='outer')
        return df


    def purge(self, df):
        df.index.name = 'Timestamp'
        df.index = df.index.ceil("1Min")
        df = df.resample("1Min").last().fillna(method="pad")
        return df

    def get_tag_series(self, tag_data):
        values = {}
        #print(tag_data)
        #print(tag_data['Samples'])

        for sample in tag_data['Samples']:
            time = pd.Timestamp(sample['TimeStamp'])
            value = sample['Value']
            values[time] = value
            # print(value)
            # value[time] = str(random.random())
        return values

    def get_json(self, auth_token, end, start, tag_list):
        tags_str = ''
        for tag in tag_list:
            tags_str = tags_str + tag + ';'
        params = {
            'tagNames': tags_str,
            'start': start,
            'end': end,
            'count': '0',
            'direction': '0',
            #'intervalMs': '60000'
        }
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        response = requests.get(self.url, params, headers=auth_token, verify=False)
        json_data = json.loads(response.text)
        return json_data


