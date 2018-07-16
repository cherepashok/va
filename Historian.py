import pandas as pd
import subprocess
import requests
import json
import logging
import configparser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Historian:
    AUTH      = 'curl -s -i -k -u {}:{} https://{}:{}/uaa/oauth/token -d grant_type=client_credentials'
    TAGS_FILE = '{}/misc/feature_tags.csv'
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
        self.AUTH      = self.AUTH.format(self.username,self.password,self.domain,self.port)
        self.TAGS_FILE = self.TAGS_FILE.format(self.HOME)
        self.url       = self.url.format(self.domain,self.port)


    # def get_next_second_data_stub(self):
    #     df = pd.DataFrame()
    #
    #     DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    #     tags = pd.read_csv(self.TAGS_FILE)
    #     now = datetime.datetime.now()
    #     s_now = now.strftime(DATE_FORMAT)
    #
    #     tag_values = {}
    #
    #     i = 0
    #     for tag in tags.tag:
    #         tag_values[tag] = random.random()
    #         i = i + 1
    #         #print([tag,',',s_now,',',1,'Good'])
    #
    #     df = pd.DataFrame.from_records(tag_values,index=[s_now])
    #     df.index = pd.DatetimeIndex(df.index)
    #
    #    return(df)

    def get_auth_token(self):
        command = self.AUTH.split()
        result = subprocess.run(command, stdout=subprocess.PIPE)
        s = (result.stdout).decode('utf-8')
        index = s.find('{')
        js = json.loads(s[index:])
        token = js['access_token']
        return token


    def test(self):
        token = self.get_auth_token()
        # self.create_tags(['tag2'],token)

        #
        # tag_list = ['TN.01.01.00.T1FQ.00.0.F0271.P','TN.01.01.00.T1P.00.0.P0216.P']
        #
        #
        # start = '2018-07-07T16:40:00.000Z'
        # end =   '2018-07-07T21:00:00.000Z'
        # df = self.get_historian_data({'Authorization':'Bearer '+token},tag_list,start,end)
        #
        # start = '2018-07-07T21:00:00.000Z'
        # end =   '2018-07-07T21:01:00.000Z'
        # n_df = self.get_historian_data({'Authorization':'Bearer '+token},tag_list,start,end)
        #
        # df = df.append(n_df, sort=True)
        # print(df.head())
        # print(df.tail())
        # print(df.index.min())
        # print(df.index.max())


        #
        auth_token = self.get_auth_token()
        urlr = 'https://91.202.222.202:8443/historian-rest-api/v1/datapoints/raw'

        params = {
            'tagNames': 'TN.01.01.00.T1P.00.0.P0216.P',
            'start': '2018-07-14T09:00:00',
            'end':   '2018-07-14T09:01:00',
            'count': '0',
            'direction':'0',
            'intervalMs' : '60000'
        }
        response = requests.get(urlr, params, headers={'Authorization':'Bearer '+token}, verify=False)
        json_data = json.loads(response.text)
        logging.debug(json_data)

        exit(0)

        df = pd.DataFrame()

        if(json_data['ErrorCode'] != 0):
            logging.debug(json_data['ErrorMessage'])
            return df


        logging.debug(json_data)

        df = pd.DataFrame()
        for tag_data in json_data['Data']:
            #print(tag_data['TagName'])
            values = {}
            for sample in tag_data['Samples']:
                time = pd.Timestamp(sample['TimeStamp'])
                value = sample['Value']
                values[time] = value
                #print(value)
                #value[time] = str(random.random())

            null = self.add_values_to(df, tag_data, values)

        df.index.name = 'Timestamp'
        df.index = df.index.ceil("1Min")
        df = df.resample("1Min").last().fillna(method="pad")
        return

    def get_historian_data_batch(self, auth_token, tag_list, start, end):
        df = pd.DataFrame()
        json_data = self.get_json(auth_token, end, start, tag_list)
        logging.debug(json_data)
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
        for tag in tag_list:
            json_data = self.get_json(auth_token, end, start, [tag])

            if 'Data' in json_data:
                for tag_data in json_data['Data']:
                    logging.debug(tag_data)
                    values = self.get_tag_series(tag_data)
                    df = self.add_values_to(df, tag_data, values)
            else:
                logging.debug(json_data)

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
        response = requests.get(self.url, params, headers=auth_token, verify=False)
        json_data = json.loads(response.text)
        return json_data


