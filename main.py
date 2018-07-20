from DataLoader import CircularDataFrame
from sklearn.externals import joblib
from Historian import Historian
from datetime import datetime

import pandas as pd
import time
import sched
import logging
import configparser

HOME       = ''
OUTPUT     = ''
MODEL_NAME = '{}/models_vf'
TAGS_FILE  = '{}/misc/feature_tags.csv'
model_name_list = ['model_C12', 'model_C3', 'model_iC4', 'model_iC5', 'model_nC4', 'model_nC5', 'model_sumC6']

s = sched.scheduler(time.time, time.sleep)

def main():
    #logging.basicConfig(filename='log/va.log',level=logging.info(),format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',filemode='w')
    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger = logging.getLogger(__name__)

    generate_targets_tags(model_name_list)

    config = configparser.ConfigParser()
    config.read('va_config.ini')
    global HOME
    HOME = config['DEFAULT']['HOME']
    global OUTPUT
    OUTPUT = config['DEFAULT']['OUTPUT']

    tags = pd.read_csv(TAGS_FILE.format(HOME))
    h = Historian()
    logger.info('Initial data collection')
    dc = CircularDataFrame(tags.tag)
    #logger.info('Add new data')
    #dc.add_interval_data()
    #df = dc.get_data_frame()
    #prediction(df)

    s.enter(60, 1, main_loop, (s, dc, h,))
    s.run()


def main_loop(sc, dc, h):
    start = time.time()
    logging.info('Add new data')
    dc.add_interval_data()
    df = dc.get_data_frame()
    logging.info('Start prediction')
    prediction(df)
    end = time.time()
    logging.info('Duration - '+str(end - start))
    #gc.collect()
    s.enter(60, 1, main_loop, (sc,dc,h,))


def chrom_feature_extract(data,
                          trend_lags=[10, 30, 50, 70]):
    """Feature Extraction for chrom data.
    Trends and lagged previous values.
    Returns new DataFrame"""
    result = data.copy()
    #  some shit comprehension to avoid calulating rolling_mean twice
    result = result.join([pd.DataFrame(data=(rolled_data.shift(1)).values,
                                       index=data.index,
                                       columns=data.columns + '_trend_%d' % trend) \
                          for rolled_data, trend in map(lambda trend: (data.rolling(trend).mean(), trend),
                                                        trend_lags)])
    return result


def prediction(tags_data):
    # Clean data
    #tags_data = tags_data[tags_data.columns[tags_data.nunique() > 10]]

    #NA_THRESHOLD = 0.5
    #tags_data = tags_data[
    #    tags_data.columns[tags_data.count() > NA_THRESHOLD * tags_data.shape[0]]]

    DEFAULT_LAGS = [2, 15, 30, 60, 90, 120]
    logging.info('New feature calculation')
    start = time.time()

    tags_data_v = chrom_feature_extract(tags_data, trend_lags=DEFAULT_LAGS)
    end = time.time()

    logging.info('Exttraction of new feature - ' + str(end-start))

    logging.info('Load model')
    logging.debug(tags_data_v.index.min())
    logging.debug(tags_data_v.index.max())

    # Some magic to prepare values to predictor
    val = tags_data_v.loc[tags_data_v.index.max():]
    val = val.values.reshape(1,-1)

    #logging.info(tags_data.head())
    #logging.DEBUG(tags_data_v.head())

    try:
        for model in model_name_list:
            loaded_model = joblib.load(MODEL_NAME.format(HOME)+'/'+model)
            model_result = loaded_model.predict(val)
            output_model_result(model,model_result)
    except:
        logging.DEBUG(tags_data_v.head)
        #logging.ERROR(tags_data_v.head())

        pass


def output_model_result(tag,value):
    logging.info('Writing {} results to file'.format(tag))

    DATE_FORMAT = "%m/%d/%y %H:%M:%S"
    str_tstamp = datetime.now().strftime(DATE_FORMAT)

    fd = open('{}/{}.csv'.format(OUTPUT, tag), 'w')
    fd.write('[Data]\n')
    fd.write('Tagname,TimeStamp,Value,DataQuality\n')
    fd.write('{},{},{},Good\r\n'.format(tag, str_tstamp, value[0]))
    fd.close()

def generate_targets_tags(tag_list):
    header = '[Tags]\n'+'Tagname,Description,DataType\n'
    body = ''
    for tag in tag_list:
        fd = open('{}/{}.csv'.format(OUTPUT, tag), 'w')
        fd.write(header)
        body = body + tag + ',predicted tag, DoubleFloat\n'
        fd.write(body)
        fd.close()

if __name__ == "__main__":
    main()

