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

s = sched.scheduler(time.time, time.sleep)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():

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
    logger.info('Add new data')
    dc.add_interval_data()
    #df = dc.get_data_frame()
    #prediction(df)

    s.enter(60, 1, main_loop, (s, dc, h,))
    s.run()


def main_loop(sc, dc, h):
    start = time.time()
    logger.info('Add new data')
    dc.add_interval_data()
    df = dc.get_data_frame()
    logger.info('Start prediction')
    prediction(df)
    end = time.time()
    logger.info('Duration - '+str(end - start))
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
    logger.info('New feature calculation')
    start = time.time()

    tags_data_v = chrom_feature_extract(tags_data, trend_lags=DEFAULT_LAGS)
    end = time.time()

    logger.info('Exttraction of new feature - ' + str(end-start))

    logger.info('Load model')
    model_name_list = ['model_C12','model_C3','model_iC4','model_iC5','model_nC4','model_nC5','model_sumC6']
    logger.debug(tags_data_v.index.min())
    logger.debug(tags_data_v.index.max())

    # Some magic to prepare values to predictor
    val = tags_data_v.loc[tags_data_v.index.max():]
    val = val.values.reshape(1,-1)

    for model in model_name_list:
        loaded_model = joblib.load(MODEL_NAME.format(HOME)+'/'+model)
        model_result = loaded_model.predict(val)
        output_model_result(model,model_result)


def output_model_result(tag,value):
    logger.info('Writing {} results to file'.format(tag))

    DATE_FORMAT = "%m/%d/%y %H:%M:%S"
    str_tstamp = datetime.now().strftime(DATE_FORMAT)

    fd = open('{}/{}.csv'.format(OUTPUT, tag), 'w')
    fd.write('[Data]\n')
    fd.write('Tagname,TimeStamp,Value,DataQuality\n')
    fd.write('{},{},{},Good\r\n'.format(tag, str_tstamp, value[0]))
    fd.close()

if __name__ == "__main__":
    main()

