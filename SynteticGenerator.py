from datetime import datetime
from datetime import timedelta
import pandas as pd
import random

TAGS_FILE = 'feature_tags.csv'
OUTPUT = 'C:/Proficy Historian Data/sdata'
HOURS = 45
SECONDS = 9

if __name__ == "__main__":

    DATE_FORMAT = "%m/%d/%y %H:%M:%S"
    tags = pd.read_csv(TAGS_FILE)

    for tag in tags.tag:

        marker = datetime.now() - timedelta(hours=48)
        end = marker + timedelta(hours=HOURS)
        fd = open('{}/{}.csv'.format(OUTPUT,tag), 'w')

        fd.write('[Data]\n')
        fd.write('Tagname,TimeStamp,Value,DataQuality\n')

        while marker < end:
            s_marker = marker.strftime(DATE_FORMAT)
            fd.write('{},{},{},Good\n'.format(tag,s_marker,round(random.random(),ndigits=5)))
            marker = marker + timedelta(seconds=SECONDS)
        fd.close()



