import configparser

import pandas as pd
import os
import numpy as np
from Historian import Historian
import sched, time
from datetime import datetime
from datetime import timedelta


from sklearn.externals import joblib
MODEL_NAME = '/Users/dumbfly/PycharmProjects/himtech/models_vf'

TAGS_DIR = '/Users/dumbfly/PycharmProjects/himtech/data'
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def load_data(tag_list):
    tag_list_data = {}
    for i, tag in enumerate(tag_list):
        #         print(i, len(tag_list))
        try:
            tag_data = pd.read_csv("{}.csv".format(os.path.join(TAGS_DIR, tag)))

            #print(tag_data)

            #tag_data['timestamp'] = pd.to_datetime(tag_data['timestamp'], format=DATE_FORMAT)
            #tag_data = tag_data[(tag_data.timestamp >= START_END_TIME[0]) & (tag_data.timestamp <= START_END_TIME[1])]
            tag_data.set_index("timestamp", inplace=True)
            #print(tag_data)
            tag_data = tag_data[tag_data.quality == "Good"]
            tag_list_data[tag] = tag_data.value
        except IOError as e:
            continue

    tags_data = pd.DataFrame.from_dict(tag_list_data, orient="columns")
    tags_data.index = pd.DatetimeIndex(tags_data.index)
    return tags_data



df = load_data(['TN.01','TN.02','TN.03'])

ndf = pd.DataFrame({'TN.01':[1],'TN.02':[2],'TN.03':[3],'TN.04':[0]}, index = ['2016-08-08 15:40:00'])
ndf.index = pd.DatetimeIndex(ndf.index)

df = df.append(ndf,sort=True)

values = df.loc[df.index.max():]
print(df)
print(values.values.reshape(1,-1))


print('load model')
model_name_list = ['model_C12', 'model_C3', 'model_iC4', 'model_iC5', 'model_nC4', 'model_nC5', 'model_sumC6']

values = [np.NaN for i in range(1,7*199 + 1)]

print(values)

for model in model_name_list:
    print(model)
    loaded_model = joblib.load(MODEL_NAME + '/' + model)


    l = np.array(values).reshape(1,-1)
    #print(l)
    print(loaded_model.predict(l))

#print()

#print(df['2016-08-08 15:41:00'])
#print(df)
#df = df.drop([df.index.min()])
#df.drop(df.index.min().strftime(DATE_FORMAT))
print(df.index.max())

ts = df.index.max()
end = datetime.now()
print((end - ts) > timedelta(minutes = 5))
pass
# l = {'a':'123','b':'456'}
# print(l)
# print(l.items())

# historian = Historian()
# historian.test()


# s = sched.scheduler(time.time, time.sleep)
# def do_something(sc):
#     print("Doing stuff...")
#     # do your stuff
#     s.enter(60, 1, do_something, (sc,))
#
# s.enter(60, 1, do_something, (s,))
# s.run()


# def output_model_result(tag,value):
#
#     DATE_FORMAT = "%Y/%m/%d %H:%M:%S"
#     str_tstamp = datetime.now().strftime(DATE_FORMAT)
#
#     fd = open('{}/{}.csv'.format(OUTPUT, tag), 'w')
#     fd.write('[Data]\r\n')
#     fd.write('Tagname,TimeStamp,Value,DataQuality\r\n')
#     fd.write('{},{},{},Good\r\n'.format(tag, str_tstamp, value[0]))
#     fd.close()
#
# OUTPUT = '/Users/dumbfly/PycharmProjects/himtech/output'
# output_model_result('kvakva',[0.5])


# end = datetime.now()
# print(end)
# end = datetime(end.year,end.month,end.day,end.hour,end.minute + 1)
# print(end)

#print(df)
print('duppl - ' + str(len(set(df.index.duplicated()))))
#print(df.resample('1Min').last())