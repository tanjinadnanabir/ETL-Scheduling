
import datetime, timedelta
import time
import pandas as pd

def extractor():
    column = ['target','id','date','flag','user','text']

    df = pd.read_csv('tweets.csv', encoding='latin1',names=column)
    return df
def extractor():
    column = ['target','id','date','flag','user','text']

    df = pd.read_csv('tweets.csv', encoding='latin1',names=column)
    return df
def transformaion(df):
    df['sentiment'] = df['target'].map({0:'negative',2:'neutral',4:'positve'})
    return df
def loading(df):
    df.to_csv('new.csv')



def etl_pipeline():
    data = extractor()
    new_data = transformaion(data)
    loading()

def time_threshold():
    now = datetime.datetime.now()
    print("now",now)
    threshold_time = now.replace(hour=22, minute=37, second=0, microsecond=0)
    print("th",threshold_time)
    if now>threshold_time:
        threshold_time = threshold_time+ timedelta(1)
        print("thn",threshold_time)
    print("th",(threshold_time-now).total_seconds())
    return (threshold_time-now).total_seconds()

def scheduler():
    while True:
        moment = time_threshold()
        time.sleep(moment)
        etl_pipeline()
        print("done")
scheduler()