import datetime
import time
from datetime import timedelta
import pandas as pd
import re

def extractor():
    column = ['target','id','date','flag','user','text']
    df = pd.read_csv('dataset/tweets.csv', encoding='latin1', names=column)
    return df

def remove_emojis(text):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def transformation(df):
    df['sentiment'] = df['target'].map({0:'negative', 2:'neutral', 4:'positive'})
    df['text'] = df['text'].str.lower()
    df['text'] = df['text'].apply(lambda x: re.sub(r'https?://\S+|www\.\S+', '', x))  # Remove URLs
    df['text'] = df['text'].apply(lambda x: re.sub(r'@\w+', '', x))  # Remove mentions
    df['text'] = df['text'].apply(lambda x: re.sub(r'#\w+', '', x))  # Remove hashtags
    df['text'] = df['text'].apply(lambda x: re.sub(r'\d+', '', x))  # Remove digits
    df['text'] = df['text'].apply(lambda x: re.sub(r'[^\w\s]', '', x))  # Remove punctuation
    df['text'] = df['text'].str.strip()  # Remove leading and trailing whitespace
    df['text'] = df['text'].apply(lambda x: re.sub(r'\s+', ' ', x))  # Replace multiple spaces with a single space
    df['text'] = df['text'].apply(remove_emojis)
    df = df[['target', 'text']]
    return df

def loading(df):
    df.to_csv('dataset/tweets_new.csv', index=False, encoding='utf-8')

def etl_pipeline():
    df = extractor()
    df = transformation(df)
    loading(df)
    print("ETL Done at", datetime.datetime.now())

def time_threshold():
    now = datetime.datetime.now()
    print(now)
    threshold_time = now.replace(hour=00, minute=36, second=0, microsecond=0)
    print(threshold_time)
    if now > threshold_time:
        threshold_time += timedelta(days=1)
    return (threshold_time - now).total_seconds()

def scheduler():
    # while True:
    momemnt = time_threshold()
    time.sleep(momemnt)
    etl_pipeline()

scheduler()
