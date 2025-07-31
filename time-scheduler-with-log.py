import logging
from datetime import datetime, timedelta
import time
import pandas as pd
import re

# ---------------------- Setup Logging ----------------------
logging.basicConfig(
    filename='etl_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------- ETL Functions ----------------------
def extractor():
    try:
        logging.info("Starting data extraction.")
        column = ['target', 'id', 'date', 'flag', 'user', 'text']
        df = pd.read_csv('dataset/tweets.csv', encoding='latin1', names=column)
        logging.info("Data extraction successful.")
        return df
    except Exception as e:
        logging.error(f"Error in extractor: {e}")
        raise

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
    try:
        logging.info("Starting data transformation.")
        df['sentiment'] = df['target'].map({0: 'negative', 2: 'neutral', 4: 'positive'})
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
        logging.info("Data transformation successful.")
        return df
    except Exception as e:
        logging.error(f"Error in transformation: {e}")
        raise

def loading(df):
    try:
        logging.info("Starting data loading.")
        df.to_csv('dataset/tweets_new_data.csv', index=False, encoding='utf-8')
        logging.info("Data loading successful.")
    except Exception as e:
        logging.error(f"Error in loading: {e}")
        raise

def etl_pipeline():
    try:
        logging.info("ETL pipeline started.")
        df = extractor()
        df = transformation(df)
        loading(df)
        logging.info("ETL pipeline completed successfully.")
    except Exception as e:
        logging.critical(f"ETL pipeline failed: {e}")

# ---------------------- Time Scheduler ----------------------
def time_threshold():
    now = datetime.now()
    threshold_time = now.replace(hour=00, minute=43, second=0, microsecond=0)
    if now > threshold_time:
        threshold_time += timedelta(days=1)
    return (threshold_time - now).total_seconds()

def scheduler():
    wait_seconds = time_threshold()
    logging.info(f"Sleeping for {wait_seconds:.2f} seconds until ETL run at 21:00.")
    time.sleep(wait_seconds)
    etl_pipeline()

# ---------------------- Run Scheduler ----------------------
if __name__ == "__main__":
    logging.info("ETL Scheduler initialized.")
    scheduler()
    logging.info("ETL Scheduler finished.")
