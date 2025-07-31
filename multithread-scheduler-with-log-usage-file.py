import threading
import logging
from datetime import datetime, timedelta
import time
import pandas as pd
import os
import psutil
import platform
import re 

# ---------------------- Logging Setup ----------------------
logging.basicConfig(
    filename='parallel_etl_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)

# ---------------------- Save System Info ----------------------
def save_system_info_to_file(filepath='system_info.txt'):
    try:
        with open(filepath, 'w') as f:
            f.write("===== System Information =====\n")
            f.write(f"Platform       : {platform.system()} {platform.release()}\n")
            f.write(f"CPU Cores      : {psutil.cpu_count(logical=False)} physical / {psutil.cpu_count(logical=True)} logical\n")
            f.write(f"Total Memory   : {round(psutil.virtual_memory().total / (1024 ** 3), 2)} GB\n")
            f.write(f"Available Mem  : {round(psutil.virtual_memory().available / (1024 ** 3), 2)} GB\n")
            f.write(f"CPU Usage per Core: {psutil.cpu_percent(interval=1, percpu=True)}%\n")
            f.write("==============================\n")
    except Exception as e:
        logging.error(f"Failed to save system info: {e}")

# ---------------------- ETL Functions ----------------------
def extractor(filepath):
    try:
        logging.info(f"Starting extraction from {filepath}")
        column = ['target', 'id', 'date', 'flag', 'user', 'text']
        df = pd.read_csv(filepath, encoding='latin1', names=column)
        logging.info(f"Extraction successful for {filepath}")
        return df
    except Exception as e:
        logging.error(f"Error extracting from {filepath}: {e}")
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

def transformation(df, source_name):
    try:
        logging.info(f"Starting transformation for {source_name}")
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
        logging.info(f"Transformation successful for {source_name}")
        return df
    except Exception as e:
        logging.error(f"Error transforming {source_name}: {e}")
        raise

def loading(df, output_path):
    try:
        logging.info(f"Starting loading to {output_path}")
        df.to_csv(output_path, index=False, encoding='utf-8')
        logging.info(f"Loading successful to {output_path}")
    except Exception as e:
        logging.error(f"Error loading to {output_path}: {e}")
        raise

def etl_pipeline(source_name, input_path, output_path):
    try:
        start_time = datetime.now()
        logging.info(f"ETL started for {source_name}")

        try:
            pid = os.getpid()
            core_id = psutil.Process(pid).cpu_num()
        except AttributeError:
            core_id = "N/A"

        # Save per-thread system info to its own file
        thread_sysinfo = (
            f"[{source_name}]\n"
            f"Thread Name   : {threading.current_thread().name}\n"
            f"PID           : {os.getpid()}\n"
            f"Core ID       : {core_id}\n"
            f"Memory Usage  : {round(psutil.Process().memory_info().rss / (1024 ** 2), 2)} MB\n"
            f"CPU/Core Usage: {psutil.cpu_percent(interval=1, percpu=True)}%\n"
            f"----------------------------\n"
        )

        with open(f"{source_name.lower()}_thread_info.txt", 'w') as f:
            f.write(thread_sysinfo)

        # ETL steps
        df = extractor(input_path)
        df = transformation(df, source_name)
        loading(df, output_path)

        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        logging.info(f"ETL finished for {source_name} in {elapsed:.2f} seconds")

    except Exception as e:
        logging.error(f"ETL error for {source_name}: {e}")

# ---------------------- Time Scheduler ----------------------
def time_threshold():
    now = datetime.now()
    threshold_time = now.replace(hour=1, minute=14, second=0, microsecond=0)
    if now > threshold_time:
        threshold_time += timedelta(days=1)
    return (threshold_time - now).total_seconds()

def run_etl_thread(source_name, input_path, output_path):
    try:
        wait_time = time_threshold()
        logging.info(f"{source_name} sleeping for {wait_time:.2f} seconds")
        time.sleep(wait_time)
        etl_pipeline(source_name, input_path, output_path)
    except Exception as e:
        logging.error(f"Thread crashed for {source_name}: {e}")

# ---------------------- Threaded Scheduler ----------------------
def threaded_scheduler():
    # Save general system info
    save_system_info_to_file()

    sources = [
        {"name": "Source1", "input": "dataset/source1.csv", "output": "dataset/source1_transformed_data.csv"},
        {"name": "Source2", "input": "dataset/source2.csv", "output": "dataset/source2_transformed_data.csv"}
    ]

    threads = []
    for src in sources:
        t = threading.Thread(
            target=run_etl_thread,
            name=src["name"],
            args=(src["name"], src["input"], src["output"])
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    logging.info("All ETL jobs completed.")

# ---------------------- Run Main ----------------------
if __name__ == "__main__":
    logging.info("Parallel ETL Scheduler started.")
    threaded_scheduler()
    logging.info("Parallel ETL Scheduler finished.")
