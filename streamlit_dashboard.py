# import streamlit as st
# import pandas as pd
# import re

# LOG_FILE = "parallel_etl_log.txt"

# st.set_page_config(page_title="ETL Log Dashboard", layout="wide")
# st.title("📊 Parallel ETL Log Dashboard")

# @st.cache_data
# def load_logs():
#     log_entries = []

#     pattern = re.compile(r"^(?P<time>[\d\-:\s,]+)\s-\s(?P<level>\w+)\s-\s(?P<thread>[\w\d]+)\s-\s(?P<message>.+)$")
#     with open(LOG_FILE, 'r', encoding='utf-8') as f:
#         for line in f:
#             match = pattern.match(line)
#             if match:
#                 log_entries.append(match.groupdict())

#     return pd.DataFrame(log_entries)

# log_df = load_logs()

# if log_df.empty:
#     st.warning("No logs found.")
#     st.stop()

# # Convert to datetime for sorting/filtering
# log_df['time'] = pd.to_datetime(log_df['time'], errors='coerce')

# # --- Filters ---
# col1, col2, col3 = st.columns(3)

# levels = log_df['level'].unique()
# selected_levels = col1.multiselect("Log Level", options=levels, default=list(levels))

# threads = log_df['thread'].unique()
# selected_threads = col2.multiselect("Thread", options=threads, default=list(threads))

# date_range = col3.date_input("Date Range", [], help="Optional filter by date range")

# # --- Filter Data ---
# filtered_df = log_df[
#     (log_df['level'].isin(selected_levels)) &
#     (log_df['thread'].isin(selected_threads))
# ]

# if len(date_range) == 2:
#     start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
#     filtered_df = filtered_df[(filtered_df['time'] >= start) & (filtered_df['time'] <= end)]

# # --- Display Logs ---
# st.dataframe(filtered_df.sort_values(by="time", ascending=False), use_container_width=True)

# # --- Download Option ---
# st.download_button(
#     label="📥 Download Logs as CSV",
#     data=filtered_df.to_csv(index=False).encode('utf-8'),
#     file_name="etl_filtered_logs.csv",
#     mime="text/csv"
# )

# ------------------------------------------------------- #

# import streamlit as st
# import pandas as pd
# import re
# from datetime import datetime

# LOG_FILE = "parallel_etl_log.txt"

# st.set_page_config(page_title="ETL Log Dashboard", layout="wide")
# st.title("📊 Parallel ETL Log Dashboard")

# # Auto-refresh every 10 seconds
# st.experimental_rerun_interval = 10  # 10 seconds

# @st.cache_data(ttl=5)
# def load_logs():
#     log_entries = []

#     pattern = re.compile(r"^(?P<time>[\d\-:\s,]+)\s-\s(?P<level>\w+)\s-\s(?P<thread>[\w\d]+)\s-\s(?P<message>.+)$")
#     with open(LOG_FILE, 'r', encoding='utf-8') as f:
#         for line in f:
#             match = pattern.match(line)
#             if match:
#                 log_entries.append(match.groupdict())

#     df = pd.DataFrame(log_entries)
#     if not df.empty:
#         df['time'] = pd.to_datetime(df['time'], errors='coerce')
#     return df

# log_df = load_logs()

# if log_df.empty:
#     st.warning("No logs found.")
#     st.stop()

# # --- Filters ---
# col1, col2, col3 = st.columns(3)

# levels = log_df['level'].unique()
# selected_levels = col1.multiselect("Log Level", options=levels, default=list(levels))

# threads = log_df['thread'].unique()
# selected_threads = col2.multiselect("Thread", options=threads, default=list(threads))

# date_range = col3.date_input("Date Range", [], help="Optional filter by date range")

# # --- Filter Data ---
# filtered_df = log_df[
#     (log_df['level'].isin(selected_levels)) &
#     (log_df['thread'].isin(selected_threads))
# ]

# if len(date_range) == 2:
#     start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
#     filtered_df = filtered_df[(filtered_df['time'] >= start) & (filtered_df['time'] <= end)]

# # --- Chart: Log Volume Over Time ---
# st.subheader("📊 Log Volume Over Time")
# volume_df = filtered_df.copy()
# volume_df["minute"] = volume_df["time"].dt.floor("T")
# chart_df = volume_df.groupby(["minute", "thread"]).size().unstack(fill_value=0)

# st.line_chart(chart_df)

# # --- Panel: Logs per Source ---
# st.subheader("🧩 Logs by Source")
# for thread in selected_threads:
#     st.markdown(f"### 🧵 {thread}")
#     thread_df = filtered_df[filtered_df["thread"] == thread].sort_values(by="time", ascending=False)
#     st.dataframe(thread_df, use_container_width=True)

# # --- Download Option ---
# st.download_button(
#     label="📥 Download Filtered Logs as CSV",
#     data=filtered_df.to_csv(index=False).encode('utf-8'),
#     file_name="etl_filtered_logs.csv",
#     mime="text/csv"
# )

# -------------------------------------------------------- #

import streamlit as st
import pandas as pd
import re
from datetime import datetime
import psutil

LOG_FILE = "parallel_etl_log.txt"

st.set_page_config(page_title="ETL Log Dashboard", layout="wide")
st.title("📊 Parallel ETL Log Dashboard")

# Auto-refresh every 10 seconds
st.experimental_rerun_interval = 10  # in seconds

@st.cache_data(ttl=5)
def load_logs():
    log_entries = []

    pattern = re.compile(r"^(?P<time>[\d\-:\s,]+)\s-\s(?P<level>\w+)\s-\s(?P<thread>[\w\d]+)\s-\s(?P<message>.+)$")
    with open(LOG_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            match = pattern.match(line)
            if match:
                log_entries.append(match.groupdict())

    df = pd.DataFrame(log_entries)
    if not df.empty:
        df['time'] = pd.to_datetime(df['time'], errors='coerce')
    return df

log_df = load_logs()

if log_df.empty:
    st.warning("No logs found.")
    st.stop()

# --- Filters ---
col1, col2, col3 = st.columns(3)

levels = log_df['level'].unique()
selected_levels = col1.multiselect("Log Level", options=levels, default=list(levels))

threads = log_df['thread'].unique()
selected_threads = col2.multiselect("Thread", options=threads, default=list(threads))

date_range = col3.date_input("Date Range", [])

# --- Filter Data ---
filtered_df = log_df[
    (log_df['level'].isin(selected_levels)) &
    (log_df['thread'].isin(selected_threads))
]

if len(date_range) == 2:
    start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    filtered_df = filtered_df[(filtered_df['time'] >= start) & (filtered_df['time'] <= end)]

# --- Chart: Log Volume Over Time ---
st.subheader("📈 Log Volume Over Time")
volume_df = filtered_df.copy()
volume_df["minute"] = volume_df["time"].dt.floor("T")
chart_df = volume_df.groupby(["minute", "thread"]).size().unstack(fill_value=0)
st.line_chart(chart_df)

# --- System Info Panel ---
st.subheader("🧠 System Resource Usage")

cpu_usage = psutil.cpu_percent(interval=1)
mem = psutil.virtual_memory()
mem_usage = mem.percent
col_cpu, col_mem = st.columns(2)
col_cpu.metric("CPU Usage (%)", f"{cpu_usage} %")
col_mem.metric("Memory Usage (%)", f"{mem_usage} %")

# --- Highlighted Logs by Source ---
st.subheader("🧩 Logs by Source (Errors Highlighted 🔴)")
for thread in selected_threads:
    st.markdown(f"### 🧵 {thread}")
    thread_df = filtered_df[filtered_df["thread"] == thread].sort_values(by="time", ascending=False)

    def highlight_errors(row):
        return ['background-color: red; color: white' if row['level'] == 'ERROR' else '' for _ in row]

    styled = thread_df.style.apply(highlight_errors, axis=1)
    st.dataframe(styled, use_container_width=True)

# --- Download Option ---
st.download_button(
    label="📥 Download Filtered Logs as CSV",
    data=filtered_df.to_csv(index=False).encode('utf-8'),
    file_name="etl_filtered_logs.csv",
    mime="text/csv"
)

