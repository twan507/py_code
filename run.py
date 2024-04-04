import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import os
import datetime as dt
from datetime import datetime
import pandas as pd
import time

def run_notebook(path):
    # Đọc file notebook
    with open(path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)
    
    # Tạo một executor để chạy notebook
    ep = ExecutePreprocessor(timeout=600, kernel_name='python3')

    # Chạy notebook
    # Sử dụng đường dẫn của notebook để chạy các cells trong cùng thư mục đó
    notebook_dir = os.path.dirname(path)
    ep.preprocess(nb, {'metadata': {'path': notebook_dir}})

def get_current_time(start_time_am, end_time_am, start_time_pm, end_time_pm):
    if (dt.datetime.now()).weekday() <= 4:
        current_time = dt.datetime.now().time()
        if current_time < start_time_am: current_time = end_time_pm
        elif (current_time >= start_time_am) & (current_time < end_time_am): current_time = current_time
        elif (current_time >= end_time_am) & (current_time < start_time_pm): current_time = end_time_am
        elif (current_time >= start_time_pm) & (current_time < end_time_pm): current_time = current_time
        elif current_time >= end_time_pm: current_time = end_time_pm
        return current_time
    if (dt.datetime.now()).weekday() > 4:
        return end_time_pm

while True:
    try:
        notebook_path = 'd:\\t2m_invest\\py_code\\t2m_python.ipynb'
        run_notebook(notebook_path)

        current_time = get_current_time(dt.time(9, 00), dt.time(11, 30), dt.time(13, 00), dt.time(15, 00))
        date_series = pd.read_csv('../ami_data/VNINDEX.csv').iloc[-1]
        date_series['date'] = pd.to_datetime(date_series['date'].astype(str), format='%y%m%d')
        print(f"Cập nhật: {datetime.combine(date_series['date'].date(), current_time).strftime('%d/%m/%Y %H:%M:%S')}")

    except Exception as e:
        print(f"Error: {type(e).__name__}")
    time.sleep(30)