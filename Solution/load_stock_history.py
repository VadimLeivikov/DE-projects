import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column
import logs_writer 
import sys
import os
from dotenv import load_dotenv


load_dotenv("/home/seadata/Documents/Python projects/Final project/config.env")

path = os.getenv('PATH_FILES')
files = os.listdir(path)

##  Getting the data:
path_symbols = os.getenv('PATH_SYMBOLS')
symbols = pd.read_csv(path_symbols)

## Creating engine for connection to MySQL DB:
engine = create_engine(logs_writer.engine_conf, isolation_level="AUTOCOMMIT")

# engine = create_engine('mysql+pymysql://dev:dev@127.0.0.1:3306/stocks?charset=utf8mb4',isolation_level="AUTOCOMMIT")
conn = engine.connect()

## Gathering data into 'stock_rates' in MySQL DB:
stocks = {}
i = 0
for f in sorted(files):
    file_name = f[:-4]
    try:
        logs_writer.logs(1, file_name, path)
        stocks[f'{file_name}'] = pd.read_csv(os.path.join(path, f))
        logs_writer.logs(2, file_name, path)
    except FileNotFoundError as e:
        logs_writer.logs(6, file_name, path, sys.exc_info())

    try:
        df = stocks[f'{file_name}']
        company_name = symbols[symbols['Symbol']==file_name]['Security Name']
        df['Company_name'] = str(company_name.values).strip('[]\' ')
        logs_writer.logs(3, file_name, path)
    except:
        df = pd.DataFrame()
        logs_writer.logs(7, file_name, sys.exc_info())
        df['Company_name'] = 'N/A'

    df['Symbol'] = file_name
    df = df.reindex(['Symbol','Company_name', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'], axis=1)

    try:
        df.to_sql('stock_rates', con=conn, if_exists='append', index=False, chunksize = 500)
        logs_writer.logs(4, file_name, path)
    except:
        logs_writer.logs(8, file_name, sys.exc_info())

    i +=1
    print()
    if i > 20: break
logs_writer.logs(5, file_name, path)
