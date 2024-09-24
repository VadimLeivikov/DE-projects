import logging
import io
from functools import wraps
import sys
import os
import toml
from dotenv import load_dotenv
load_dotenv("/home/seadata/Documents/Python projects/Final project/config.env")

## Creating engine for connection to MySQL DB:
engine_conf = os.getenv('ENGINE')


## Defining global variable:
process_name = str('')
log_stream = io.StringIO()

## Configuring the logging system to log to both console and a file with timestamps
config = toml.load("/home/seadata/Documents/Python projects/Final project/config.toml")

handlers_ = []
if "file" in config["handlers"]:
    file_handler = logging.FileHandler(config["handlers"]["file"]["filename"]) # Log to a file
    handlers_.append(file_handler)

if "stream" in config["handlers"]:
    stream_handler = logging.StreamHandler(log_stream) # Log to DB
    handlers_.append(stream_handler)

logging.basicConfig(
    level = config["logging"]["basicConfig"]["level"],
    format = config["logging"]["basicConfig"]["format"],  
    datefmt = config["logging"]["basicConfig"]["datefmt"],
    handlers = handlers_
    )

## Decorator for writing logs into 'ETL_PROCESS_LOGS' table:
def write_logs(log_:str):
    from sqlalchemy import create_engine, MetaData, Table, Column, insert, Integer, String, Text
    metadata = MetaData()
    engine = create_engine(engine_conf, isolation_level="AUTOCOMMIT")
    ETL_PROCESS_LOGS = Table(
        'ETL_PROCESS_LOGS', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('etl_process', String(255)),
        Column('log_time', String(50)),
        Column('log_level', String(50)),
        Column('log_desc', Text)
    )
    metadata.create_all(engine, tables=[ETL_PROCESS_LOGS])
    
    conn = engine.connect()
    timemark, levelname, _, log_message = log_.split(' - ', 3)
    insert_log_stmt = (insert(ETL_PROCESS_LOGS).values(etl_process ='stocks_data', log_time = timemark, log_level = levelname, log_desc = log_message))
    result = conn.execute(insert_log_stmt)


## Function decorator for method "get_details" of the Class "stock_details"
def log_get_details(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        class_name = type(args[0]).__name__
        logging.info(f"{class_name}: Entering function: {func.__name__} with args: '{args[0].symbol}' {args[1:]}")
        log_ = log_stream.getvalue()
        log_stream.truncate(0)
        log_stream.seek(0) 
        write_logs(log_)
        # print(log_)
        
        result = func(*args, **kwargs)

        if log_stream.getvalue().lower().rfind('error')>-1: 
            class_name = type(args[0]).__name__
            start_date = args[1]
            end_date = args[2]
            log_msg = f"{class_name}: Date interval ({start_date} : {end_date}) should be less than 30 days"
            logger.error(log_msg)
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)
        logging.info(f"{class_name}: Processing of the stock '{args[0].symbol}' succeeded. Exiting function: {func.__name__}.")
        log_ = log_stream.getvalue()
        log_stream.truncate(0)
        log_stream.seek(0) 
        write_logs(log_)
        return result
    return wrapper


## Function decorator for method "aggregations" of the Class "stock_stats"
def log_function_call(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        class_name = type(args[0]).__name__
        logging.info(f"{class_name}: Entering function: {func.__name__} with args: '{args[0].symbol}' {args[1:]}")
        log_ = log_stream.getvalue()
        log_stream.truncate(0)
        log_stream.seek(0) 
        write_logs(log_)
        # print(log_)
        
        result = func(*args, **kwargs)
        
        logging.info(f"{class_name}: Processing of the stock '{args[0].symbol}' succeeded. Exiting function: {func.__name__}.")
        log_ = log_stream.getvalue()
        log_stream.truncate(0)
        log_stream.seek(0) 
        write_logs(log_)
        # print(log_)
        return result
    return wrapper


## Property decorator for logging calculated attribute's processing:
def log_property_call(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        class_name = type(args[0]).__name__
        logging.info(f"{class_name}: Property '{func.__name__}' has been joined : {result}")
        log_ = log_stream.getvalue()
        log_stream.truncate(0)
        log_stream.seek(0) 
        write_logs(log_)
        # print(log_)
        return result
    return wrapper

## Logging function for statements in the 'load_stock_history' module:
logger = logging.getLogger(__name__)

def logs(*params):
    match params[0]:
        case 1: 
            log_msg = 'Reading "%s.csv" file from folder "%s" has been started'
            logger.info(log_msg, params[1], params[2])
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)
        case 2: 
            log_msg = 'Reading "%s.csv" file from folder "%s" has been finished'
            logger.info(log_msg, params[1], params[2])
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)
        case 3: 
            log_msg = 'Company name "%s" has been added into dataframe'
            logger.info(log_msg, params[1]) 
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)          
        case 4: 
            log_msg = "Stock '%s' data has been loaded into table 'stock_rates' in DB 'stocks'"
            logger.info(log_msg, params[1])
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)
        case 5: 
            log_msg = 'All the stocks have been processed into table "stock_rates" in DB "stocks"'
            logger.info(log_msg)
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)
        case 6:
            exc_type, exc_value, _ = params[3]
            log_msg = f'File {params[1]}.csv hasn\'t been found in the path.\n'
            log_msg2 = f'Exception type: {exc_type}, decription: {exc_value}'
            logger.error(log_msg + log_msg2)
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)
        case 7:
            exc_type, exc_value, _ = params[2]
            log_msg = f"Attribute 'Company_name' of  the %s-stock hasn't been added to the dataframe\n"
            log_msg2 = f'Exception type: {exc_type}, decription: {exc_value}'
            logger.error(log_msg + log_msg2)
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)
        case 8:
            exc_type, exc_value, _ = params[2]
            log_msg = f"%s-dataframe hasn't been loaded into table 'stock_rates' in DB 'stocks'\n"
            log_msg2 = f'Exception type: {exc_type}, decription: {exc_value}'
            logger.error(log_msg + log_msg2)
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)
        case 9:
            log_msg = f"%s: There is no data in the 'stock_rates' table under the selected start_date %s'\n"
            logger.error(log_msg, params[1], params[2])
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)
        case 10:
            log_msg = f"%s: There is no data in the 'stock_rates' table under the selected end_date %s'\n"
            logger.error(log_msg, params[1], params[2])
            log_ = log_stream.getvalue()
            log_stream.truncate(0)
            log_stream.seek(0) 
            write_logs(log_)
            # print(log_)




            


