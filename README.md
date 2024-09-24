# DE-projects
This is the study Python project.

Description:
Python Project 
1.	Download the  zip file to your VM ,extract the zip file content to folder name /stocks 
2.	 The zip file contain the following files 
a.	Symbols_valid_meta.csv - CSV with the Stock Symbol and its full name 
For example A - Agilent Technologies, Inc. Common Stock
b.	Under folder stocks - there is CSV for each company symbol and its rates 
3.	Develop Python Pipeline load_stock_history.py  that reads the company's CSV files from /stocks folder 1 by 1 from the file system and writes the data to the stock_rates table in MySQL DB as follows:
	a. add the company_name from the CSV Symbols_valid_meta.csv (don't load the Symbols_valid_meta.csv to the database)   
b. create table according to the CSV columns + company_name field
c. Use Pandas to read the CSV files and write the database in chunks of 500 rows. The process should enrich the company name before writing to the database .
4.	Create a python module stock_details that retrieves company stock details by the following filters . The maximum range should be 30 days. 
Incase filters are not accurate raise exception 
a.	Inputs 
i.	Company/stock symbol 
ii.	Start_date
iii.	End_date 
b.	Json array of the results - verify that the json format are valid
5.	Create a python module stocks_stats that retrieves companןקד stock details by the following filters
a.	Input 
i.	Json - List of stock symbols 
ii.	Start_date 
iii.	end_date
b.	Output 
i.	Json array with the following fields 
1.	Stock Symbol 
2.	Company full name 
3.	MAX Rate 
4.	MIN Rate 
5.	AVG Rate 
6.	Yield 
   Sort by Yield descending 
6.	General Instructions
a.	Use the Virtualbox for development and  running pipelines on several companies + testing 
b.	You will have a strong VM to test whole company data 
c.	Add module which handle the logging mechanism and use it to log important steps 
d.	Use a table called ETL_PROCESS_LOGS to write the progress and logs  of each company . The table should contain the following fields 
i.	ID - auto increment - PK
ii.	ETL_PROCESS - for now stocks_data
iii.	Log_time - default courant time 
iv.	Log_level - INFO Default , ERROR in case of error
v.	Log_DESC  - Log Description 
