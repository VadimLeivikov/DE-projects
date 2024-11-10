## Building an Analytical Data Warehouse

The objective of this work is to demonstrate a grasp of key concepts in the subject area and skills in working with essential tools that are in demand today in the field of data engineering.

### The main goals of the project are:
* to create a data warehouse following the Inmon methodology and using a multi-layer architecture (NDS and DDS);
* to develope ETL processes for loading, processing, and analyzing data;
* to build data marts for visualizing key metrics of store performance.

### Project stages
1. Develop DWH architecture, write DDL scripts using **PostgreSQL**
2. Obtain original raw data from kaggle.com and put them to the stage layer;
3. Generate new daily reports on time schedule based on the original dataset (**Docker + Python + cron**);
3. Clean the data and forward them to normalized layer (nds layer);
4. Check the outliers & anomalies and send corrupted data to the DQ layer;
5. Forward data to data mart layer (dds layer); 
6. Adjust ETL process and orchestrate it with **Apache Airflow**;
7. Monitor status of the ETL loads through the metadata.layer;
8. Build data marts with **PowerBI**;
9. Adjust **dbt-project** with using models, snapshots and creating documentations for it.

### The developed layers of the DWH are:
* stage - raw layer, save the origin data AS IS
* nds - data normalized to 3NF 
* dds - layer of the data marts
* dq - data quality layer, consists of corrupted data needs to be resoolved
* metadata - information about statuses of the daily ETL processes

### Technology stack
* PostgreSQL 12.9 — DBMS for implementing the data warehouse
* Python (pandas, faker) 3.11.9  — for processing and generating daily store reports
* Apache Airflow 2.10.2  — for automating and orchestrating ETL processes
* Docker 27.2.0  — for containerizing services
* PowerBI Desktop 2.137.952.0 64-bit — for creating interactive dashboards
* dbt Core 1.8.8 + Postgres plugin 1.8.2 — utilized as additional ETL tool 


### Detailed description 
Detailed description of the project is provided in [documentation](https://github.com/VadimLeivikov/DE-projects/tree/small_dwh_project/documentation).

### Results and conclusions

The following tasks were accomplished within the project using various technologies:

| No.	| Task Completed	| Technology Stack |
|:-----:|:----------------------|:-----------------|
| 1	| A data warehouse was created, and DDL and DML scripts were written for all layers of the data warehouse	| PostgreSQL |
|2	|A daily report generator was created, simulating both "correct" data from a DQ perspective and erroneous data	|Python + Docker|
|3	|Orchestration of daily report loading was set up	|Crontab + .bat script (SQL script execution) + PostgreSQL stored procedure|
|4	|A DAG was created to implement ETL processes for loading data into the data warehouse, validating it, and logging ETL task statuses|	Apache Airflow |
|5	|Dashboards were built based on the data mart layer	|Power BI |
|6	|dbt models have been configured for an alternative ETL process setup and data quality (DQ) checks	| dbt |

The structure of the NDS and DDS layers facilitates efficient storage and analysis of data, while the creation of dashboards helps visualize key metrics and improve decision-making processes.<br> 
The project demonstrates the potential for using ETL processes to automate data loading and provide flexibility in configuration and scaling.<br> 
Additionally, data quality control and tracking of uploads are implemented through the dq and metadata layers.

