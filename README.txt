# ETL Process for SF Police Incidents Data
This ETL (Extract, Transform, Load) process is designed to fetch data from the San Francisco government’s API, perform necessary transformations, and load the data into a Redshift database.

## Prerequisites
- Python 3.7 or higher
- Required Python libraries: requests, pandas, sqlalchemy, os, json, io, psycopg2, and python-dotenv.

## Setup
1. Clone this repository to your local machine.
2. Install the required Python libraries using pip:

pip install requests pandas sqlalchemy psycopg2-binary python-dotenv

3. Set up your environment variables in a .env file in the root directory of the project. The file should contain the following variables:

- REDSHIFT_HOST=your_redshift_host 
- REDSHIFT_DATABASE=your_redshift_database 
- REDSHIFT_USER=your_redshift_user 
- REDSHIFT_PWD=your_redshift_password 
- REDSHIFT_PORT=your_redshift_port

Replace your_redshift_host, your_redshift_database, your_redshift_user, your_redshift_password, and your_redshift_port with your actual Redshift credentials.

## Running the ETL Process
To run the ETL process, simply execute the Python script:

python etl.py

This will start the ETL process, which includes the following steps:
1. Extract: Data is extracted from the San Francisco government’s API. The data includes information about police incidents in San Francisco.
2. Transform: The extracted data is transformed to match the schema of the target database. This includes converting certain columns to the correct data types, cleaning up column names, and removing duplicates.
3. Load: The transformed data is loaded into a Redshift database.

Please note that this ETL process is designed to be run manually. If you want to schedule the ETL process to run at specific intervals, consider using a task scheduler like cron (on Unix-based systems) or Task Scheduler (on Windows).

## Troubleshooting
If you encounter any issues while running the ETL process, please check the following:
1. Ensure that all the required Python libraries are installed and up-to-date.
2. Check that your Redshift credentials in the .env file are correct.
3. Make sure that your Redshift cluster is running and accessible.

