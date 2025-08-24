# Code for ETL operations for List of largest banks

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

#initialize all the known variables
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
exchange_rate_path = './exchange_rate.csv'
log_file = 'code_log.txt'

#logs the mentioned message of a given stage of the code execution to a log file
def log_progress(message):
    timestamp_format = '%Y-%m-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

#extracts the required information from the website and save it to a data frame
def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            name = col[1].get_text(strip=True)
            market_cap = float(col[2].get_text(strip=True))
            data_dict = {
                "Name": name,
                "MC_USD_Billion": market_cap
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

''' 
This function accesses the CSV file for exchange rate
information, and adds three columns to the data frame, each
containing the transformed version of Market Cap column to
respective currencies'''
def transform(df, exchange_rate_path):
    exchange_rate = pd.read_csv(exchange_rate_path, index_col=0).to_dict()['Rate']
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate['GBP'], 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate['EUR'], 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate['INR'], 2)
    return df

#saves the final data frame as a CSV file
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

#saves the final data frame to a database
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

#runs the query on the database table and prints the output on the terminal
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Log the initialization of the ETL process 
log_progress('Preliminaries complete. Initiating ETL process')

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
df = extract(url, table_attribs)

# Log the completion of the Extraction process and beginning of Transformation process
log_progress('Data extraction complete. Initiating Transformation process')
df=transform(df, exchange_rate_path)

# Log the completion of the Transformation process and beginning of the Loading process 
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)

# Log the completion of the Loading process
log_progress('Data saved to CSV file')

# Log the initialization of the SQL connection
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')

# Log data loaded to the database
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Executing queries')

# Log finalization of the process
query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

# Log the closing of SQL connection
sql_connection.close()
log_progress('Server Connection closed')