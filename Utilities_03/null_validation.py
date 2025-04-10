import pandas as pd
import json
import os
import logging
from datetime import datetime
from Utilities_03.Source_Target_DB_Conn import DB_Conn


dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = f'C:\\Users\Admin\\PycharmProjects\\ETL_Automation_Testing\\Logs\\ETL_Logs_{dt}.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def Null_Checks():

    Test_cases_json = r'C:\\Users\\Admin\\PycharmProjects\\ETL_Automation_Testing\\Config_Testcases_01'

    # empty List to store the results for all tables
    all_results = []

    logging.info("*** Null checks initiated in Target Table ***")


    #Loop through all JSON files in the directoryy
    for filename in os.listdir(Test_cases_json):
        tc_json_file_path = Test_cases_json + '/' + filename

        try:

            db_conn = DB_Conn()
            target_db_conn = db_conn.Oracle_DB_Conn()
            target_cursor = target_db_conn.cursor()

            #Load SQL queries from all JSON files into python program
            with open(tc_json_file_path, 'r') as SQL_file:
                SQL_Queries = json.load(SQL_file)

            # Fetch the target table name
            t_table_name = SQL_Queries["tc_01_Verifying Source & Target Table Existence"]["t_table"]

            logging.info(f"*** Target Table:{t_table_name} validations initiated ***")

            #Capturing null check queries for all target tables from all .Json files
            t_query = SQL_Queries['tc_03_Verifying Null Values in Target Table']['null_rec_cnt']
            target_cursor.execute(t_query)
            df_null_cnt = pd.DataFrame(target_cursor)

            # Find if there are any null values
            if df_null_cnt.empty or df_null_cnt.iloc[0, 0] == 0:
                null_count = 0
                null_records = None  # No null rows
                result = "Null records not found!"
                status = "PASS"
            else:
                null_count = df_null_cnt.iloc[0, 0]
                null_rec_query = SQL_Queries['tc_03_Verifying Null Values in Target Table']['null_records']
                target_cursor.execute(null_rec_query)
                null_records = pd.DataFrame(target_cursor).to_string(index=False, header=False)
                null_records = ','.join(null_records.split())
                result = "Null records found!"
                status = "FAIL"
                logging.info(f"Null records found in table {t_table_name}: {null_records}")

            # Create a DataFrame to return the result
            df_nulls = pd.DataFrame(
                {
                    "Database": ["Target table"],
                    'Table_names': [t_table_name],
                    'Count': [null_count],
                    'Result': [result],
                    'Status': [status],
                    'Null_Records':[null_records]
                }
            )

            # Append the result for the current file to the list
            all_results.append(df_nulls)

        except Exception as e:
            logging.error(f"in file - {filename}: {e}")
            continue

    # Return the list of DataFrames
    return all_results