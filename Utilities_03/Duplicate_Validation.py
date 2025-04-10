import pandas as pd
import json
import os
import logging
from datetime import datetime
from Utilities_03.Source_Target_DB_Conn import DB_Conn

dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = f'C:\\Users\Admin\\PycharmProjects\\ETL_Automation_Testing\\Logs\\ETL_Logs_{dt}.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def Duplicate_Records_chk():
    Test_cases_json = r'C:\\Users\\Admin\\PycharmProjects\\ETL_Automation_Testing\\Config_Testcases_01'

    # List to collect the results for all files
    all_results = []

    # Loop through all JSON files in the directory
    for filename in os.listdir(Test_cases_json):
        tc_json_file_path = Test_cases_json + '/' + filename

        try:

            db_conn = DB_Conn()
            target_db_conn = db_conn.Oracle_DB_Conn()
            target_cursor = target_db_conn.cursor()

            #Load SQL queries from all JSON files into python program
            with open(tc_json_file_path, 'r') as SQL_file:
                SQL_Queries = json.load(SQL_file)

            #Capturing Source & Target table names from .json file
            t_table_name = SQL_Queries["tc_01_Verifying Source & Target Table Existence"]["t_table"]

            # Get the query for checking duplicate records in the target table
            t_query = SQL_Queries['tc_04_Checking Duplicate Records in Target Table']['t_dup_cnt']
            target_cursor.execute(t_query)
            df_tgt_cnt = pd.DataFrame(target_cursor)

            # Determine if there are any duplicate records
            if df_tgt_cnt.empty or df_tgt_cnt.iloc[0, 0] == 0:
                dup_count = 0
                dup_records = None  # No duplicates found
                result = "Duplicate rows not found!"
                status = "PASS"
            else:
                dup_count = df_tgt_cnt.iloc[0, 0]
                dup_rec_query = SQL_Queries['tc_04_Checking Duplicate Records in Target Table']['t_dup_records']
                target_cursor.execute(dup_rec_query)
                dup_records = pd.DataFrame(target_cursor).to_string(index=False,header=False)
                dup_records =','.join(dup_records.split())
                result = "Duplicate rows found!"
                status = "FAIL"
                logging.info(f"Duplicate records found in table {t_table_name}: {dup_records}")

            #Create a DataFrame to write the result in Excel sheet
            df_Duplicate_records = pd.DataFrame(
                {
                    "Database": ["Target table"],
                    "Table_names": [t_table_name],
                    'Count': [dup_count],
                    'Result': [result],
                    'Status': [status],
                    'Duplicate_Records':[dup_records]
                }
            )

            #Append each result to the list
            all_results.append(df_Duplicate_records)

        except Exception as e:
            logging.error(f"in file - {filename}: {e}")
            continue

    #Return all collected results once all the json file (tables) process completed
    return all_results