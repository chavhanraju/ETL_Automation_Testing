import pandas as pd
import json
import os
import logging
from datetime import datetime
from Utilities_03.Source_Target_DB_Conn import DB_Conn

dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = f'C:\\Users\Admin\\PycharmProjects\\ETL_Automation_Testing\\Logs\\ETL_Logs_{dt}.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def Column_mapping_Validation():
    Test_cases_json = r'C:\\Users\\Admin\\PycharmProjects\\ETL_Automation_Testing\\Config_Testcases_01'

    #List to collect results for all files
    all_results = []

    logging.info("*** Source & Target column mapping validation initiated ***")

    # Loop through all JSON files in the directory
    for filename in os.listdir(Test_cases_json):
        tc_json_file_path = Test_cases_json + '/' + filename

        try:

            db_conn = DB_Conn()
            source_db_conn = db_conn.MySQL_DB_Conn()
            source_cursor = source_db_conn.cursor()

            target_db_conn = db_conn.Oracle_DB_Conn()
            target_cursor = target_db_conn.cursor()

            # Load SQL queries from the JSON file
            with open(tc_json_file_path, 'r') as SQL_file:
                SQL_Queries = json.load(SQL_file)

            # Fetch source and target table names
            s_table_name = SQL_Queries["tc_01_Verifying Source & Target Table Existence"]["s_table"]
            t_table_name = SQL_Queries["tc_01_Verifying Source & Target Table Existence"]["t_table"]

            # Get the source and target queries for columns
            s_query = SQL_Queries['tc_05_Verifying Source & Target Columns mapping']['s_table']
            source_cursor.execute(s_query)
            df_source_result = pd.DataFrame(source_cursor)

            t_query = SQL_Queries['tc_05_Verifying Source & Target Columns mapping']['t_table']
            target_cursor.execute(t_query)
            df_target_result = pd.DataFrame(target_cursor)

            # Initialize an empty list to track mismatches records
            mismatches_list = []

            # Iterate through rows to compare source and target data row by row
            for i in range(len(df_source_result)):
                source_row = df_source_result.iloc[i].values  # Get the row from source
                target_row = df_target_result.iloc[i].values  # Get the row from target

                if any(source_row != target_row):
                    mismatches_list.append(f"p_id:{source_row[0]}")

            # Create a dataframe to store mismatches records
            if mismatches_list ==[]:
                mismatch_result_df = pd.DataFrame({
                    "Database": ["Source table", "Target table"],
                    "Table_names": [s_table_name, t_table_name],
                    "Result": ["Source & Target tables data matched!", None],
                    "Status": ["PASS", None]
                })

            # If mistmaches list is empty execute the below code
            else:
                mismatch_result_df = pd.DataFrame({
                    "Database": ["Source table", "Target table"],
                    "Table_names": [s_table_name, t_table_name],
                    "Result": ["Source & Target tables data not matched!", None],
                    "Status": ["FAIL", None],
                    "Mismatched_records": [mismatches_list, None]
                })


            # Append the result for the current file to the list
            all_results.append(mismatch_result_df)

        except Exception as e:
            logging.error(f"in file - {filename}: {e}")
            continue

    # Return the list of DataFrames
    return all_results