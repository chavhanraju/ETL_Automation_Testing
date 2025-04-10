import pandas as pd
import json
import os
import logging
from datetime import datetime
from Utilities_03.Source_Target_DB_Conn import DB_Conn

dt=datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
log_file = f'C:\\Users\Admin\\PycharmProjects\\ETL_Automation_Testing\\Logs\\ETL_Logs_{dt}.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def Source_Target_Count_Check():
    Test_cases_Json=r'C:\\Users\\Admin\\PycharmProjects\\ETL_Automation_Testing\\Config_Testcases_01'

    #empty list to store the result of all table
    all_table_result_list=[]
    logging.info("*** Source and Target table Validation initiated ***")
    logging.info(f"*** Source Database MySQL Connection initiated ***")
    logging.info(f"*** Target Database Oracle Connection initiated ***")

    #loop Through all json file in the directory
    for filename in os.listdir(Test_cases_Json):
        tc_json_file_path=Test_cases_Json + '/' + filename
        try:
            db_conn=DB_Conn()
            source_db_conn=db_conn.MySQL_DB_Conn()
            source_cursor=source_db_conn.cursor()

            target_db_conn=db_conn.Oracle_DB_Conn()
            target_cursor=target_db_conn.cursor()

            logging.info(f"*** Initiated processing of file:{filename} ***")
            #load SQL queries from all Json file into python program
            with open(tc_json_file_path,'r') as SQL_file:
                SQL_Queries=json.load(SQL_file)

            #Capturing Source and Target name from .jon filr

            s_table_name=SQL_Queries["tc_01_Verifying Source & Target Table Existence"]["s_table"]
            t_table_name=SQL_Queries["tc_01_Verifying Source & Target Table Existence"]["t_table"]
            logging.info(f'*** Source table:{s_table_name} Target table {t_table_name} validation initiated ***')

            #Run the source count query and load the result into a dataframe
            s_query = SQL_Queries["tc_02_Verifying Source & Target Record counts"]["s_count"]
            source_cursor.execute(s_query)
            df_source_cnt=pd.DataFrame(source_cursor)

            ##Run the target count query and load the result into a dataframe
            t_query=SQL_Queries["tc_02_Verifying Source & Target Record counts"]["s_count"]
            target_cursor.execute(t_query)
            df_target_cnt=pd.DataFrame(target_cursor)
            logging.info(f"Source table count:{df_source_cnt.loc[0,0]} Target table count:{df_target_cnt[0,0]}")

            ##compare the source and target record count
            if df_target_cnt.iloc[0,0] == df_target_cnt.iloc[0,0]:
                Result="Source & Target count are matched"
                Status="PASS"
            else:
                Result="Source & Target count are not matched"
                Status="FAIL"

            #Create a data frame to write the results in excel sheet
            df_s_t_count_result=pd.DataFrame(
                {
                    "Database":["Source_MySQL", "Target Oracle"],
                    "Table_Name":[s_table_name,t_table_name],
                    "Count":[df_source_cnt.iloc[0,0],df_target_cnt.iloc[0,0]],
                    "Result":["Result","Result"],
                    "Status":["Status","Status"]

                }
            )

        #Append Eeach results to the list
            all_table_result_list.append(df_s_t_count_result)
        except Exception as e:
            logging.error(f"in file - {filename}: {e}")
            continue
         #return all the collection results once all the json file (tablr) process
    return all_table_result_list
