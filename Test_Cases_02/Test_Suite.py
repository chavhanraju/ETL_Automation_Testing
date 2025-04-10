import pandas as pd
from datetime import datetime
from Utilities_03.Count_validation import Source_Target_Count_check
from Utilities_03.null_validation import Null_Checks
from Utilities_03.duplicate_validation import Duplicate_Records_chk
#from Utilities_03.column_mapping_validation import Column_mapping_Validation

dt = datetime.now().strftime("%Y-%m-%d_%H-%M%S")
# Creating a dictionary to store all validation results in list

validation_results = {
    "Src_Tgt_count": [],
    "Null_checks":[],
    "Duplicate_checks":[]
    #"Column mapping":[]
}

# Adding test result into src Tqt count list.
validation_results["Src_Tgt_count"] = Source_Target_Count_check()
validation_results["Null_checks"]=Null_Checks()
validation_results["Duplicate_checks"]=Duplicate_Records_chk()
#validation_results["Column_mapping"]=Column_mapping_Validation()

print(validation_results["Src_Tgt_count"])

# Combine results of all tables count validation
df_count_checks = pd.concat(validation_results["Src_Tgt_count"], ignore_index=True)
df_null_checks = pd.concat(validation_results["Null_checks"],ignore_index=True)
df_duplicate_checks = pd.concat(validation_results["Duplicate_checks"],ignore_index=True)
#df_column_mapping = pd.concat(validation_results["Column_mapping"],ignore_index=True)

print(df_count_checks)

# test Results output file name to export the test result.
output_file_path = f"C:\\Users\\rashm\\PycharmProjects\\pythonProject\\pythonProject\\pythonProject1\\pythonProject\\pythonProject\\pythonProject\\ETL_Automation_Testing_2025\\Test_Output_04\\Test_Results_{dt}.xlsx"

# Write all results to the corresponding excel sheets
with pd.ExcelWriter(output_file_path) as output_file:
    # Write each Dataframe to its corresponding excel sheet
    df_count_checks.to_excel(output_file, sheet_name="Count_Result", index=False)
    df_null_checks.to_excel(output_file,sheet_name="Null_checks",index=False)
    df_duplicate_checks.to_excel(output_file,sheet_name="Duplicate_checks",index=False)
    #df_column_mapping.to_excel(output_file, sheet_name="Column_mapping", index=False)