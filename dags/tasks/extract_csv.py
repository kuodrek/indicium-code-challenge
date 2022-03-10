import shutil
import os
import sys

# Get {{ execution_date }}
date = sys.argv[1][:10]

path_file_input = "/data/order_details.csv"

path_file_output = "/data/csv/{0}/order_details-{1}.csv".format(date, date)

isExist = os.path.exists(path_file_output)
# Check if a directory exists and create if it doesnt
if not isExist:
    os.makedirs(os.path.dirname(path_file_output), exist_ok=True)
    print(f"Folder csv created successfuly")
else:
    print("Folder csv already created")

shutil.copy(path_file_input, path_file_output)
