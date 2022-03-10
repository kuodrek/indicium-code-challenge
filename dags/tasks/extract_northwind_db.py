import psycopg2
import os
import sys

from task_utils import get_tables

# Get {{ execution_date }}
exec_date = sys.argv[1][:10]

# Connect to northwind database

host = "northwind_db"
database = "northwind"
user = "northwind_user"
password = "thewindisblowing"

db_conn = psycopg2.connect(host=host, database=database, user=user, password=password)
db_conn.set_client_encoding("UTF8")
db_cursor = db_conn.cursor()


def sql_to_csv(db_cursor, table_name):

    # Get path file
    path_file = "/data/postgres/{0}/{1}".format(table_name, exec_date)

    # Check if directory exists
    isExist = os.path.exists(path_file)
    # Check if a directory exists and create if it doesnt
    if not isExist:
        os.makedirs(path_file, exist_ok=True)
        print(f"Folder {table_name} created successfuly")

    # Append data name to path file
    path_file += "/" + table_name + "-" + exec_date + ".csv"

    # SQL Query
    sql_to_file = "COPY {0} TO STDOUT WITH CSV DELIMITER ',' HEADER;".format(table_name)

    # Write to file
    with open(path_file, "w") as file_output:
        db_cursor.copy_expert(sql_to_file, file_output)
        print(f"File {table_name}-{exec_date}.csv created successfuly")


for table_name in get_tables(db_conn):
    sql_to_csv(db_cursor, table_name)

db_conn.close()
