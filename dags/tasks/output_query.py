import psycopg2
import os
import sys

# Get {{ execution_date }}
exec_date = sys.argv[1][:10]

# Connect to output database

host = "output_db"
database = "northwind_output_db"
user = "postgres"
password = "postgres"

db_conn = psycopg2.connect(host=host, database=database, user=user, password=password)
db_conn.set_client_encoding("UTF8")
db_cursor = db_conn.cursor()

# Get path file
path_file = "/data/output/{0}".format(exec_date)

# Check if directory exists
isExist = os.path.exists(path_file)

# Check if a directory exists and create if it doesnt
if not isExist:
    os.makedirs(path_file, exist_ok=True)
    print(f"Folder Orders Query created successfuly")

# Append data name to path file
path_file += "/orders_query-" + exec_date + ".csv"

# Select tables
join_tables_query = """
SELECT orders.order_id, orders.order_date, orders.customer_id,
order_details.unit_price, order_details.quantity, order_details.discount
FROM orders
INNER JOIN order_details ON orders.order_id=order_details.order_id
"""

# SQL Query
sql_to_file = "COPY ({0}) TO STDOUT WITH CSV DELIMITER ',' HEADER;".format(
    join_tables_query
)

# Write results on csv file
with open(path_file, "w") as file_output:
    db_cursor.copy_expert(sql_to_file, file_output)
    print(f"File orders_query-{exec_date}.csv created successfuly")

db_conn.close()
