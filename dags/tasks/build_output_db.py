# print('build_output_db')
import pandas as pd
import psycopg2
import sys
from initialize_tables import init_tables

date = sys.argv[1][:10]

host = "output_db"
database = "northwind_output_db"
user = "postgres"
password = "postgres"

db_conn = psycopg2.connect(host=host, database=database, user=user, password=password)
db_conn.set_client_encoding("UTF8")
db_cursor = db_conn.cursor()

print("Connection to output db was successful")

init_tables(db_conn)

db_conn.close()
