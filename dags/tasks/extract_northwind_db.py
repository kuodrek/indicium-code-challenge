import psycopg2
import pandas as pd
import os
import sys

date = sys.argv[1][:10]

#PostgreSQL Connection

host = "northwind_db"
database = "northwind"
user = "northwind_user"
password = "thewindisblowing"

db_conn = psycopg2.connect(host=host,database = database, user = user, password = password)
db_cursor = db_conn.cursor()

def get_table_names(db_cursor):

    table_names = []

    db_cursor.execute("""SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'""")

    for name in db_cursor.fetchall():
        table_names.append(name[0])

    return table_names

for table_name in get_table_names(db_cursor): print(table_name)