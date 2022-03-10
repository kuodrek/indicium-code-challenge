# print('build_output_db')
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import sys
from task_utils import get_tables, init_tables

# Function to insert dataframe values into a table
def execute_values(db_conn, df, table):

    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ",".join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    db_cursor = db_conn.cursor()
    try:
        extras.execute_values(db_cursor, query, tuples)
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        db_conn.rollback()
        db_cursor.close()
        return 1
    print(f"Inserted dataframe: {table}")
    db_cursor.close()


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
print("Connection to output db was successful")

# Initialize tables
init_tables(db_conn, exec_date)

# Categories, region, us_states

for table_name in get_tables(db_conn):
    if table_name == "order_details":
        df = pd.read_csv(
            "/data/csv/{0}/{1}.csv".format(exec_date, table_name + "-" + exec_date)
        )
    else:
        df = pd.read_csv(
            "/data/postgres/{0}/{1}/{2}.csv".format(
                table_name, exec_date, table_name + "-" + exec_date
            )
        )

    # Dropping any rows containing NaN
    df = df.dropna(how="any")

    # Insert a column to store execution_date and avoid duplicates from the same day
    # The verification is done by init_tables()
    df_n_rows = df.shape[0]
    date_list = [exec_date] * df_n_rows

    df["execution_date"] = date_list
    execute_values(db_conn, df, table_name)

db_conn.close()
