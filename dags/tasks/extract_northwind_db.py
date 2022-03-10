import psycopg2
import os
import sys

from sqlalchemy import table

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


def get_tables(db_cursor):

    table_names = []

    db_cursor.execute(
        """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE';
    """
    )

    for name in db_cursor.fetchall():
        table_names.append(name[0])

    print("Table names acquired")
    return table_names


def sql_to_csv(db_cursor, table_name):
    # 1: Selecionar linhas de uma tabela
    # 2: Criar pasta da tabela
    # 3: Criar pasta de exec_date
    # 4: Salvar dados em csv

    path_file = "/data/postgres/{0}/{1}".format(
        table_name, exec_date, table_name + "-" + exec_date + ".csv"
    )

    isExist = os.path.exists(path_file)
    # Check if a directory exists and create if it doesnt
    if not isExist:
        os.makedirs(path_file, exist_ok=True)
        print(f"Folder {table_name} created successfuly")

    path_file += "/" + table_name + "-" + exec_date + ".csv"

    # os.makedirs(path_file, exist_ok=True)

    sql_to_file = "COPY {0} TO STDOUT WITH CSV DELIMITER ',' HEADER;".format(table_name)

    with open(path_file, "w") as file_output:
        db_cursor.copy_expert(sql_to_file, file_output)
        print(f"File {table_name}-{exec_date}.csv created successfuly")


for table_name in get_tables(db_cursor):
    sql_to_csv(db_cursor, table_name)

db_conn.close()
