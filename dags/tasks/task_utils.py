import sys
import psycopg2

N_TABLES = 14

# Get table names of a database
def get_tables(db_conn):
    db_cursor = db_conn.cursor()
    table_names = []

    db_cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE';
    """
    )

    for name in db_cursor.fetchall():
        table_names.append(name[0])

    print("Table names acquired")
    return table_names


# Create tables for the output database
def init_tables(db_conn, exec_date):
    db_cursor = db_conn.cursor()

    print("Checking number of tables")
    db_cursor.execute(
        """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type='BASE TABLE';
    """
    )

    table_count = db_cursor.fetchone()[0]
    print(f"table count: {table_count}")

    if table_count != N_TABLES:
        print("Droping existing tables")
        db_cursor.execute(
            """
            DROP TABLE IF EXISTS customer_customer_demo;
            DROP TABLE IF EXISTS customer_demographics;
            DROP TABLE IF EXISTS employee_territories;
            DROP TABLE IF EXISTS orders;
            DROP TABLE IF EXISTS customers;
            DROP TABLE IF EXISTS products;
            DROP TABLE IF EXISTS shippers;
            DROP TABLE IF EXISTS suppliers;
            DROP TABLE IF EXISTS territories;
            DROP TABLE IF EXISTS us_states;
            DROP TABLE IF EXISTS categories;
            DROP TABLE IF EXISTS region;
            DROP TABLE IF EXISTS employees;
            DROP TABLE IF EXISTS order_details;
        """
        )
        print("All tables dropped successfuly")

        db_cursor.execute(
            """CREATE TABLE categories (
            category_id smallint NOT NULL,
            category_name character varying(15) NOT NULL,
            description text,
            picture bytea,
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE customer_customer_demo (
            customer_id bpchar NOT NULL,
            customer_type_id bpchar NOT NULL,
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE customer_demographics (
            customer_type_id bpchar NOT NULL,
            customer_desc text,
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE customers (
            customer_id bpchar NOT NULL,
            company_name character varying(40) NOT NULL,
            contact_name character varying(30),
            contact_title character varying(30),
            address character varying(60),
            city character varying(15),
            region character varying(15),
            postal_code character varying(10),
            country character varying(15),
            phone character varying(24),
            fax character varying(24),
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE employees (
            employee_id smallint NOT NULL,
            last_name character varying(20) NOT NULL,
            first_name character varying(10) NOT NULL,
            title character varying(30),
            title_of_courtesy character varying(25),
            birth_date date,
            hire_date date,
            address character varying(60),
            city character varying(15),
            region character varying(15),
            postal_code character varying(10),
            country character varying(15),
            home_phone character varying(24),
            extension character varying(4),
            photo bytea,
            notes text,
            reports_to smallint,
            photo_path character varying(255),
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE employee_territories (
            employee_id smallint NOT NULL,
            territory_id character varying(20) NOT NULL,
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE orders (
            order_id smallint NOT NULL,
            customer_id bpchar,
            employee_id smallint,
            order_date date,
            required_date date,
            shipped_date date,
            ship_via smallint,
            freight real,
            ship_name character varying(40),
            ship_address character varying(60),
            ship_city character varying(15),
            ship_region character varying(15),
            ship_postal_code character varying(10),
            ship_country character varying(15),
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE products (
            product_id smallint NOT NULL,
            product_name character varying(40) NOT NULL,
            supplier_id smallint,
            category_id smallint,
            quantity_per_unit character varying(20),
            unit_price real,
            units_in_stock smallint,
            units_on_order smallint,
            reorder_level smallint,
            discontinued integer NOT NULL,
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE region (
            region_id smallint NOT NULL,
            region_description bpchar NOT NULL,
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE shippers (
            shipper_id smallint NOT NULL,
            company_name character varying(40) NOT NULL,
            phone character varying(24),
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE suppliers (
            supplier_id smallint NOT NULL,
            company_name character varying(40) NOT NULL,
            contact_name character varying(30),
            contact_title character varying(30),
            address character varying(60),
            city character varying(15),
            region character varying(15),
            postal_code character varying(10),
            country character varying(15),
            phone character varying(24),
            fax character varying(24),
            homepage text,
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE territories (
            territory_id character varying(20) NOT NULL,
            territory_description bpchar NOT NULL,
            region_id smallint NOT NULL,
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE us_states (
            state_id smallint NOT NULL,
            state_name character varying(100),
            state_abbr character varying(2),
            state_region character varying(50),
            execution_date date
        );"""
        )

        db_cursor.execute(
            """CREATE TABLE order_details (
            order_id smallint NOT NULL,
            product_id smallint NOT NULL,
            unit_price real NOT NULL,
            quantity smallint NOT NULL,
            discount real NOT NULL,
            execution_date date
        );"""
        )

        db_conn.commit()
        print("Tables created successfuly")

    print(f"Deleting data where exec_date = {exec_date}:")
    for table_name in get_tables(db_conn):
        sql_delete_query = """DELETE FROM {0} WHERE execution_date = '{1}'; """.format(
            table_name, exec_date
        )
        db_cursor.execute(sql_delete_query)

    try:
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        db_conn.rollback()
        db_cursor.close()
        return 1
    finally:
        print(f"Data where exec_date = {exec_date} deleted successfuly")
