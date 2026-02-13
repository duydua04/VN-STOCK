import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_postgres_schema(connection, cursor, db_password):
    database = "vn_stock_data"
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        cursor.execute(f"DROP DATABASE IF EXISTS {database}")
        print(f"--------DROP database: {database} in POSTGRES---------")

        cursor.execute(f"CREATE DATABASE {database}")
        print(f"--------CREATE database: {database} in POSTGRES--------")

    except psycopg2.Error as e:
        raise Exception(f"-------Failed to CREATE DB: ERROR : {e}--------") from e

    dsn_params = connection.get_dsn_parameters()
    db_user = dsn_params.get('user')
    db_host = dsn_params.get('host')
    db_port = dsn_params.get('port')

    cursor.close()
    connection.close()

    print(f"--------Reconnecting to new database: {database}--------")

    try:
        # Tạo kết nối mới tới database
        new_connection = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=database
        )
        new_cursor = new_connection.cursor()

        with open("/src/sql/schema.sql", 'r') as f:
            sql_script = f.read()
            new_cursor.execute(sql_script)
            print("-----Executed Schema Script------")

        new_connection.commit()
        print("-----CREATED POSTGRES SCHEMA------")

        return new_connection, new_cursor

    except psycopg2.Error as e:
        if 'new_connection' in locals() and new_connection:
            new_connection.rollback()
        raise Exception(f"-------Failed to CREATE POSTGRES SCHEMA: ERROR : {e}--------") from e