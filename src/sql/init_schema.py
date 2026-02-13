from src.sql.postgresql_connect import PostgresConnect
from configs.db_config import get_database_config
from src.sql.schema_manager import create_postgres_schema


def init_database_schema(db_config):
    print(f"Connecting to host: {db_config.host}")
    with PostgresConnect(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password,
            database="postgres"
    ) as pg_client:
        connection, cursor = pg_client.connection, pg_client.cursor
        create_postgres_schema(connection, cursor, db_config.password)