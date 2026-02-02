from pyspark.sql import SparkSession, DataFrame


class SparkWritePostgres:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_data(
            self,
            df: DataFrame,
            db_config,
            table_name: str,
            mode: str = "append"
    ):
        """
        Ghi DataFrame vào PostgreSQL
        """
        print(f"-------- Writing data to Postgres table: {table_name} --------")

        try:
            df.write \
                .format("jdbc") \
                .option("url", db_config.jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", db_config.user) \
                .option("password", db_config.password) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()

            print(f"-------- Successfully wrote data to Postgres: {table_name} --------")

        except Exception as e:
            raise Exception(f"--------Failed to write to Postgres table {table_name}: {e}-------") from e