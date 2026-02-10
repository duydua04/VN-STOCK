from pyspark.sql import SparkSession, DataFrame


class SparkWritePostgres:
    def __init__(self, spark: SparkSession, db_config):
        """
        Class ghi dữ liệu vào postgresql.
        """
        self.spark = spark
        self.db_config = db_config

    def write_data(
            self,
            df: DataFrame,
            table_name: str,
            mode: str = "append",
            **kwargs
    ):
        """
        Ghi DataFrame vào PostgreSQL.
        """
        print(f"-------- Writing data to Postgres table: {table_name} --------")

        try:
            writer = df.write \
                .format("jdbc") \
                .option("url", self.db_config.jdbc_url) \
                .option("user", self.db_config.user) \
                .option("password", self.db_config.password) \
                .option("dbtable", table_name) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode)

            if kwargs:
                writer = writer.options(**kwargs)

            writer.save()

            print(f"-------- Successfully wrote data to Postgres: {table_name} --------")

        except Exception as e:
            raise Exception(f"-------- Failed to write to Postgres table {table_name}: {e} -------") from e