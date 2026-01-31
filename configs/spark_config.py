from typing import Optional, List, Dict
from pyspark.sql import SparkSession

from configs.db_config import db_config
from db_config import get_database_config
from bucket_config import get_bucket_config

class SparkConnect:
    def __init__(
            self,
            app_name: str,
            master_url: str,
            executor_memory: Optional[str] = "4g",
            executor_cores: Optional[int] = 2,
            driver_memory: Optional[str] = "2g",
            num_executors: Optional[int] = 1,
            jar_packages: Optional[List[str]] = None,
            spark_conf: Optional[Dict[str, str]] = None,
            log_level: str = "INFO"
    ):
        self.app_name = app_name
        self.spark = self.create_spark_session(
            master_url,
            executor_memory,
            executor_cores,
            driver_memory,
            num_executors,
            jar_packages,
            spark_conf,
            log_level
        )

    def create_spark_session(
            self,
            master_url: str,
            executor_memory: Optional[str] = "4g",
            executor_cores: Optional[int] = 2,
            driver_memory: Optional[str] = "2g",
            num_executors: Optional[int] = 1,
            jar_packages: Optional[List[str]] = None,
            spark_conf: Optional[Dict[str, str]] = None,
            log_level: str = "INFO"
    ) -> SparkSession:

        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master_url)

        if executor_memory:
            builder.config("spark.executor.memory", executor_memory)
        if executor_cores:
            builder.config("spark.executor.cores", executor_cores)
        if driver_memory:
            builder.config("spark.driver.memory", driver_memory)
        if num_executors:
            builder.config("spark.executor.instances", num_executors)
        if jar_packages:
            jar_packages_url = ",".join([jar_package for jar_package in jar_packages])
            builder.config("spark.jars.packages", jar_packages_url)
        if spark_conf:
            for key, value in spark_conf.items():
                builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        return spark

    def stop(self):
        if self.spark:
            self.spark.stop()
            print("-------Stop Spark Session--------")

def get_spark_config():
    db_configs = get_database_config()
    bucket_config = get_bucket_config()

    spark_config = {
        "database":{
            "jdbc" : db_configs.jdbc_url,
            "config" : {
                "host" : db_configs.host,
                "port" : db_configs.port,
                "user" :db_configs.user,
                "password" : db_configs.password,
                "database" : db_configs.database,
                "driver": "org.postgresql.Driver"
            }
        },
        "s3": bucket_config.spark_s3_conf
    }
    return spark_config