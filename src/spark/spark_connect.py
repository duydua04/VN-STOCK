from configs.spark_config import get_spark_config, SparkConnect

def create_spark_connect():
    spark_config = get_spark_config()

    jars = ["org.postgresql:postgresql:42.6.2",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.540"
    ]

    spark_connect = SparkConnect(
        app_name="hoangduy",
        master_url="local[*]",
        executor_memory="2g",
        executor_cores=1,
        driver_memory="1g",
        num_executors=1,
        jar_packages=jars,
        spark_conf=spark_config["s3"],
        log_level="WARN"
    )

    return spark_connect

sc = create_spark_connect()


