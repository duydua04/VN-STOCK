from dataclasses import dataclass
from dotenv import load_dotenv
import os

@dataclass
class MinioConfig:
    endpoint_url: str
    access_key: str
    secret_key: str

    @property
    def storage_options(self):
        """Dùng cho Pandas"""
        return {
            "key": self.access_key,
            "secret": self.secret_key,
            "client_kwargs": {
                "endpoint_url": self.endpoint_url
            }
        }

    @property
    def spark_s3_conf(self):
        """Dùng cho Spark """
        return {
            "spark.hadoop.fs.s3a.endpoint": self.endpoint_url,
            "spark.hadoop.fs.s3a.access.key": self.access_key,
            "spark.hadoop.fs.s3a.secret.key": self.secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
            "spark.hadoop.fs.s3a.connection.timeout": "10000",
            "spark.hadoop.fs.s3a.socket.timeout": "10000",

            "spark.hadoop.fs.s3a.threads.keepalivetime": "60",

            "spark.hadoop.yarn.router.subcluster.cleaner.interval.time": "60",
            "spark.hadoop.yarn.resourcemanager.delegation-token-renewer.thread-retry-interval": "60",
            "spark.hadoop.yarn.resourcemanager.delegation-token-renewer.thread-timeout": "60",

            "spark.hadoop.fs.s3a.committer.name": "directory",
            "spark.hadoop.fs.s3a.committer.magic.enabled": "false",

            "spark.hadoop.fs.s3a.multipart.purge.age": "86400",
        }

def get_bucket_config():
    load_dotenv()
    return MinioConfig(
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY")
    )

